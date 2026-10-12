//! 局域网同步服务（P2）。
//!
//! 设计前提（见需求文档 A2/A4）：**PC 是唯一数据源，协议单向只增**。
//! 因此服务端不需要队列、不需要冲突合并：所有记录先落 `vocab.db`（`seq` 全局递增），
//! 有 WebSocket 连接就即时推，没有就等手机端下次 `GET /api/words?since=<seq>` 补拉。
//!
//! 鉴权：每台配对设备一个独立 token，便于单独吊销（`sync_unpair`）。
//! 明文 HTTP 是有意为之——只跑在用户自己的局域网内，且需求明确不要云与账号（E18）。

use crate::utils::vocab::{self, VocabAddResult};
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use qrcode::render::svg;
use qrcode::QrCode;
use rand::distributions::Alphanumeric;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, oneshot};

pub const DEFAULT_PORT: u16 = 31415;
const PAIRING_TTL_MS: i64 = 5 * 60 * 1000;
const DEFAULT_PAGE_SIZE: i64 = 500;
const MAX_PAGE_SIZE: i64 = 2000;
const KEEPALIVE_SECS: u64 = 15;

// 手机端自更新：APK 与版本信息放在 <data_dir>/update/ 下，
// 由发布脚本（scripts/publish-update.ps1）拷贝进去。
const APK_DIR: &str = "update";
const APK_NAME: &str = "pointra.apk";
const APK_META_NAME: &str = "pointra.json";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 本机在局域网中的地址。用 UDP connect 探测出口网卡，不会真的发包。
pub fn local_ip() -> Option<IpAddr> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    match socket.local_addr().ok()?.ip() {
        ip @ IpAddr::V4(_) if !ip.is_loopback() => Some(ip),
        _ => None,
    }
}

fn display_ip() -> String {
    local_ip()
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

// ---------------------------------------------------------------------------
// 数据契约
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct SyncEvent {
    #[serde(rename = "type")]
    pub kind: String,
    pub seq: i64,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize)]
struct SyncPage {
    items: Vec<Value>,
    next_since: i64,
    has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairedDevice {
    pub id: String,
    pub name: String,
    pub token: String,
    pub paired_at: i64,
    pub last_seen: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeviceView {
    pub id: String,
    pub name: String,
    pub paired_at: i64,
    pub last_seen: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncStatus {
    pub enabled: bool,
    pub running: bool,
    pub port: u16,
    pub address: String,
    pub device_id: String,
    /// 不含 token：设备凭证没必要下发到 WebView
    pub devices: Vec<DeviceView>,
    pub connections: usize,
    pub word_count: i64,
    pub sentence_count: i64,
    pub last_seq: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PairingInfo {
    pub code: String,
    pub expires_at: i64,
    pub address: String,
    pub port: u16,
    pub qr_svg: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyncPersist {
    enabled: bool,
    port: u16,
    device_id: String,
    devices: Vec<PairedDevice>,
}

impl Default for SyncPersist {
    fn default() -> Self {
        Self {
            enabled: false,
            port: DEFAULT_PORT,
            device_id: uuid::Uuid::new_v4().to_string(),
            devices: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct PairingCode {
    code: String,
    expires_at: i64,
}

// ---------------------------------------------------------------------------
// SyncManager
// ---------------------------------------------------------------------------

struct SyncInner {
    /// 持久化配置（sync.json）
    persist: Mutex<SyncPersist>,
    persist_path: PathBuf,
    db_path: PathBuf,
    pairing: Mutex<Option<PairingCode>>,
    events: broadcast::Sender<SyncEvent>,
    /// `Some` 表示服务正在运行；send 后触发 graceful shutdown
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    /// 实际监听端口（配置为 0 时由系统分配，与配置值不同）
    running_port: Mutex<Option<u16>>,
    connections: AtomicUsize,
}

#[derive(Clone)]
pub struct SyncManager {
    inner: Arc<SyncInner>,
}

impl SyncManager {
    pub fn new(data_dir: &Path) -> Self {
        let persist_path = data_dir.join("sync.json");
        let persist = std::fs::read_to_string(&persist_path)
            .ok()
            .and_then(|text| serde_json::from_str::<SyncPersist>(&text).ok())
            .unwrap_or_default();

        let (events, _) = broadcast::channel(256);

        Self {
            inner: Arc::new(SyncInner {
                persist: Mutex::new(persist),
                persist_path,
                db_path: data_dir.join("vocab.db"),
                pairing: Mutex::new(None),
                events,
                shutdown: Mutex::new(None),
                running_port: Mutex::new(None),
                connections: AtomicUsize::new(0),
            }),
        }
    }

    fn save(&self) {
        // 注意：调用方必须已经释放 persist 锁，否则自死锁
        let snapshot = self.inner.persist.lock().unwrap().clone();
        if let Ok(text) = serde_json::to_string_pretty(&snapshot) {
            if let Some(parent) = self.inner.persist_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let _ = std::fs::write(&self.inner.persist_path, text);
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SyncEvent> {
        self.inner.events.subscribe()
    }

    /// 数据目录（`vocab.db` 所在目录）。手机端自更新的 APK 也放这里的 `update/` 下。
    pub fn data_dir(&self) -> PathBuf {
        self.inner
            .db_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
    }

    /// 把一次收藏广播给所有在线手机端。
    /// 没有连接时 `send` 返回 Err，这里刻意忽略——手机端下次补拉即可。
    pub fn broadcast_add(&self, result: &VocabAddResult) {
        let kind = match (result.kind.as_str(), result.action.as_str()) {
            ("word", "added") => "word.added",
            ("word", "updated") => "word.updated",
            ("sentence", "added") => "sentence.added",
            ("sentence", "updated") => "sentence.updated",
            _ => return,
        };
        let _ = self.inner.events.send(SyncEvent {
            kind: kind.to_string(),
            seq: result.seq,
            payload: result.payload.clone(),
        });
    }

    pub fn is_running(&self) -> bool {
        self.inner.shutdown.lock().unwrap().is_some()
    }

    /// 保存开关与端口。**不会**自动启停服务，由命令层显式调用 start/stop。
    ///
    /// `port == 0` 表示「让系统随机分配端口」；设置页不会传 0，仅集成测试使用。
    pub fn set_config(&self, enabled: bool, port: u16) -> Result<(), String> {
        if port != 0 && port < 1024 {
            return Err("端口需在 1024-65535 之间".to_string());
        }
        {
            let mut p = self.inner.persist.lock().unwrap();
            p.enabled = enabled;
            p.port = port;
        }
        self.save();
        Ok(())
    }

    /// 当前生效端口：运行中用实际监听端口，否则用配置值。
    fn effective_port(&self) -> u16 {
        self.inner
            .running_port
            .lock()
            .unwrap()
            .unwrap_or_else(|| self.inner.persist.lock().unwrap().port)
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.persist.lock().unwrap().enabled
    }

    /// 启动服务，返回实际监听端口（配置为 0 时由系统随机分配）。
    pub fn start(&self) -> Result<u16, String> {
        let mut guard = self.inner.shutdown.lock().unwrap();
        if guard.is_some() {
            return Err("同步服务已经在运行".to_string());
        }

        let port = self.inner.persist.lock().unwrap().port;
        let addr = SocketAddr::from(([0, 0, 0, 0], port));

        // 用同步 bind，这样端口占用 / 权限错误能立刻返回给设置页
        let std_listener = std::net::TcpListener::bind(addr)
            .map_err(|e| format!("端口 {port} 绑定失败（可能被占用，或未在防火墙放行）: {e}"))?;
        std_listener
            .set_nonblocking(true)
            .map_err(|e| format!("设置监听为非阻塞失败: {e}"))?;
        let actual_port = std_listener
            .local_addr()
            .map_err(|e| format!("读取监听地址失败: {e}"))?
            .port();

        let (tx, rx) = oneshot::channel::<()>();
        *guard = Some(tx);
        drop(guard);
        *self.inner.running_port.lock().unwrap() = Some(actual_port);

        let router = build_router(self.clone());

        tauri::async_runtime::spawn(async move {
            let listener = match tokio::net::TcpListener::from_std(std_listener) {
                Ok(listener) => listener,
                Err(err) => {
                    eprintln!("[Pointra][同步] 接管监听器失败: {err}");
                    return;
                }
            };
            let server = axum::serve(listener, router).with_graceful_shutdown(async move {
                let _ = rx.await;
            });
            if let Err(err) = server.await {
                eprintln!("[Pointra][同步] 服务异常退出: {err}");
            }
        });

        Ok(actual_port)
    }

    pub fn stop(&self) {
        if let Some(tx) = self.inner.shutdown.lock().unwrap().take() {
            let _ = tx.send(());
        }
        *self.inner.running_port.lock().unwrap() = None;
    }

    /// 生成（或复用未过期的）配对码，并连同二维码一起返回。
    pub fn pairing_info(&self) -> PairingInfo {
        let now = now_ms();
        let code = {
            let mut guard = self.inner.pairing.lock().unwrap();
            let expired = guard.as_ref().map(|p| p.expires_at <= now).unwrap_or(true);
            if expired {
                let fresh = format!("{:06}", rand::thread_rng().gen_range(0..1_000_000u32));
                *guard = Some(PairingCode {
                    code: fresh,
                    expires_at: now + PAIRING_TTL_MS,
                });
            }
            guard.as_ref().unwrap().clone()
        };

        let port = self.effective_port();
        let ip = display_ip();
        let qr_payload = format!("pointra://pair?host={ip}&port={port}&code={}", code.code);
        let qr_svg = QrCode::new(qr_payload.as_bytes())
            .map(|qr| qr.render::<svg::Color>().min_dimensions(220, 220).build())
            .unwrap_or_default();

        PairingInfo {
            code: code.code,
            expires_at: code.expires_at,
            address: format!("http://{ip}:{port}"),
            port,
            qr_svg,
        }
    }

    /// 配对码一次性消费；成功后签发该设备专属 token。
    pub fn pair(&self, code: &str, device_name: &str) -> Result<(String, String), String> {
        let now = now_ms();
        {
            let mut guard = self.inner.pairing.lock().unwrap();
            match guard.as_ref() {
                Some(p) if p.expires_at > now && p.code == code => *guard = None,
                Some(_) => return Err("配对码不正确或已过期".to_string()),
                None => return Err("请先在电脑端生成配对码".to_string()),
            }
        }

        let token: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(48)
            .map(char::from)
            .collect();
        let device_id = uuid::Uuid::new_v4().to_string();

        {
            let mut p = self.inner.persist.lock().unwrap();
            p.devices.push(PairedDevice {
                id: device_id.clone(),
                name: device_name.trim().chars().take(64).collect(),
                token: token.clone(),
                paired_at: now,
                last_seen: now,
            });
        }
        self.save();

        Ok((token, device_id))
    }

    pub fn unpair(&self, device_id: &str) -> bool {
        let removed = {
            let mut p = self.inner.persist.lock().unwrap();
            let before = p.devices.len();
            p.devices.retain(|d| d.id != device_id);
            p.devices.len() != before
        };
        if removed {
            self.save();
        }
        removed
    }

    /// token 校验。命中即刷新 last_seen（只改内存，不写盘，避免每请求一次 IO）。
    pub fn authenticate(&self, token: &str) -> bool {
        if token.is_empty() {
            return false;
        }
        let mut p = self.inner.persist.lock().unwrap();
        match p.devices.iter_mut().find(|d| d.token == token) {
            Some(device) => {
                device.last_seen = now_ms();
                true
            }
            None => false,
        }
    }

    pub fn status(&self) -> SyncStatus {
        let (enabled, device_id, devices) = {
            let p = self.inner.persist.lock().unwrap();
            (p.enabled, p.device_id.clone(), p.devices.clone())
        };
        let port = self.effective_port();

        let (word_count, sentence_count, last_seq) = match vocab::open(&self.inner.db_path) {
            Ok(conn) => vocab::get_stats(&conn)
                .map(|s| (s.word_count, s.sentence_count, s.last_seq))
                .unwrap_or((0, 0, 0)),
            Err(_) => (0, 0, 0),
        };

        SyncStatus {
            enabled,
            running: self.is_running(),
            port,
            address: format!("http://{}:{port}", display_ip()),
            device_id,
            devices: devices
                .into_iter()
                .map(|d| DeviceView {
                    id: d.id,
                    name: d.name,
                    paired_at: d.paired_at,
                    last_seen: d.last_seen,
                })
                .collect(),
            connections: self.inner.connections.load(Ordering::Relaxed),
            word_count,
            sentence_count,
            last_seq,
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP / WebSocket 层
// ---------------------------------------------------------------------------

fn not_found(msg: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": msg }))).into_response()
}

fn apk_path(data_dir: &Path) -> PathBuf {
    data_dir.join(APK_DIR).join(APK_NAME)
}

fn apk_meta_path(data_dir: &Path) -> PathBuf {
    data_dir.join(APK_DIR).join(APK_META_NAME)
}

/// 读发布元数据，**容错 UTF-8 BOM**：
/// 用户可能用记事本 / PowerShell 写这个文件，很容易带上 `EF BB BF`，
/// 而 `serde_json` 会直接报 `expected value at line 1 column 1`。
fn read_meta(path: &Path) -> Result<AppRelease, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("读不到版本信息：{e}"))?;
    serde_json::from_str(text.trim_start_matches('\u{feff}'))
        .map_err(|e| format!("版本信息 pointra.json 损坏: {e}"))
}

/// 手机端「检查更新」读的版本信息（由发布脚本写成 JSON）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRelease {
    pub version: String,
    pub version_code: i64,
    pub size: u64,
    pub sha256: String,
}

/// `GET /api/app/latest`。**免鉴权**（与 ping/pair 一致）：
/// 即使配对 token 被吊销，也还能把 App 升上来。
async fn handle_app_latest(State(mgr): State<SyncManager>) -> Response {
    let data_dir = mgr.data_dir();
    if !apk_path(&data_dir).exists() {
        return not_found("电脑端还没放新版本 APK（update/pointra.apk）");
    }
    match read_meta(&apk_meta_path(&data_dir)) {
        Ok(info) => Json(info).into_response(),
        Err(err) => internal_error(err),
    }
}

/// `GET /api/app/download`。与 latest 配套，同样免鉴权。
async fn handle_app_download(State(mgr): State<SyncManager>) -> Response {
    let path = apk_path(&mgr.data_dir());
    match tokio::fs::read(&path).await {
        Ok(bytes) => (
            [(
                header::CONTENT_TYPE,
                "application/vnd.android.package-archive",
            )],
            bytes,
        )
            .into_response(),
        Err(_) => not_found("APK 不存在"),
    }
}

fn build_router(mgr: SyncManager) -> Router {
    Router::new()
        .route("/api/ping", get(handle_ping))
        .route("/api/pair", post(handle_pair))
        .route("/api/words", get(handle_words))
        .route("/api/sentences", get(handle_sentences))
        .route("/api/stats", get(handle_stats))
        .route("/api/events", get(handle_events))
        // 手机端自更新（免鉴权，见 docs/02）
        .route("/api/app/latest", get(handle_app_latest))
        .route("/api/app/download", get(handle_app_download))
        .with_state(mgr)
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": "未授权的设备，请先配对" })),
    )
        .into_response()
}

fn internal_error(msg: String) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": msg })),
    )
        .into_response()
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(|s| s.trim().to_string())
}

async fn handle_ping(State(mgr): State<SyncManager>) -> Response {
    let (device_id, device_count) = {
        let status = mgr.status();
        (status.device_id, status.devices.len())
    };
    Json(json!({
        "name": "Pointra",
        "version": env!("CARGO_PKG_VERSION"),
        "device_id": device_id,
        "paired": device_count > 0,
    }))
    .into_response()
}

#[derive(Deserialize)]
struct PairRequest {
    code: String,
    #[serde(default)]
    device_name: String,
}

async fn handle_pair(State(mgr): State<SyncManager>, Json(req): Json<PairRequest>) -> Response {
    let name = if req.device_name.trim().is_empty() {
        "Android".to_string()
    } else {
        req.device_name
    };

    match mgr.pair(&req.code, &name) {
        Ok((token, device_id)) => Json(json!({
            "token": token,
            "device_id": device_id,
            "server_name": "Pointra",
        }))
        .into_response(),
        Err(msg) => (StatusCode::UNAUTHORIZED, Json(json!({ "error": msg }))).into_response(),
    }
}

#[derive(Deserialize)]
struct SinceQuery {
    since: Option<i64>,
    limit: Option<i64>,
}

fn page_response(mgr: &SyncManager, table: &str, q: &SinceQuery, headers: &HeaderMap) -> Response {
    let token = match bearer_token(headers) {
        Some(t) if mgr.authenticate(&t) => t,
        _ => return unauthorized(),
    };
    let _ = token;

    let since = q.since.unwrap_or(0).max(0);
    let limit = q.limit.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, MAX_PAGE_SIZE);

    let conn = match vocab::open(&mgr.inner.db_path) {
        Ok(conn) => conn,
        Err(err) => return internal_error(err),
    };

    match vocab::query_since(&conn, table, since, limit) {
        Ok((items, has_more)) => {
            let next_since = items
                .last()
                .and_then(|item| item.get("seq"))
                .and_then(|seq| seq.as_i64())
                .unwrap_or(since);
            Json(SyncPage {
                items,
                next_since,
                has_more,
            })
            .into_response()
        }
        Err(err) => internal_error(err),
    }
}

async fn handle_words(
    State(mgr): State<SyncManager>,
    headers: HeaderMap,
    Query(q): Query<SinceQuery>,
) -> Response {
    page_response(&mgr, "words", &q, &headers)
}

async fn handle_sentences(
    State(mgr): State<SyncManager>,
    headers: HeaderMap,
    Query(q): Query<SinceQuery>,
) -> Response {
    page_response(&mgr, "sentences", &q, &headers)
}

async fn handle_stats(State(mgr): State<SyncManager>, headers: HeaderMap) -> Response {
    match bearer_token(&headers) {
        Some(t) if mgr.authenticate(&t) => {}
        _ => return unauthorized(),
    }
    match vocab::open(&mgr.inner.db_path).and_then(|conn| vocab::get_stats(&conn)) {
        Ok(stats) => Json(json!({
            "word_count": stats.word_count,
            "sentence_count": stats.sentence_count,
            "last_seq": stats.last_seq,
        }))
        .into_response(),
        Err(err) => internal_error(err),
    }
}

#[derive(Deserialize)]
struct WsQuery {
    token: Option<String>,
}

async fn handle_events(
    State(mgr): State<SyncManager>,
    Query(q): Query<WsQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let token = q.token.or_else(|| bearer_token(&headers));
    match token {
        Some(t) if mgr.authenticate(&t) => {}
        _ => return unauthorized(),
    }
    ws.on_upgrade(move |socket| ws_session(socket, mgr))
}

/// 单条 WS 连接的生命周期。
///
/// 每 `KEEPALIVE_SECS` 发一次 Ping：手机端被系统冻结或拔网线时，
/// `send` 会在 30 秒内失败，从而让连接计数回落（验收标准之一）。
async fn ws_session(mut socket: WebSocket, mgr: SyncManager) {
    mgr.inner.connections.fetch_add(1, Ordering::Relaxed);
    let mut rx = mgr.subscribe();
    let mut keepalive = tokio::time::interval(Duration::from_secs(KEEPALIVE_SECS));
    keepalive.tick().await; // 第一次 tick 立即返回，跳过

    loop {
        tokio::select! {
            event = rx.recv() => match event {
                Ok(evt) => {
                    let text = match serde_json::to_string(&evt) {
                        Ok(text) => text,
                        Err(_) => continue,
                    };
                    if socket.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                // 慢消费者落后了：跳过丢失的事件，手机端会靠 since 补拉
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            },
            _ = keepalive.tick() => {
                if socket.send(Message::Ping(Bytes::new())).await.is_err() {
                    break;
                }
            }
            incoming = socket.recv() => match incoming {
                None => break,
                Some(Err(_)) => break,
                Some(Ok(Message::Close(_))) => break,
                Some(Ok(_)) => {}
            }
        }
    }

    mgr.inner.connections.fetch_sub(1, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("pointra-sync-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn seed(dir: &Path) {
        let conn = vocab::open(&dir.join("vocab.db")).unwrap();
        vocab::add_entry(
            &conn,
            &json!({ "translate": { "text": "scout", "dit": "侦察员" } }),
        )
        .unwrap();
        vocab::add_entry(
            &conn,
            &json!({ "translate": { "text": "one two three", "dit": "一二三" } }),
        )
        .unwrap();
    }

    /// 手机端自更新：没放包时 404（手机端据此认为「已是最新」）；
    /// 放了包后 latest 返回版本信息、download 返回 APK 字节，且**两个接口都免鉴权**。
    #[tokio::test]
    async fn app_update_endpoints_are_public_and_serve_the_apk() {
        let dir = temp_dir();
        let mgr = SyncManager::new(&dir);
        mgr.set_config(true, 0).unwrap();
        let port = mgr.start().unwrap();
        let base = format!("http://127.0.0.1:{port}");

        // 还没发布新版本
        let res = reqwest::get(format!("{base}/api/app/latest"))
            .await
            .unwrap();
        assert_eq!(res.status(), 404);
        let res = reqwest::get(format!("{base}/api/app/download"))
            .await
            .unwrap();
        assert_eq!(res.status(), 404);

        // 发布一个「新版本」
        let update_dir = dir.join("update");
        std::fs::create_dir_all(&update_dir).unwrap();
        let apk = b"FAKE-APK-BYTES";
        std::fs::write(update_dir.join("pointra.apk"), apk).unwrap();
        std::fs::write(
            update_dir.join("pointra.json"),
            serde_json::to_string(&AppRelease {
                version: "0.2.0".into(),
                version_code: 2000,
                size: apk.len() as u64,
                sha256: "deadbeef".into(),
            })
            .unwrap(),
        )
        .unwrap();

        let info: Value = reqwest::get(format!("{base}/api/app/latest"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(info["version"], "0.2.0");
        assert_eq!(info["version_code"], 2000);
        assert_eq!(info["size"], apk.len() as u64);

        // 未配对也能下（token 被吊销时仍能升级）
        let res = reqwest::get(format!("{base}/api/app/download"))
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/vnd.android.package-archive"
        );
        assert_eq!(res.bytes().await.unwrap().as_ref(), apk);
    }

    /// 发布元数据可能被记事本/PowerShell 写成带 UTF-8 BOM 的文件，
    /// `serde_json` 会因此解析失败——服务端必须容错，否则手机端永远看到「信息损坏」。
    #[tokio::test]
    async fn app_latest_tolerates_utf8_bom() {
        let dir = temp_dir();
        let mgr = SyncManager::new(&dir);
        mgr.set_config(true, 0).unwrap();
        let port = mgr.start().unwrap();
        let base = format!("http://127.0.0.1:{port}");

        let update_dir = dir.join("update");
        std::fs::create_dir_all(&update_dir).unwrap();
        std::fs::write(update_dir.join("pointra.apk"), b"APK").unwrap();
        let json = serde_json::to_string(&AppRelease {
            version: "0.3.0".into(),
            version_code: 3000,
            size: 3,
            sha256: "x".into(),
        })
        .unwrap();
        let mut with_bom = vec![0xEF, 0xBB, 0xBF];
        with_bom.extend_from_slice(json.as_bytes());
        std::fs::write(update_dir.join("pointra.json"), with_bom).unwrap();

        let res = reqwest::get(format!("{base}/api/app/latest"))
            .await
            .unwrap();
        assert_eq!(res.status(), 200, "带 BOM 的 json 也必须能解析");
        let info: Value = res.json().await.unwrap();
        assert_eq!(info["version"], "0.3.0");
    }

    /// 一条测试打通：ping → 鉴权 → 配对（一次性）→ 增量拉取 → 再拉为空。
    #[tokio::test]
    async fn ping_pair_and_incremental_pull() {
        let dir = temp_dir();
        seed(&dir);

        let mgr = SyncManager::new(&dir);
        mgr.set_config(true, 0).unwrap();
        let port = mgr.start().unwrap();
        let base = format!("http://127.0.0.1:{port}");

        let ping: Value = reqwest::get(format!("{base}/api/ping"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(ping["name"], "Pointra");
        assert_eq!(ping["paired"], false);

        // 未配对 → 401
        let res = reqwest::get(format!("{base}/api/words?since=0"))
            .await
            .unwrap();
        assert_eq!(res.status(), 401);

        // 配对
        let code = mgr.pairing_info().code;
        let client = reqwest::Client::new();
        let paired: Value = client
            .post(format!("{base}/api/pair"))
            .json(&json!({ "code": code, "device_name": "test-phone" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let token = paired["token"].as_str().unwrap().to_string();
        assert_eq!(token.len(), 48);

        // 配对码是一次性的
        let reuse = client
            .post(format!("{base}/api/pair"))
            .json(&json!({ "code": code, "device_name": "replay" }))
            .send()
            .await
            .unwrap();
        assert_eq!(reuse.status(), 401);

        // 增量拉取 words
        let page: Value = client
            .get(format!("{base}/api/words?since=0"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "只有 scout 是单词");
        assert_eq!(items[0]["word"], "scout");
        assert_eq!(items[0]["usual_dict"], json!([]));
        assert_eq!(page["has_more"], false);
        let cursor = page["next_since"].as_i64().unwrap();

        // sentences 共用同一个全局 seq 序列，因此序号必须大于 words 的
        let spage: Value = client
            .get(format!("{base}/api/sentences?since=0"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(spage["items"][0]["trans"], "一二三");
        assert!(spage["items"][0]["seq"].as_i64().unwrap() > cursor);

        // 游标之后没有新数据
        let empty: Value = client
            .get(format!("{base}/api/words?since={cursor}"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(empty["items"].as_array().unwrap().is_empty());

        // stats
        let stats: Value = client
            .get(format!("{base}/api/stats"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(stats["word_count"], 1);
        assert_eq!(stats["sentence_count"], 1);
        assert_eq!(stats["last_seq"], 2);

        mgr.stop();
    }

    /// WS 客户端实测收到 word.added 推送（对应 P2 验收标准）。
    #[tokio::test]
    async fn websocket_receives_word_added() {
        use futures_util::StreamExt;
        use tokio_tungstenite::tungstenite::Message as WsMessage;

        let dir = temp_dir();
        seed(&dir);

        let mgr = SyncManager::new(&dir);
        mgr.set_config(true, 0).unwrap();
        let port = mgr.start().unwrap();
        let base = format!("http://127.0.0.1:{port}");

        let code = mgr.pairing_info().code;
        let paired: Value = reqwest::Client::new()
            .post(format!("{base}/api/pair"))
            .json(&json!({ "code": code, "device_name": "ws-test" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let token = paired["token"].as_str().unwrap().to_string();

        let (mut ws, _) = tokio_tungstenite::connect_async(format!(
            "ws://127.0.0.1:{port}/api/events?token={token}"
        ))
        .await
        .unwrap();

        // 触发一次真实收藏 + 广播
        let conn = vocab::open(&dir.join("vocab.db")).unwrap();
        let added = vocab::add_entry(
            &conn,
            &json!({ "translate": { "text": "harbor", "dit": "港口" } }),
        )
        .unwrap();
        mgr.broadcast_add(&added);

        // keepalive Ping 可能先到，循环直到拿到文本帧
        let mut received: Option<String> = None;
        for _ in 0..20 {
            match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
                Ok(Some(Ok(WsMessage::Text(text)))) => {
                    received = Some(text.to_string());
                    break;
                }
                Ok(Some(Ok(_))) => continue,
                _ => break,
            }
        }

        let payload = received.expect("应当收到一条 WebSocket 文本消息");
        let event: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(event["type"], "word.added");
        assert_eq!(event["payload"]["word"], "harbor");
        assert_eq!(event["payload"]["dit"], "港口");
        assert_eq!(event["seq"], added.seq);

        let _ = ws.close(None).await;
        mgr.stop();
    }

    /// 未授权 token 不允许建立 WS。
    #[tokio::test]
    async fn websocket_rejects_bad_token() {
        let dir = temp_dir();
        seed(&dir);
        let mgr = SyncManager::new(&dir);
        mgr.set_config(true, 0).unwrap();
        let port = mgr.start().unwrap();

        let result = tokio_tungstenite::connect_async(format!(
            "ws://127.0.0.1:{port}/api/events?token=nope"
        ))
        .await;
        assert!(result.is_err(), "无效 token 的 WS 握手应当失败");
        mgr.stop();
    }

    /// 配对 → 解绑后，旧 token 立即失效。
    #[tokio::test]
    async fn unpair_revokes_token() {
        let dir = temp_dir();
        seed(&dir);
        let mgr = SyncManager::new(&dir);
        mgr.set_config(true, 0).unwrap();
        let port = mgr.start().unwrap();
        let base = format!("http://127.0.0.1:{port}");

        let code = mgr.pairing_info().code;
        let paired: Value = reqwest::Client::new()
            .post(format!("{base}/api/pair"))
            .json(&json!({ "code": code, "device_name": "phone" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let token = paired["token"].as_str().unwrap().to_string();
        let device_id = paired["device_id"].as_str().unwrap().to_string();

        assert!(mgr.unpair(&device_id));
        let res = reqwest::Client::new()
            .get(format!("{base}/api/words?since=0"))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 401);

        mgr.stop();
    }

    #[test]
    fn status_reports_db_counts() {
        let dir = temp_dir();
        seed(&dir);
        let mgr = SyncManager::new(&dir);
        let status = mgr.status();
        assert_eq!(status.word_count, 1);
        assert_eq!(status.sentence_count, 1);
        assert_eq!(status.last_seq, 2);
        assert!(!status.running);
        assert!(status.address.starts_with("http://"));
    }

    /// WS 客户端断开后，服务端连接计数应回落
    /// （对应 P2 验收标准里的「连接断开后被清理」）。
    #[tokio::test]
    async fn websocket_connection_is_cleaned_up() {
        use futures_util::StreamExt;

        let dir = temp_dir();
        seed(&dir);
        let mgr = SyncManager::new(&dir);
        mgr.set_config(true, 0).unwrap();
        let port = mgr.start().unwrap();
        let base = format!("http://127.0.0.1:{port}");

        let code = mgr.pairing_info().code;
        let paired: Value = reqwest::Client::new()
            .post(format!("{base}/api/pair"))
            .json(&json!({ "code": code, "device_name": "cleanup" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let token = paired["token"].as_str().unwrap().to_string();

        let (mut ws, _) = tokio_tungstenite::connect_async(format!(
            "ws://127.0.0.1:{port}/api/events?token={token}"
        ))
        .await
        .unwrap();

        let mut counted = false;
        for _ in 0..50 {
            if mgr.status().connections == 1 {
                counted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(counted, "连接建立后计数应为 1");

        ws.close(None).await.unwrap();
        while let Some(item) = ws.next().await {
            if item.is_err() {
                break;
            }
        }

        let mut cleared = false;
        for _ in 0..50 {
            if mgr.status().connections == 0 {
                cleared = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(cleared, "客户端断开后连接计数应回落到 0");

        mgr.stop();
    }
}
