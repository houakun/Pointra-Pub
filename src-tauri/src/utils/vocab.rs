//! 生词库 / 句子本：本地 SQLite 存储。
//!
//! P1 阶段只负责「收藏」：把最近一次翻译结果落库，并返回同步序号（`seq`）。
//! P2 的同步服务会复用这里的 `seq` 作为手机端增量拉取游标，因此有两条硬约束：
//!
//! 1. `seq` 全局单调递增，绝不回收、绝不复用；
//! 2. **重复收藏同一个词时 `seq` 保持不变**，只更新词义字段。
//!    否则手机端 `since=<seq>` 的增量游标会跳过中间记录，导致丢词。
//!
//! 手机端的学习进度（SM-2）不在这里，PC 端不做学习状态（见需求文档 Q4/A4）。

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use sha1::{Digest, Sha1};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS seq_counter (
    id    INTEGER PRIMARY KEY CHECK (id = 1),
    value INTEGER NOT NULL
);
INSERT OR IGNORE INTO seq_counter (id, value) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS words (
    seq          INTEGER PRIMARY KEY,
    id           TEXT    NOT NULL UNIQUE,
    word         TEXT    NOT NULL,
    normalized   TEXT    NOT NULL,
    phonetic_us  TEXT,
    phonetic_uk  TEXT,
    audio_us     TEXT,
    audio_uk     TEXT,
    dit          TEXT,
    usual_dict   TEXT,
    exchange     TEXT,
    added_at     INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sentences (
    seq       INTEGER PRIMARY KEY,
    id        TEXT    NOT NULL UNIQUE,
    text      TEXT    NOT NULL,
    trans     TEXT,
    added_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT
);
"#;

/// 一次收藏的结果，用于前端 Toast / Rust 端日志 / 后续同步推送。
#[derive(Debug, Clone, Serialize)]
pub struct VocabAddResult {
    /// `"word"` | `"sentence"`
    pub kind: String,
    /// `"added"`（首次收藏）| `"updated"`（重复收藏，仅刷新词义）
    pub action: String,
    /// 被收藏的文本（单词原形或整句）
    pub text: String,
    /// 同步序号；重复收藏时保持首次入库的值
    pub seq: i64,
    /// 入库后的完整记录，仅供同步服务广播使用；不序列化给前端
    #[serde(skip_serializing)]
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct VocabStats {
    pub word_count: i64,
    pub sentence_count: i64,
    /// 当前最大 seq，等价于「已入库记录数」（PC 端从不删除）
    pub last_seq: i64,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn sha1_hex(input: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(input.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// 分配一个**全局唯一**的同步序号。
///
/// `words` 与 `sentences` 必须共用同一个序列：手机端只维护**一个** `last_seq` 游标，
/// 若让两表各自 AUTOINCREMENT，`max(seq)` 会小于实际记录总数，
/// 那么「用全局 max 当作两表的 since」就会漏掉序号较小的那一半数据。
fn next_seq(conn: &Connection) -> Result<i64, String> {
    conn.execute("UPDATE seq_counter SET value = value + 1 WHERE id = 1", [])
        .map_err(|e| format!("分配同步序号失败: {e}"))?;
    conn.query_row("SELECT value FROM seq_counter WHERE id = 1", [], |row| {
        row.get(0)
    })
    .map_err(|e| format!("读取同步序号失败: {e}"))
}

/// 打开（必要时创建）生词库。
pub fn open(db_path: &Path) -> Result<Connection, String> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("创建数据目录失败: {e}"))?;
    }

    let conn = Connection::open(db_path).map_err(|e| format!("打开生词库失败: {e}"))?;
    conn.execute_batch(SCHEMA)
        .map_err(|e| format!("初始化生词库失败: {e}"))?;
    Ok(conn)
}

pub fn get_stats(conn: &Connection) -> Result<VocabStats, String> {
    let count = |table: &str| -> Result<i64, String> {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .map_err(|e| format!("统计 {table} 失败: {e}"))
    };

    // 全局序号计数器（words / sentences 共用），见 next_seq 的说明
    let last_seq: i64 = conn
        .query_row("SELECT value FROM seq_counter WHERE id = 1", [], |row| {
            row.get(0)
        })
        .map_err(|e| format!("读取同步序号失败: {e}"))?;

    Ok(VocabStats {
        word_count: count("words")?,
        sentence_count: count("sentences")?,
        last_seq,
    })
}

/// 收藏一条翻译结果。自动区分单词 / 句子。
pub fn add_entry(conn: &Connection, data: &Value) -> Result<VocabAddResult, String> {
    let text = data
        .pointer("/translate/text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    if text.is_empty() {
        return Err("暂无可收藏的内容".to_string());
    }

    if judge_sentence(&text) {
        add_sentence(conn, &text, data)
    } else {
        add_word(conn, &text, data)
    }
}

/// 与前端 `src/main/utils/tool.ts::judgeSentence` 保持一致的判定：
/// 多个英文词（且不是 `U.S.A.` 这类缩写）视为句子。
pub fn judge_sentence(input: &str) -> bool {
    let cleaned = input.trim();
    if cleaned.is_empty() {
        return false;
    }

    // 等价于 JS 的 /[a-zA-Z'-]+/g
    let mut words: Vec<&str> = Vec::new();
    let mut start: Option<usize> = None;
    for (idx, ch) in cleaned.char_indices() {
        let is_word_char = ch.is_ascii_alphabetic() || ch == '\'' || ch == '-';
        if is_word_char {
            if start.is_none() {
                start = Some(idx);
            }
        } else if let Some(begin) = start.take() {
            words.push(&cleaned[begin..idx]);
        }
    }
    if let Some(begin) = start {
        words.push(&cleaned[begin..]);
    }

    // 一个英文词都没匹配到 → 交给翻译接口按句子处理
    if words.is_empty() {
        return true;
    }

    if words.len() > 1 && words.iter().all(|w| w.len() == 1) && cleaned.contains('.') {
        return false; // U.S.A. 之类的缩写
    }

    words.len() != 1
}

fn add_word(conn: &Connection, word: &str, data: &Value) -> Result<VocabAddResult, String> {
    let normalized = word.to_lowercase();
    let id = sha1_hex(&normalized);
    let dit = data
        .pointer("/translate/dit")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let (usual_dict, exchange) = match data.get("wordCard") {
        Some(card) => (
            card.get("usualDict")
                .filter(|v| !v.is_null())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "[]".to_string()),
            card.get("exchange")
                .filter(|v| v.is_object())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "{}".to_string()),
        ),
        None => ("[]".to_string(), "{}".to_string()),
    };

    let voice = data.get("voice");
    let phonetic_us = extract_phonetic(voice, "usa");
    let phonetic_uk = extract_phonetic(voice, "uk");
    let audio_us = extract_audio(voice, word, true);
    let audio_uk = extract_audio(voice, word, false);

    let now = now_ms();

    let existing: Option<i64> = conn
        .query_row("SELECT seq FROM words WHERE id = ?1", params![id], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|e| format!("查询生词失败: {e}"))?;

    if let Some(seq) = existing {
        // 重复收藏：seq 保持不变（见模块头部约束 2），只刷新词义
        conn.execute(
            "UPDATE words SET word = ?1, normalized = ?2, phonetic_us = ?3, phonetic_uk = ?4, \
             audio_us = ?5, audio_uk = ?6, dit = ?7, usual_dict = ?8, exchange = ?9, \
             updated_at = ?10 WHERE id = ?11",
            params![
                word,
                normalized,
                phonetic_us,
                phonetic_uk,
                audio_us,
                audio_uk,
                dit,
                usual_dict,
                exchange,
                now,
                id
            ],
        )
        .map_err(|e| format!("更新生词失败: {e}"))?;

        return Ok(VocabAddResult {
            kind: "word".to_string(),
            action: "updated".to_string(),
            text: word.to_string(),
            seq,
            payload: fetch_by_seq(conn, "words", seq)?.unwrap_or(Value::Null),
        });
    }

    let seq = next_seq(conn)?;

    conn.execute(
        "INSERT INTO words (seq, id, word, normalized, phonetic_us, phonetic_uk, audio_us, \
         audio_uk, dit, usual_dict, exchange, added_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12)",
        params![
            seq,
            id,
            word,
            normalized,
            phonetic_us,
            phonetic_uk,
            audio_us,
            audio_uk,
            dit,
            usual_dict,
            exchange,
            now
        ],
    )
    .map_err(|e| format!("写入生词失败: {e}"))?;

    Ok(VocabAddResult {
        kind: "word".to_string(),
        action: "added".to_string(),
        text: word.to_string(),
        seq,
        payload: fetch_by_seq(conn, "words", seq)?.unwrap_or(Value::Null),
    })
}

fn add_sentence(conn: &Connection, text: &str, data: &Value) -> Result<VocabAddResult, String> {
    let id = sha1_hex(&text.to_lowercase());
    let trans = data
        .pointer("/translate/dit")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let now = now_ms();

    let existing: Option<i64> = conn
        .query_row(
            "SELECT seq FROM sentences WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| format!("查询句子失败: {e}"))?;

    if let Some(seq) = existing {
        conn.execute(
            "UPDATE sentences SET text = ?1, trans = ?2 WHERE id = ?3",
            params![text, trans, id],
        )
        .map_err(|e| format!("更新句子失败: {e}"))?;

        return Ok(VocabAddResult {
            kind: "sentence".to_string(),
            action: "updated".to_string(),
            text: text.to_string(),
            seq,
            payload: fetch_by_seq(conn, "sentences", seq)?.unwrap_or(Value::Null),
        });
    }

    let seq = next_seq(conn)?;

    conn.execute(
        "INSERT INTO sentences (seq, id, text, trans, added_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![seq, id, text, trans, now],
    )
    .map_err(|e| format!("写入句子失败: {e}"))?;

    Ok(VocabAddResult {
        kind: "sentence".to_string(),
        action: "added".to_string(),
        text: text.to_string(),
        seq,
        payload: fetch_by_seq(conn, "sentences", seq)?.unwrap_or(Value::Null),
    })
}

/// 从 `voice.phonetic[]` 中取指定口音的音标文本。
fn extract_phonetic(voice: Option<&Value>, kind: &str) -> Option<String> {
    voice?
        .get("phonetic")?
        .as_array()?
        .iter()
        .find(|item| item.get("type").and_then(|t| t.as_str()) == Some(kind))
        .and_then(|item| item.get("text").and_then(|t| t.as_str()))
        .filter(|text| !text.is_empty())
        .map(|text| text.to_string())
}

/// 与前端 `tool.ts::getPhoneticUrl` 保持一致：
/// 优先用接口给的 `filename`，取不到时按 `type=0 美音 / type=1 英音` 拼。
///
/// ⚠️ 注意 `trans_server.rs` 头部注释写的是「type=2 美音 / type=1 英音」，
/// 与前端实现不符。此处以**前端实际行为**为准；P3 接入手机端前会实测统一。
fn extract_audio(voice: Option<&Value>, word: &str, is_usa: bool) -> Option<String> {
    let kind = if is_usa { "usa" } else { "uk" };

    if let Some(filename) = voice
        .and_then(|v| v.get("phonetic"))
        .and_then(|p| p.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|item| item.get("type").and_then(|t| t.as_str()) == Some(kind))
                .and_then(|item| item.get("filename").and_then(|f| f.as_str()))
        })
        .filter(|s| !s.is_empty())
    {
        return Some(filename.to_string());
    }

    let voice_type = if is_usa { "0" } else { "1" };
    Some(format!(
        "https://dict.youdao.com/dictvoice?type={voice_type}&audio={}",
        word.to_lowercase()
    ))
}

// ---------------------------------------------------------------------------
// 同步读接口（P2）：供 sync_server 做增量拉取与广播载荷
// ---------------------------------------------------------------------------

const WORD_COLS: &str = "seq, id, word, normalized, phonetic_us, phonetic_uk, audio_us, \
                         audio_uk, dit, usual_dict, exchange, added_at, updated_at";
const SENTENCE_COLS: &str = "seq, id, text, trans, added_at";

fn parse_json_column(raw: Option<String>, fallback: Value) -> Value {
    raw.and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .unwrap_or(fallback)
}

/// 行 → JSON。**增量拉取与广播推送共用这一个函数**，
/// 保证手机端无论从哪条路径拿到的记录，字段与形态都完全一致。
fn word_row_to_json(row: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    Ok(json!({
        "seq": row.get::<_, i64>(0)?,
        "id": row.get::<_, String>(1)?,
        "word": row.get::<_, String>(2)?,
        "normalized": row.get::<_, String>(3)?,
        "phonetic_us": row.get::<_, Option<String>>(4)?,
        "phonetic_uk": row.get::<_, Option<String>>(5)?,
        "audio_us": row.get::<_, Option<String>>(6)?,
        "audio_uk": row.get::<_, Option<String>>(7)?,
        "dit": row.get::<_, Option<String>>(8)?,
        "usual_dict": parse_json_column(row.get::<_, Option<String>>(9)?, json!([])),
        "exchange": parse_json_column(row.get::<_, Option<String>>(10)?, json!({})),
        "added_at": row.get::<_, i64>(11)?,
        "updated_at": row.get::<_, i64>(12)?,
    }))
}

fn sentence_row_to_json(row: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    Ok(json!({
        "seq": row.get::<_, i64>(0)?,
        "id": row.get::<_, String>(1)?,
        "text": row.get::<_, String>(2)?,
        "trans": row.get::<_, Option<String>>(3)?,
        "added_at": row.get::<_, i64>(4)?,
    }))
}

type RowMapper = fn(&rusqlite::Row<'_>) -> rusqlite::Result<Value>;

fn table_spec(table: &str) -> Result<(&'static str, RowMapper), String> {
    match table {
        "words" => Ok((WORD_COLS, word_row_to_json)),
        "sentences" => Ok((SENTENCE_COLS, sentence_row_to_json)),
        _ => Err(format!("未知的同步表: {table}")),
    }
}

/// 按 seq 取单条记录（收藏后用于广播）。
pub fn fetch_by_seq(conn: &Connection, table: &str, seq: i64) -> Result<Option<Value>, String> {
    let (cols, mapper) = table_spec(table)?;
    conn.query_row(
        &format!("SELECT {cols} FROM {table} WHERE seq = ?1"),
        params![seq],
        mapper,
    )
    .optional()
    .map_err(|e| format!("读取 {table} 记录失败: {e}"))
}

/// 增量拉取：返回 `seq > since` 的记录（升序）以及是否还有更多。
pub fn query_since(
    conn: &Connection,
    table: &str,
    since: i64,
    limit: i64,
) -> Result<(Vec<Value>, bool), String> {
    let (cols, mapper) = table_spec(table)?;
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {cols} FROM {table} WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2"
        ))
        .map_err(|e| format!("准备 {table} 增量查询失败: {e}"))?;

    // 多取一条用来判断 has_more
    let rows = stmt
        .query_map(params![since, limit + 1], mapper)
        .map_err(|e| format!("执行 {table} 增量查询失败: {e}"))?;

    let mut items = Vec::new();
    for row in rows {
        items.push(row.map_err(|e| format!("读取 {table} 记录失败: {e}"))?);
    }

    let has_more = items.len() as i64 > limit;
    if has_more {
        items.truncate(limit.max(0) as usize);
    }

    Ok((items, has_more))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn judge_sentence_matches_frontend_rules() {
        assert!(!judge_sentence("scout"));
        assert!(!judge_sentence("  well-being "));
        assert!(!judge_sentence("don't"));
        assert!(!judge_sentence("U.S.A."));
        assert!(judge_sentence("hello world"));
        assert!(judge_sentence("This is a sentence."));
        // 前端：一个英文词都没匹配到也算句子
        assert!(judge_sentence("你好"));
    }

    #[test]
    fn repeat_save_keeps_seq() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();

        let data = json!({
            "translate": { "text": "scout", "dit": "n. 侦察员" },
            "wordCard": { "show": true, "usualDict": [{"pos": "n.", "values": ["侦察员"]}] },
            "voice": { "phonetic": [{"type": "usa", "text": "/skaʊt/", "filename": ""}] }
        });

        let first = add_entry(&conn, &data).unwrap();
        assert_eq!(first.action, "added");
        assert_eq!(first.kind, "word");

        let second = add_entry(&conn, &data).unwrap();
        assert_eq!(second.action, "updated");
        assert_eq!(second.seq, first.seq, "重复收藏必须保持原 seq");

        let stats = get_stats(&conn).unwrap();
        assert_eq!(stats.word_count, 1);
        assert_eq!(stats.last_seq, first.seq);
    }

    #[test]
    fn sentence_and_word_go_to_separate_tables() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();

        add_entry(&conn, &json!({"translate": {"text": "scout", "dit": "x"}})).unwrap();
        add_entry(
            &conn,
            &json!({"translate": {"text": "the committee will reconvene", "dit": "委员会将重新开会"}}),
        )
        .unwrap();

        let stats = get_stats(&conn).unwrap();
        assert_eq!(stats.word_count, 1);
        assert_eq!(stats.sentence_count, 1);
        assert_eq!(stats.last_seq, 2);
    }

    /// 回归测试：两表必须共用同一个序号序列。
    /// 早期实现里两张表各自 AUTOINCREMENT，会双双拿到 seq=1，
    /// 手机端只用一个 `last_seq` 游标时就会漏数据。
    #[test]
    fn seq_is_global_across_tables() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();

        let word = add_entry(&conn, &json!({"translate": {"text": "scout", "dit": "x"}})).unwrap();
        let sentence = add_entry(
            &conn,
            &json!({"translate": {"text": "one two three", "dit": "一二三"}}),
        )
        .unwrap();

        assert_ne!(word.seq, sentence.seq, "两表序号不得重复");
        assert_eq!(sentence.seq, word.seq + 1);
        assert_eq!(get_stats(&conn).unwrap().last_seq, 2);
    }
}
