#![allow(unexpected_cfgs)]

mod commands;
// `pub` 仅为 `examples/sync_dev_server.rs`（本机联调用开发工具）能复用同步服务与
// 生词库代码；主程序功能不受影响。
pub mod utils;
use commands::access_mac::{check_permission, request_permission};
use commands::audio::{play_phonetic_url, AudioState};
use commands::config::{complete_setup, get_config, update_config};
use commands::sync::{sync_apply, sync_get_pairing, sync_get_status, sync_unpair};
use commands::translate::fetch_trans_res;
use commands::vocab::{vocab_add_current, vocab_get_stats};
use commands::window::{stop_shortcuts, trigger_update_check, update_hover_status};
use reqwest::Client;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use tauri::Manager;
use tauri_plugin_autostart::ManagerExt;
use tauri_plugin_global_shortcut::ShortcutState;
use utils::define_config::{get_config_from_store, AppConfig};

#[cfg(target_os = "macos")]
use utils::{ocr_mac::OcrState, set_window::apply_window_effects};

#[cfg(target_os = "windows")]
use utils::ocr_win::OcrState;

use utils::shortcuts::{handle_shortcut_event, init_point_listener, init_shortcuts};
use utils::sync_server::SyncManager;
use utils::tray;

pub struct AppState {
    ocr_state: Arc<OcrState>,
    client: Client,
    window_locked: Arc<AtomicBool>,
    audio_state: AudioState,
    config: Mutex<AppConfig>,
    /// 最近一次成功翻译的结果，供 Alt+S / 收藏按钮使用
    last_trans: Mutex<Option<serde_json::Value>>,
    /// 生词库（SQLite）。初始化失败时为 None，此时收藏会返回可读的错误而不是崩溃
    vocab: Mutex<Option<rusqlite::Connection>>,
    /// 局域网同步服务（配对 / 增量拉取 / WebSocket 推送）
    sync: SyncManager,
}

pub fn run() {
    // `mut` 只在 macOS 分支（tauri_nspanel 插件）里需要，
    // 不要为了让 Windows 少一条 warning 就删掉它，否则 macOS 编译会失败。
    #[allow(unused_mut)]
    let mut builder = tauri::Builder::default()
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_autostart::Builder::new().build());

    #[cfg(target_os = "macos")]
    {
        builder = builder.plugin(tauri_nspanel::init());
    }

    builder
        // 注册命令
        .invoke_handler(tauri::generate_handler![
            update_hover_status,
            fetch_trans_res,
            play_phonetic_url,
            get_config,
            update_config,
            complete_setup,
            stop_shortcuts,
            check_permission,
            request_permission,
            trigger_update_check,
            vocab_add_current,
            vocab_get_stats,
            sync_get_status,
            sync_apply,
            sync_get_pairing,
            sync_unpair
        ])
        // 初始化应用状态
        .setup(|app| {
            let config = get_config_from_store(app.handle()).unwrap_or_default();

            // 数据目录：生词库与同步配对信息都放在这里。
            // 初始化失败不致命（比如磁盘只读），收藏时会返回明确错误。
            let data_dir = match app.path().app_data_dir() {
                Ok(dir) => {
                    let _ = std::fs::create_dir_all(&dir);
                    Some(dir)
                }
                Err(err) => {
                    eprintln!("[Pointra][数据目录] 无法获取 app_data_dir: {err}");
                    None
                }
            };

            let vocab_conn =
                data_dir
                    .as_ref()
                    .and_then(|dir| match utils::vocab::open(&dir.join("vocab.db")) {
                        Ok(conn) => Some(conn),
                        Err(err) => {
                            eprintln!("[Pointra][生词库] 初始化失败: {err}");
                            None
                        }
                    });

            let sync_manager = match &data_dir {
                Some(dir) => SyncManager::new(dir),
                // 拿不到数据目录时退到临时目录，至少不让应用启动失败
                None => SyncManager::new(&std::env::temp_dir()),
            };
            let sync_enabled = sync_manager.is_enabled();

            // 先初始化 AppState（后续命令依赖它）
            app.manage(AppState {
                ocr_state: OcrState::new(),
                client: Client::new(),
                window_locked: Arc::new(AtomicBool::new(false)),
                audio_state: AudioState::new(),
                config: Mutex::new(config.clone()),
                last_trans: Mutex::new(None),
                vocab: Mutex::new(vocab_conn),
                sync: sync_manager.clone(),
            });

            #[cfg(target_os = "macos")]
            {
                // macOS 首次启动检测权限
                if !config.first_setup_completed {
                    let (acc, screen) = unsafe {
                        (
                            crate::utils::macos_api::check_accessibility(),
                            crate::utils::macos_api::check_screen_recording(),
                        )
                    };

                    if !(acc && screen) {
                        // 权限未授予，打开设置界面引导用户配置
                        tray::create_setting_win(app.handle());
                        // 跳过托盘、快捷键等初始化，重启后会正常加载
                        return Ok(());
                    } else {
                        // 权限已全部授予，标记首次设置完成
                        let mut updated_config = config.clone();
                        updated_config.first_setup_completed = true;
                        let _ = crate::utils::define_config::save_config_to_store(
                            app.handle(),
                            &updated_config,
                        );
                    }
                }

                app.set_activation_policy(tauri::ActivationPolicy::Accessory);
                apply_window_effects(app.handle().clone());
            }

            // 确保自动启动状态与配置一致
            let autostart_manager = app.autolaunch();
            if config.auto_start && !autostart_manager.is_enabled().unwrap_or(false) {
                let _ = autostart_manager.enable();
            } else if !config.auto_start && autostart_manager.is_enabled().unwrap_or(false) {
                let _ = autostart_manager.disable();
            }

            // 按上次的开关自动恢复同步服务
            if sync_enabled {
                if let Err(err) = sync_manager.start() {
                    eprintln!("[Pointra][同步] 自动启动失败: {err}");
                }
            }

            // 初始化托盘
            tray::init(app)?;

            init_point_listener(app.handle().clone());
            init_shortcuts(app.handle());
            Ok(())
        })
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_os::init())
        .plugin(
            tauri_plugin_global_shortcut::Builder::new()
                .with_handler(|app, shortcut, event| {
                    if event.state() == ShortcutState::Pressed {
                        handle_shortcut_event(app, shortcut);
                    }
                })
                .build(),
        )
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
