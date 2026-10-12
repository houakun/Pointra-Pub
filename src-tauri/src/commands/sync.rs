//! 手机同步服务的命令层。
//!
//! 配置（开关/端口）刻意**不放进 `AppConfig`**：`AppConfig` 由设置页整体覆写，
//! 若混在一起，用户在「快捷按键」里保存一次就会把同步配置打回默认值。
//! 这里改为独立存储（`sync.json`）+ 独立命令，与 `ConfigManager` 完全解耦。

use crate::utils::sync_server::{PairingInfo, SyncStatus};
use crate::AppState;
use tauri::State;

#[tauri::command]
pub async fn sync_get_status(state: State<'_, AppState>) -> Result<SyncStatus, String> {
    Ok(state.sync.status())
}

/// 保存开关与端口，并让服务状态与之一致。
#[tauri::command]
pub async fn sync_apply(
    state: State<'_, AppState>,
    enabled: bool,
    port: u16,
) -> Result<SyncStatus, String> {
    state.sync.set_config(enabled, port)?;

    if enabled {
        if !state.sync.is_running() {
            state.sync.start()?;
        }
    } else {
        state.sync.stop();
    }

    Ok(state.sync.status())
}

/// 取配对码与二维码（未运行时自动启动，避免用户来回点两次）。
#[tauri::command]
pub async fn sync_get_pairing(state: State<'_, AppState>) -> Result<PairingInfo, String> {
    if !state.sync.is_running() {
        state.sync.start()?;
    }
    Ok(state.sync.pairing_info())
}

#[tauri::command]
pub async fn sync_unpair(
    state: State<'_, AppState>,
    device_id: String,
) -> Result<SyncStatus, String> {
    state.sync.unpair(&device_id);
    Ok(state.sync.status())
}
