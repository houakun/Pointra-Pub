//! 生词本 / 句子本相关命令。
//!
//! 收藏的数据来源是 `fetch_trans_res` 缓存的「最近一次翻译结果」，
//! 因此界面按钮与全局快捷键（默认 `Alt+S`）走的是完全相同的逻辑。

use crate::utils::vocab as vocab_db;
use crate::utils::vocab::{VocabAddResult, VocabStats};
use crate::AppState;
use serde_json::json;
use tauri::{AppHandle, Emitter, State};

/// 收藏当前翻译结果。
///
/// `notify` 为 `true` 时（快捷键路径）额外向前端发事件弹 Toast；
/// 界面按钮路径传 `false`，由按钮自己根据返回值弹提示，避免重复弹两次。
pub fn add_current(
    app: &AppHandle,
    state: &AppState,
    notify: bool,
) -> Result<VocabAddResult, String> {
    let outcome = (|| -> Result<VocabAddResult, String> {
        let data = {
            let guard = state.last_trans.lock().unwrap();
            guard
                .clone()
                .ok_or_else(|| "暂无可收藏内容，请先查词".to_string())?
        };

        let guard = state.vocab.lock().unwrap();
        let conn = guard.as_ref().ok_or_else(|| "生词库不可用".to_string())?;
        vocab_db::add_entry(conn, &data)
    })();

    // 收藏成功 → 即时广播给在线手机端；
    // 没有连接时 send 会失败并忽略，手机下次靠 since 补拉
    if let Ok(result) = &outcome {
        state.sync.broadcast_add(result);
    }

    if notify {
        match &outcome {
            Ok(result) => app
                .emit("win-event", json!({ "type": "vocab", "data": result }))
                .ok(),
            Err(msg) => app
                .emit("win-event", json!({ "type": "vocab-error", "data": msg }))
                .ok(),
        };
    }

    outcome
}

/// 前端「收藏」按钮与 `Alt+S` 的公共入口。
#[tauri::command]
pub async fn vocab_add_current(
    app: AppHandle,
    state: State<'_, AppState>,
) -> Result<VocabAddResult, String> {
    add_current(&app, state.inner(), false)
}

/// 生词库统计（设置页展示，也用于确认收藏是否真的落库）。
#[tauri::command]
pub async fn vocab_get_stats(state: State<'_, AppState>) -> Result<VocabStats, String> {
    let guard = state.vocab.lock().unwrap();
    let conn = guard.as_ref().ok_or_else(|| "生词库不可用".to_string())?;
    vocab_db::get_stats(conn)
}
