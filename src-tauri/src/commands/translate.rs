use crate::utils::trans_server::{fetch_translation, TransResult};
use crate::AppState;
use tauri::{AppHandle, State};

#[tauri::command]
pub async fn fetch_trans_res(
    state: State<'_, AppState>,
    app: AppHandle,
    word: String,
) -> Result<TransResult, ()> {
    match fetch_translation(&word, &state, &app).await {
        Ok(data) => {
            // 缓存最近一次成功的结果：Alt+S 收藏与界面上的收藏按钮都依赖它。
            // 这里存的就是「主窗口当前正显示的那一条」，所以两者语义一致。
            *state.last_trans.lock().unwrap() = Some(data.clone());
            Ok(TransResult::success(data))
        }
        Err(err_msg) => Ok(TransResult::fail(err_msg)),
    }
}
