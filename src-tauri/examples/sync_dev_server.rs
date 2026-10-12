//! 开发用同步服务（仅供本机联调，不参与 App 打包）。
//!
//! 用途：在模拟器里验证手机端配对 / 全量拉取。它会：
//! 1. 在指定的数据目录里建 `vocab.db` 并按需灌入 N 个单词 + M 个句子；
//! 2. 用**与正式 App 完全相同的** `SyncManager` 在 `0.0.0.0:31415` 起服务；
//! 3. 打印一次配对码，然后常驻。
//!
//! 运行：
//! ```text
//! cargo run --example sync_dev_server -- <data_dir> [words] [sentences] [port]
//! ```
//! 端口放最后，方便和宿主上已在跑的正式 App（占着 31415）共存。

use pointra_lib::utils::sync_server::SyncManager;
use pointra_lib::utils::vocab;
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = PathBuf::from(
        args.next()
            .unwrap_or_else(|| "D:/pointra-dev-server".to_string()),
    );
    let words: i64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(30_000);
    let sentences: i64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(50);
    // 端口可改：本机正式 App 通常已占着 31415
    let port: u16 = args.next().and_then(|v| v.parse().ok()).unwrap_or(31415);

    // SyncManager::start 内部用 tauri::async_runtime::spawn，先给它一个 tokio 运行时
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let _ = tauri::async_runtime::set(rt.handle().clone());

    let db_path = dir.join("vocab.db");
    let conn = vocab::open(&db_path).expect("打开生词库失败");

    let stats = vocab::get_stats(&conn).expect("统计失败");
    println!(
        "[dev] 灌库前：words={} sentences={} last_seq={}",
        stats.word_count, stats.sentence_count, stats.last_seq
    );

    if stats.word_count < words {
        let tx = conn.unchecked_transaction().expect("开启事务失败");
        for i in stats.word_count..words {
            let word = format!("word{i:05}");
            vocab::add_entry(
                &tx,
                &json!({
                    "translate": { "text": word, "dit": format!("释义 {i}") },
                    "wordCard": {
                        "usualDict": [{ "pos": "n.", "values": [format!("释义 {i}")] }],
                        "exchange": {}
                    },
                    "voice": {}
                }),
            )
            .expect("写入单词失败");
        }
        tx.commit().expect("提交失败");
        println!("[dev] 已灌入 {} 个单词", words - stats.word_count);
    }

    let stats = vocab::get_stats(&conn).expect("统计失败");
    if stats.sentence_count < sentences {
        let tx = conn.unchecked_transaction().expect("开启事务失败");
        for i in stats.sentence_count..sentences {
            let text = format!("this is dev sentence number {i}");
            vocab::add_entry(
                &tx,
                &json!({
                    "translate": { "text": text, "dit": format!("开发用句子 {i}") },
                    "voice": {}
                }),
            )
            .expect("写入句子失败");
        }
        tx.commit().expect("提交失败");
        println!("[dev] 已灌入 {} 个句子", sentences - stats.sentence_count);
    }

    let stats = vocab::get_stats(&conn).expect("统计失败");
    drop(conn);

    let mgr = SyncManager::new(&dir);
    mgr.set_config(true, port).expect("写入同步配置失败");
    let port = mgr.start().expect("启动同步服务失败");
    let info = mgr.pairing_info();

    println!("[dev] 数据目录 = {}", dir.display());
    println!(
        "[dev] PC 端计数：words={} sentences={} last_seq={}",
        stats.word_count, stats.sentence_count, stats.last_seq
    );
    println!("[dev] 服务监听 = 0.0.0.0:{port}");
    println!("[dev] 配对码 = {}", info.code);
    println!("[dev] 配对地址 = {}", info.address);
    println!("READY code={} port={}", info.code, port);

    // 开发用：每 400ms 扫一次 `<data_dir>/dev-add.txt`，把新增行当作
    // 「PC 端刚刚按下 Alt+S 收藏了一个词」，写库并广播给在线手机。
    // 用来验证「前台 1 秒内出现」与「断线后 since 补拉」。
    {
        let dir = dir.clone();
        let mgr = mgr.clone();
        std::thread::spawn(move || {
            let watch = dir.join("dev-add.txt");
            let mut processed = 0usize;
            loop {
                std::thread::sleep(Duration::from_millis(400));
                let Ok(text) = std::fs::read_to_string(&watch) else {
                    continue;
                };
                let lines: Vec<String> = text
                    .lines()
                    .map(|l| l.trim().to_string())
                    .filter(|l| !l.is_empty())
                    .collect();
                while processed < lines.len() {
                    let word = lines[processed].clone();
                    processed += 1;
                    let Ok(conn) = vocab::open(&dir.join("vocab.db")) else {
                        continue;
                    };
                    let data = json!({
                        "translate": { "text": word, "dit": format!("推送测试 {word}") },
                        "voice": {}
                    });
                    match vocab::add_entry(&conn, &data) {
                        Ok(res) => {
                            println!("[dev] 已收藏 {} seq={} -> 广播", word, res.seq);
                            mgr.broadcast_add(&res);
                        }
                        Err(err) => println!("[dev] 收藏失败: {err}"),
                    }
                }
            }
        });
    }

    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
