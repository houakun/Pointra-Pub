/** 一条快捷键配置 */
export interface ShortcutItem {
    id: string;
    label: string;
    keys: string[];
    defaultKeys: string[];
    save: {
        id: string;
        index: number;
    }[];
}

/** 通用设置项 */
export interface GeneralSetting {
    id: string;
    label: string;
    description: string;
    value: boolean | string | number;
}

/** 真正的配置项，定义在Rust */
export interface RustAppConfig {
    theme: string;
    auto_start: boolean;
    auto_play: boolean;
    pronunciation: string;
    pronunciation_volume: number;
    point_key: string;
    pinned_key: string;
    hide_win_key: string;
    select_text_modifiers: string;
    select_text_code: string;
    search_win_modifiers: string;
    search_win_code: string;
    vocab_modifiers: string;
    vocab_code: string;
    first_setup_completed: boolean;
}

/** 已配对的手机（不含 token，后端不会下发凭证） */
export interface SyncDevice {
    id: string;
    name: string;
    paired_at: number;
    last_seen: number;
}

/** PC 端同步服务状态 */
export interface SyncStatus {
    enabled: boolean;
    running: boolean;
    port: number;
    address: string;
    device_id: string;
    devices: SyncDevice[];
    connections: number;
    word_count: number;
    sentence_count: number;
    last_seq: number;
}

/** 一次配对会话（配对码 5 分钟有效且一次性） */
export interface PairingInfo {
    code: string;
    expires_at: number;
    address: string;
    port: number;
    qr_svg: string;
}

/** 收藏结果（Rust 端 `VocabAddResult`） */
export interface VocabAddResult {
    kind: "word" | "sentence";
    action: "added" | "updated";
    text: string;
    seq: number;
}
