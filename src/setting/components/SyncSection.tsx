import { invoke } from "@tauri-apps/api/core";
import { useEffect, useState } from "react";
import { toast } from "@heroui/react";
import {
    ArrowsRotateLeft,
    CircleCheck,
    CirclePlayFill,
    Copy,
    TrashBin,
} from "@gravity-ui/icons";
import type { PairingInfo, SyncStatus } from "../types";

/** 秒 → 「x 分 y 秒」 */
function formatLeft(ms: number) {
    const total = Math.max(0, Math.floor(ms / 1000));
    return `${Math.floor(total / 60)} 分 ${String(total % 60).padStart(2, "0")} 秒`;
}

export default function SyncSection() {
    const [status, setStatus] = useState<SyncStatus | null>(null);
    const [pairing, setPairing] = useState<PairingInfo | null>(null);
    const [portInput, setPortInput] = useState("");
    const [busy, setBusy] = useState(false);
    const [now, setNow] = useState(Date.now());

    const refresh = async () => {
        try {
            const s = await invoke<SyncStatus>("sync_get_status");
            setStatus(s);
            setPortInput(String(s.port || ""));
        } catch (err) {
            toast.danger(String(err), { timeout: 2500 });
        }
    };

    useEffect(() => {
        refresh();
    }, []);

    // 配对码倒计时
    useEffect(() => {
        const timer = setInterval(() => setNow(Date.now()), 1000);
        return () => clearInterval(timer);
    }, []);

    const apply = async (enabled: boolean, port?: number) => {
        if (busy) return;
        setBusy(true);
        try {
            const parsed = port ?? Number(portInput);
            if (enabled && (!Number.isInteger(parsed) || parsed < 1024 || parsed > 65535)) {
                toast.warning("端口需在 1024-65535 之间", { timeout: 2500 });
                return;
            }
            const s = await invoke<SyncStatus>("sync_apply", {
                enabled,
                port: enabled ? parsed : status?.port ?? 31415,
            });
            setStatus(s);
            setPortInput(String(s.port));
            if (enabled) {
                toast.success(`同步服务已启动：${s.address}`, { timeout: 2500 });
            } else {
                toast.success("同步服务已停止", { timeout: 1800 });
                setPairing(null);
            }
        } catch (err) {
            toast.danger(String(err), { timeout: 3500 });
        } finally {
            setBusy(false);
        }
    };

    const requestPairing = async () => {
        try {
            const info = await invoke<PairingInfo>("sync_get_pairing");
            setPairing(info);
            await refresh();
        } catch (err) {
            toast.danger(String(err), { timeout: 3000 });
        }
    };

    const unpair = async (deviceId: string, name: string) => {
        try {
            const s = await invoke<SyncStatus>("sync_unpair", { deviceId });
            setStatus(s);
            toast.success(`已解绑「${name}」，其凭证立即失效`, { timeout: 2500 });
        } catch (err) {
            toast.danger(String(err), { timeout: 3000 });
        }
    };

    const expired = pairing !== null && pairing.expires_at <= now;
    const running = status?.running ?? false;

    const Row = ({
        title,
        desc,
        children,
    }: {
        title: string;
        desc: string;
        children: React.ReactNode;
    }) => (
        <div className="flex items-center justify-between p-3 min-h-13 gap-3">
            <div className="flex-1 min-w-0">
                <p className="text-sm text-mainTitleW">{title}</p>
                <p className="text-[11px] text-tagSecondW mt-0.5">{desc}</p>
            </div>
            <div className="flex items-center gap-2 shrink-0">{children}</div>
        </div>
    );

    return (
        <section>
            <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                    <div className="w-1 h-4 rounded-full bg-mainBlueW" />
                    <h2 className="text-sm font-medium text-mainTitleW">
                        手机同步
                    </h2>
                </div>
                <p className="text-[11px] text-tagSecondW mt-px rounded-md">
                    Sync
                </p>
            </div>

            <div className="rounded-xl border border-borderMainW divide-y divide-borderSubW bg-white overflow-hidden">
                {/* 开关 */}
                <Row
                    title="启用局域网同步"
                    desc="手机与电脑需在同一个 WiFi 下"
                >
                    <button
                        onClick={() => apply(!(status?.enabled ?? false))}
                        disabled={busy}
                        className={`relative w-11 h-6 rounded-full transition-colors cursor-pointer
                            ${status?.enabled ? "bg-mainBlueW" : "bg-gray-300"}
                            ${busy ? "opacity-60" : ""}`}
                    >
                        <span
                            className={`absolute top-0.5 w-5 h-5 rounded-full bg-white shadow transition-all
                                ${status?.enabled ? "left-5.5" : "left-0.5"}`}
                        />
                    </button>
                </Row>

                {/* 端口 */}
                <Row title="监听端口" desc="局域网内手机连接的端口">
                    <input
                        value={portInput}
                        onChange={(e) => setPortInput(e.target.value)}
                        onBlur={() => {
                            if (running && Number(portInput) !== status?.port) {
                                apply(true);
                            }
                        }}
                        onKeyDown={(e) => {
                            if (e.key === "Enter") (e.target as HTMLInputElement).blur();
                        }}
                        inputMode="numeric"
                        className="w-20 h-7 px-2 text-xs text-center rounded-md border border-borderMainW
                                   outline-none focus:border-mainBlueW text-mainTitleW"
                    />
                </Row>

                {/* 运行状态 */}
                <Row
                    title="服务状态"
                    desc={
                        running
                            ? `${status?.address}　在线设备 ${status?.connections ?? 0}`
                            : "未启动，收藏的词只存在本机"
                    }
                >
                    <span
                        className={`flex items-center gap-1 text-[11px] px-2 py-0.5 rounded border
                            ${
                                running
                                    ? "text-emerald-600 bg-emerald-50 border-emerald-200"
                                    : "text-tagSecondW bg-gray-50 border-gray-200"
                            }`}
                    >
                        {running ? (
                            <CircleCheck width={12} height={12} />
                        ) : (
                            <CirclePlayFill width={12} height={12} />
                        )}
                        {running ? "运行中" : "已停止"}
                    </span>
                </Row>

                {/* 词库统计 */}
                <Row
                    title="本地词库"
                    desc={`词 ${status?.word_count ?? 0} · 句 ${
                        status?.sentence_count ?? 0
                    } · 同步序号 ${status?.last_seq ?? 0}`}
                >
                    <span className="text-[11px] text-tagSecondW">只读</span>
                </Row>

                {/* 配对 */}
                <div className="p-3">
                    <div className="flex items-center justify-between">
                        <div>
                            <p className="text-sm text-mainTitleW">手机配对</p>
                            <p className="text-[11px] text-tagSecondW mt-0.5">
                                手机 App 里填写地址与 6 位配对码（5 分钟内有效，仅能用一次）
                            </p>
                        </div>
                        <button
                            onClick={requestPairing}
                            disabled={busy}
                            className="flex items-center gap-1 h-7 px-2.5 rounded-md border border-blueBorderW
                                       bg-blueBgW text-mainBlueW text-xs cursor-pointer
                                       hover:brightness-105 active:scale-95 transition-all"
                        >
                            <ArrowsRotateLeft width={12} height={12} />
                            {pairing ? "重新生成" : "生成配对码"}
                        </button>
                    </div>

                    {pairing && (
                        <div className="mt-3 flex gap-3 items-start">
                            <div
                                className="w-27 h-27 shrink-0 rounded-lg border border-borderSubW p-1 bg-white
                                           [&>svg]:w-full [&>svg]:h-full"
                                dangerouslySetInnerHTML={{ __html: pairing.qr_svg }}
                            />
                            <div className="flex-1 min-w-0">
                                <p className="text-[10px] text-tagW">配对码</p>
                                <p
                                    className={`text-2xl font-semibold tracking-[0.2em] ${
                                        expired ? "text-gray-300" : "text-mainTitleW"
                                    }`}
                                >
                                    {pairing.code}
                                </p>
                                <p className="text-[11px] text-tagSecondW mt-1">
                                    {expired
                                        ? "已过期，请重新生成"
                                        : `剩余 ${formatLeft(pairing.expires_at - now)}`}
                                </p>
                                <div className="flex items-center gap-1 mt-1.5">
                                    <p className="text-[11px] text-mainBlueW truncate">
                                        模拟器用 http://10.0.2.2:{pairing.port}
                                    </p>
                                    <Copy
                                        width={12}
                                        height={12}
                                        color="#4a90d9"
                                        className="cursor-pointer shrink-0"
                                        onClick={() => {
                                            navigator.clipboard.writeText(
                                                `http://10.0.2.2:${pairing.port}`,
                                            );
                                            toast.success("已复制（模拟器地址）", {
                                                timeout: 1500,
                                            });
                                        }}
                                    />
                                </div>
                                <p className="text-[11px] text-tagSecondW truncate">
                                    真机用 {pairing.address}
                                </p>
                            </div>
                        </div>
                    )}
                </div>

                {/* 已配对设备 */}
                {status && status.devices.length > 0 && (
                    <div className="p-3">
                        <p className="text-sm text-mainTitleW mb-2">已配对设备</p>
                        <div className="flex flex-col gap-1.5">
                            {status.devices.map((device) => (
                                <div
                                    key={device.id}
                                    className="flex items-center justify-between rounded-lg border border-borderSubW px-2.5 py-1.5"
                                >
                                    <div className="min-w-0">
                                        <p className="text-xs text-mainTitleW truncate">
                                            {device.name}
                                        </p>
                                        <p className="text-[10px] text-tagSecondW">
                                            最近活跃{" "}
                                            {new Date(
                                                device.last_seen,
                                            ).toLocaleString()}
                                        </p>
                                    </div>
                                    <button
                                        onClick={() =>
                                            unpair(device.id, device.name)
                                        }
                                        className="flex items-center justify-center w-6 h-6 rounded-md border
                                                   border-borderMainW text-tagSecondW cursor-pointer
                                                   hover:bg-red-50 hover:text-red-500 hover:border-red-200
                                                   transition-all active:scale-90"
                                    >
                                        <TrashBin width={12} height={12} />
                                    </button>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </div>

            <p className="text-[10px] text-tagW mt-2 leading-relaxed">
                首次启动时 Windows 可能弹出防火墙提示，请选择「允许访问」，否则手机无法连接。
                本服务只在局域网内明文运行，不含账号与云端上传。
            </p>
        </section>
    );
}
