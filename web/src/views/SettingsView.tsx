import { useCallback, useEffect, useState } from "react";
import { ApiError, delJSON, getJSON, postJSON } from "../api/client";
import { ENDPOINTS } from "../api/endpoints";
import { getActor, setActor } from "../api/token";
import type {
  ChannelKind, ChannelWriteResponse, ChannelsResponse, NotifyChannel,
} from "../api/types";
import { PageHead } from "../components/Layout";
import { Panel, Banner, Loading } from "../components/Panel";
import { DataTable, type Column } from "../components/DataTable";
import { fmtTime, timeAgoText } from "../lib/format";

/**
 * 视图五 · 设置 · 通知渠道（W9-2 迁移批 · web 实装）。
 *
 * 契约：cmd/opscopilot/rest_notify.go + notify_channels.go（后端已就绪，本批零 Go 改动）；
 * 交互冻结参照：console.html view-settings——列表（含禁用）/新增/启停软开关/删除，
 * 内置 console 兜底渠道不落库也不可被配置占用（保留名，后端 400），故其行不渲染删除。
 *
 * 校验总口径：**前端规则逐条镜像后端 ValidateChannel**（见下方常量注释），后端仍是
 * 最终裁判——写 body 字段严格 = {name,kind,url,min_severity,enabled}（decodeStrict
 * DisallowUnknownFields，多传任何字段即 400），后端回包错误文案（400/401/404，
 * 若未来出现 409 亦同理）一律原样透出到横幅。
 *
 * 降级同族：无 OPS_DB_DSN 时 store 未接线 → GET/POST 显式 503 "not wired"——
 * 列表变灰 + 横幅明示（Runbook/Audit 同口径，不伪装"没有渠道"）。
 *
 * 关于"可选超时"：后端 REST 契约**没有** timeout 字段（WebhookOptions.Timeout 只在
 * Go 构造期、恒取默认 5s；loadChannelsIntoRegistry 不传），冻结参照表单亦无——
 * 遵循"以后端为准"，本表单不提供该输入，防止偷传未知字段换来 400。
 */

/** 渠道名规则：逐字抄 notify_channels.go channelNameRe——字母/数字开头，其后允许
 *  字母数字与 . _ -，总长 1~64。后端先 TrimSpace 再验，前端同口径先 trim。 */
const CHANNEL_NAME_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

/** 保留名（notify_channels.go ChannelConsoleName）：内置兜底渠道恒在注册表
 *  （enforce 下零渠道 = 通知静默丢失，console 至少留痕），不允许被配置占用。 */
const RESERVED_NAME = "console";

/** kind 封闭集（notify.ValidKind：generic|feishu|wecom）。下拉即白名单。 */
const KINDS: { v: ChannelKind; label: string }[] = [
  { v: "generic", label: "generic（自建 JSON）" },
  { v: "feishu", label: "feishu（飞书机器人）" },
  { v: "wecom", label: "wecom（企微机器人）" },
];
/** 列表类型徽标文案（console.html CH_KIND_LABEL 同口径；DB 脏值回退原样显示）。 */
const KIND_LABEL: Record<string, string> = {
  generic: "自建 JSON", feishu: "飞书", wecom: "企业微信",
};

/** min_severity 封闭集（notify.ValidSeverity；省略/空 = info 全收，W9-3 路由）。
 *  下拉文案与 console.html chSeverity 一致。 */
const SEVERITIES: { v: "info" | "warning" | "critical"; label: string }[] = [
  { v: "info", label: "info（全收）" },
  { v: "warning", label: "warning（告警及以上）" },
  { v: "critical", label: "critical（仅严重）" },
];

/** URL 协议白名单：后端为大小写敏感的 strings.HasPrefix 判定（"HTTP://" 同样拒），
 *  前端逐字对齐；trim 后判前缀（rest_notify.go 同口径）。 */
const URL_PREFIXES = ["http://", "https://"] as const;
/** URL 长度上限（ValidateChannel：trim 后 ≤2048 字符）。 */
const URL_MAX_LEN = 2048;

/**
 * 列表 URL 脱敏：只保留 scheme+host(+port) 与 **首段** path，其余 path 段与全部
 * query/fragment 一律隐藏。理由：IM webhook 的凭据恰好藏在被裁掉的部分——飞书
 * hook token 在 path 尾部（/open-apis/bot/v2/hook/<token>）、企微 key 在 query
 * （?key=<secret>）；列表页是常开视图，全量 URL 上屏等于把机器人密钥摊进
 * 屏幕共享/DOM 审查。因此完整 URL 连 title 提示都不进 DOM；本期无"编辑回填"
 * 交互（同名 upsert = 重新填写），若未来要回填需带鉴权的单渠道端点另行设计。
 * 形态异常的历史脏值回 "…"（同样不显示原文）。
 */
function maskChannelUrl(u: string): string {
  const m = /^(https?:\/\/[^/?#]+)(?:\/([^/?#]+))?/.exec(u ?? "");
  if (!m) return u ? "…" : "—";
  const seg = (m[2] ?? "").slice(0, 24); // 首段限长，极端脏值不撑破表格
  return m[2] === undefined ? `${m[1]}/…` : `${m[1]}/${seg}…`;
}

type FieldKey = "name" | "kind" | "url";
type FieldErrors = Partial<Record<FieldKey, string>>;

/** 前端校验 = 后端 ValidateChannel 的镜像（消息文案对齐错误语义，就地标红）。
 *  返回空对象才允许发请求；后端 400 仍会原样透出兜底（两裁判不许打架）。 */
function validateDraft(name: string, kind: string, url: string): FieldErrors {
  const errs: FieldErrors = {};
  const n = name.trim();
  if (!n) {
    errs.name = "名称必填";
  } else if (n === RESERVED_NAME) {
    errs.name = `“${RESERVED_NAME}”为内置兜底渠道保留名，不可配置（后端同名 400）`;
  } else if (!CHANNEL_NAME_RE.test(n)) {
    errs.name = "名称需匹配 ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$：字母/数字开头，可含 . _ -，总长 ≤64";
  }
  if (!KINDS.some((k) => k.v === kind)) errs.kind = "类型必须 ∈ generic|feishu|wecom";
  const u = url.trim();
  if (!u) {
    errs.url = "URL 必填";
  } else if (!URL_PREFIXES.some((p) => u.startsWith(p))) {
    errs.url = "URL 需以 http:// 或 https:// 开头（协议大小写敏感）";
  } else if (u.length > URL_MAX_LEN) {
    errs.url = `URL 过长（上限 ${URL_MAX_LEN} 字符）`;
  }
  return errs;
}

/** 错误文案（RunbookPanel 同口径）：401 补"需填 Token"指路，其余透出后端原文。 */
function errText(e: unknown): string {
  return e instanceof ApiError
    ? e.message + (e.needsToken ? "（写操作需填 Token，可在「事件」页右上 Token 框输入）" : "")
    : String(e);
}

export function SettingsView(): React.ReactElement {
  const [rows, setRows] = useState<NotifyChannel[] | null>(null);
  const [off, setOff] = useState(false); // GET 503 = store 未接线（无 OPS_DB_DSN）
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState(""); // 操作级错误横幅（加载/保存/启停/删除）
  const [warn, setWarn] = useState(""); // 写成功但热重载失败的 warning（后端原文）
  const [busyName, setBusyName] = useState(""); // 正在启停/删除的渠道名
  const [saving, setSaving] = useState(false);

  // 新增表单（+ 操作人：仅按 token.ts 的 sessionStorage 口径本地留存复用——
  // 渠道表无 actor 列、body 未知字段即 400，绝不进 POST）
  const [formOpen, setFormOpen] = useState(false);
  const [name, setName] = useState("");
  const [kind, setKind] = useState<ChannelKind>("generic");
  const [sev, setSev] = useState<"info" | "warning" | "critical">("info");
  const [url, setUrl] = useState("");
  const [actorDraft, setActorDraft] = useState(getActor());
  const [errs, setErrs] = useState<FieldErrors>({});

  const load = useCallback(async () => {
    setLoading(true);
    setErr("");
    try {
      const data = await getJSON<ChannelsResponse>(ENDPOINTS.notifyChannels);
      setRows(data.channels ?? []);
      setOff(false);
    } catch (e) {
      if (e instanceof ApiError && (e.status === 503 || e.needsDb)) { setOff(true); return; }
      setErr(`渠道加载失败：${errText(e)}`);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  async function submit(): Promise<void> {
    const v = validateDraft(name, kind, url);
    setErrs(v);
    if (Object.keys(v).length > 0) return; // 必被后端 400 的请求不发：错误就地标红
    setSaving(true);
    setErr("");
    setWarn("");
    try {
      const out = await postJSON<ChannelWriteResponse>(ENDPOINTS.notifyChannels, {
        name: name.trim(),
        kind,
        url: url.trim(),
        min_severity: sev,
        enabled: true, // 创建即启用（console.html 同形；软开关后续可调）
      });
      if (actorDraft.trim()) setActor(actorDraft.trim()); // 操作人只落 token.ts 口径
      if (out?.warning) setWarn(out.warning); // 已落库但重载失败：明示不吞
      setName("");
      setUrl("");
      setErrs({});
      setFormOpen(false);
      await load(); // 成功后列表刷新（updated_at 以服务端为准）
    } catch (e) {
      setErr(`保存失败：${errText(e)}`); // 400/409/401 等后端文案原样透出
    } finally {
      setSaving(false);
    }
  }

  async function toggleEnabled(c: NotifyChannel, next: boolean): Promise<void> {
    setErr("");
    setBusyName(c.name);
    // 乐观翻牌：失败回滚开关状态并横幅报错（禁而不删——软开关不清配置）
    setRows((p) => (p ?? []).map((r) => (r.name === c.name ? { ...r, enabled: next } : r)));
    try {
      const out = await postJSON<ChannelWriteResponse>(ENDPOINTS.notifyChannelEnabled(c.name), { enabled: next });
      if (out?.warning) setWarn(out.warning);
      await load();
    } catch (e) {
      setRows((p) => (p ?? []).map((r) => (r.name === c.name ? { ...r, enabled: !next } : r)));
      setErr(`启停失败：${errText(e)}`);
    } finally {
      setBusyName("");
    }
  }

  async function remove(c: NotifyChannel): Promise<void> {
    // 确认框明示送达后果（对齐"new-incident 通知不再进该渠道"的值班语义）
    if (!window.confirm(`删除渠道「${c.name}」？\n删除后 new-incident（降噪转正放行的新事件）不再向该渠道送达；配置不可恢复，需重新创建。`)) return;
    setErr("");
    setBusyName(c.name);
    try {
      const out = await delJSON<ChannelWriteResponse>(ENDPOINTS.notifyChannel(c.name));
      if (out?.warning) setWarn(out.warning);
      await load();
    } catch (e) {
      setErr(`删除失败：${errText(e)}`);
    } finally {
      setBusyName("");
    }
  }

  const columns: Column<NotifyChannel>[] = [
    { key: "name", label: "名称", render: (c) => <span className="mono">{c.name}</span> },
    { key: "kind", label: "类型", render: (c) => <span className="chip">{KIND_LABEL[c.kind] ?? c.kind}</span> },
    { key: "sev", label: "最低级", render: (c) => <span className="chip">{c.min_severity || "info"}</span> },
    {
      key: "url", label: "URL",
      render: (c) => <span className="mono faint ch-url">{maskChannelUrl(c.url)}</span>,
    },
    {
      key: "enabled", label: "状态",
      render: (c) => (c.enabled
        ? <span className="chip chip--open">启用</span>
        : <span className="chip">已禁用</span>),
    },
    {
      key: "updated", label: "更新时间",
      render: (c) => <span className="time" title={fmtTime(c.updated_at)}>{timeAgoText(c.updated_at)}</span>,
    },
    {
      key: "acts", label: "操作",
      render: (c) => (c.name === RESERVED_NAME
        // 内置兜底渠道行：无删除按钮、无软开关（注册表恒有、不落库；防脏行）
        ? <span className="faint">内置兜底 · 不可删</span>
        : (
          <span className="flex-row items-center gap-8">
            <label className="sw-l" title={c.enabled ? "禁用（软开关：配置保留，可随时再启）" : "启用"}>
              <input
                type="checkbox" checked={c.enabled} disabled={busyName !== "" || saving}
                aria-label={`启停渠道 ${c.name}`}
                onChange={() => void toggleEnabled(c, !c.enabled)}
              />
              <span className={c.enabled ? "switch on" : "switch"} aria-hidden />
            </label>
            <button
              type="button" className="btn btn--sm btn--crit"
              disabled={busyName !== "" || saving}
              onClick={() => void remove(c)}
            >
              删除
            </button>
          </span>
        )),
    },
  ];

  return (
    <>
      <PageHead
        title="设置 · 通知渠道"
        desc="降噪转正（enforce）后的通知出口 · generic / 飞书 / 企业微信 · 按最低严重级路由 · 保存即生效 · 内置 console 兜底渠道不可删"
        right={
          <button type="button" className="btn" disabled={loading} onClick={() => void load()}>刷新</button>
        }
      />
      <Panel
        title="渠道列表"
        sub={rows ? `${rows.length} 个配置（含禁用）` : undefined}
        actions={
          <button
            type="button" className="btn btn--acc btn--sm" disabled={off}
            onClick={() => setFormOpen((v) => !v)}
          >
            {formOpen ? "收起" : "+ 新增渠道"}
          </button>
        }
        banner={
          <>
            {loading && !rows ? <Loading text="加载渠道…" /> : null}
            {off
              ? (
                <Banner kind="warn">
                  通知渠道后端未接线（503，需 OPS_DB_DSN）：配置存取不可用，列表不伪装为空——
                  当前仅内置 console 兜底渠道在落日志。
                </Banner>
              )
              : null}
            {err ? <Banner kind="err">{err}</Banner> : null}
            {warn ? <Banner kind="warn">配置已落库，但热重载未完成：{warn}</Banner> : null}
          </>
        }
      >
        {formOpen && !off
          ? (
            <div className="panel-b">
              <div className="form-row form-narrow">
                <span className="muted">名称</span>
                <input
                  className={errs.name ? "bad" : undefined} value={name} autoComplete="off"
                  placeholder="如：ops-feishu"
                  onChange={(e) => { setName(e.target.value); setErrs((p) => ({ ...p, name: undefined })); }}
                />
                {errs.name ? <span className="field-err">{errs.name}</span> : null}
                <span className="muted">类型</span>
                <select value={kind} onChange={(e) => setKind(e.target.value as ChannelKind)}>
                  {KINDS.map((k) => <option key={k.v} value={k.v}>{k.label}</option>)}
                </select>
                {errs.kind ? <span className="field-err">{errs.kind}</span> : null}
                <span className="muted">最低严重级</span>
                <select
                  value={sev}
                  onChange={(e) => setSev(e.target.value as "info" | "warning" | "critical")}
                >
                  {SEVERITIES.map((s) => <option key={s.v} value={s.v}>{s.label}</option>)}
                </select>
                <span className="muted">Webhook URL</span>
                <input
                  className={errs.url ? "bad" : undefined} value={url} autoComplete="off"
                  placeholder="https://..."
                  onChange={(e) => { setUrl(e.target.value); setErrs((p) => ({ ...p, url: undefined })); }}
                />
                {errs.url ? <span className="field-err">{errs.url}</span> : null}
                <span className="muted">操作人</span>
                <input
                  value={actorDraft} autoComplete="off"
                  placeholder="如 zhangsan（本地留存，跨写操作视图复用）"
                  onChange={(e) => setActorDraft(e.target.value)}
                />
              </div>
              <div className="faint my-4">
                校验与后端 ValidateChannel 逐条对齐；同名即幂等覆盖（upsert 不报 409），
                保存后立即进入热重载注册表生效。操作人仅按 sessionStorage 口径留存——
                渠道表无 actor 列，不进请求体（多传字段后端 400）。
              </div>
              <div className="acts">
                <button type="button" className="btn btn--acc btn--sm" disabled={saving} onClick={() => void submit()}>
                  {saving ? <span className="spin" /> : null}保存
                </button>
                <button type="button" className="btn btn--sm" onClick={() => { setFormOpen(false); setErrs({}); }}>
                  取消
                </button>
              </div>
            </div>
          )
          : null}
        <div className={off ? "rb-off" : undefined}>
          <DataTable
            columns={columns} rows={rows ?? []}
            rowKey={(c) => c.name}
            empty={{
              title: "暂无渠道",
              desc: "当前只启用了内置 console 兜底渠道（通知落服务日志）。点右上「+ 新增渠道」配置 generic / 飞书 / 企业微信出口。",
            }}
          />
        </div>
      </Panel>
    </>
  );
}
