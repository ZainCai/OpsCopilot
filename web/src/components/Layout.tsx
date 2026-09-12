import type { ReactNode } from "react";
import { navigate, useHashRoute } from "../router/hashRouter";

export type ConnState = "ok" | "bad" | "unknown";

interface TopBarProps {
  conn: ConnState;
  lastRefresh: string;
  onRefresh: () => void;
}

const NAV = [
  { path: "overview", label: "降噪总览" },
  { path: "alerts", label: "告警中心" },
  { path: "incidents", label: "事件" },
  { path: "topology", label: "拓扑" },
  { path: "settings", label: "设置" },
] as const;

/** 应用外壳：顶栏 + 视图导航（对齐内嵌 console 的 topbar 形态）。 */
export function Layout({ conn, lastRefresh, onRefresh, children }: TopBarProps & { children: ReactNode }): ReactNode {
  const route = useHashRoute();
  return (
    <>
      <div className="topbar">
        <div className="brand">
          <div className="brand-glyph">OC</div>
          <div>
            <div className="brand-name">OpsCopilot</div>
            <div className="brand-sub">WEB CONSOLE</div>
          </div>
        </div>
        <nav className="tb-nav">
          {NAV.map((n) => (
            <button
              key={n.path}
              type="button"
              className={`tb-tab${route.path === n.path ? " on" : ""}`}
              onClick={() => navigate(n.path)}
            >
              {n.label}
            </button>
          ))}
        </nav>
        <span className="tb-meta" title="独立前端工程 web/（ADR-013）">web-dev</span>
        <div className="tb-spacer" />
        <span className={`env-chip ${conn === "ok" ? "ok" : conn === "bad" ? "bad" : "warn"}`}>
          <span className="dot" />
          <span>{conn === "ok" ? "服务正常" : conn === "bad" ? "连接异常" : "连接中"}</span>
        </span>
        <span className="tb-meta">{lastRefresh || "未刷新"}</span>
        <button type="button" className="btn" onClick={onRefresh}>刷新</button>
      </div>
      <main className="content">{children}</main>
    </>
  );
}

/** 页面头（标题 + 描述 + 右侧操作区）。 */
export function PageHead({ title, desc, right }: { title: string; desc: string; right?: ReactNode }): ReactNode {
  return (
    <div className="page-head">
      <div>
        <div className="page-title">{title}</div>
        <div className="page-desc">{desc}</div>
      </div>
      {right ? <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>{right}</div> : null}
    </div>
  );
}
