import type { ReactNode } from "react";
import { navigate, useHashRoute } from "../router/hashRouter";

export type ConnState = "ok" | "bad" | "unknown";

interface TopBarProps {
  conn: ConnState;
  lastRefresh: string;
  onRefresh: () => void;
}

type IconName = "gauge" | "bell" | "list" | "shield" | "topo" | "gear";

/** 导航分组（原型侧栏口径；path 与 App.tsx 路由表一致，零新增路由）。 */
const NAV_GROUPS: { group: string; items: { path: string; label: string; icon: IconName }[] }[] = [
  { group: "监控", items: [
    { path: "overview", label: "降噪总览", icon: "gauge" },
    { path: "alerts", label: "告警中心", icon: "bell" },
  ] },
  { group: "事件", items: [
    { path: "incidents", label: "事件", icon: "list" },
    { path: "audit", label: "审计轨迹", icon: "shield" },
  ] },
  { group: "分析", items: [
    { path: "topology", label: "拓扑", icon: "topo" },
  ] },
  { group: "配置", items: [
    { path: "settings", label: "设置", icon: "gear" },
  ] },
];

/** 16px stroke 线性图标（currentColor 随导航态着色；纯装饰 aria-hidden）。 */
function Icon({ name }: { name: IconName }): ReactNode {
  const d: Record<IconName, ReactNode> = {
    gauge: <><path d="M8 13a5 5 0 1 1 5-5" /><path d="M8 8l3-2.5" /></>,
    bell: <><path d="M4 11.5c0-3 1.5-6 4-6s4 3 4 6l1 1.5H3l1-1.5Z" /><path d="M7 15h2" /></>,
    list: <path d="M3.5 4.5h9M3.5 8h9M3.5 11.5h6" />,
    shield: <path d="M8 2.5 13 4.5v4c0 3-2.5 4.5-5 5.5-2.5-1-5-2.5-5-5.5v-4L8 2.5Z" />,
    topo: <><circle cx="4" cy="4" r="1.6" /><circle cx="12" cy="4" r="1.6" /><circle cx="8" cy="12" r="1.6" /><path d="M5 5.2 7.2 10.8M11 5.2 8.8 10.8M5.5 4h5" /></>,
    gear: <><circle cx="8" cy="8" r="2.2" /><path d="M8 2.5v1.8M8 11.7v1.8M2.5 8h1.8M11.7 8h1.8M4.1 4.1l1.3 1.3M10.6 10.6l1.3 1.3M11.9 4.1l-1.3 1.3M5.4 10.6l-1.3 1.3" /></>,
  };
  return (
    <svg className="ic" width="16" height="16" viewBox="0 0 16 16" fill="none"
      stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
      {d[name]}
    </svg>
  );
}

/** 应用外壳（原型口径）：256px 侧栏（品牌+分组导航）+ sticky 顶栏（状态/刷新）+ 1600px 内容列。 */
export function Layout({ conn, lastRefresh, onRefresh, children }: TopBarProps & { children: ReactNode }): ReactNode {
  const route = useHashRoute();
  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-glyph">OC</div>
          <div>
            <div className="brand-name">OpsCopilot</div>
            <div className="brand-sub">WEB CONSOLE</div>
          </div>
        </div>
        <nav className="nav">
          {NAV_GROUPS.map((g) => (
            <div key={g.group}>
              <div className="nav-group">{g.group}</div>
              {g.items.map((n) => (
                <button
                  key={n.path}
                  type="button"
                  className={`nav-i${route.path === n.path ? " on" : ""}`}
                  onClick={() => navigate(n.path)}
                  title={n.label}
                >
                  <Icon name={n.icon} />
                  <span>{n.label}</span>
                </button>
              ))}
            </div>
          ))}
        </nav>
        <div className="side-foot" title="独立前端工程 web/（ADR-013）">web-dev</div>
      </aside>
      <div className="main">
        <div className="topbar">
          <span className={`env-chip ${conn === "ok" ? "ok" : conn === "bad" ? "bad" : "warn"}`}>
            <span className="dot" />
            <span>{conn === "ok" ? "服务正常" : conn === "bad" ? "连接异常" : "连接中"}</span>
          </span>
          <span className="tb-meta">{lastRefresh || "未刷新"}</span>
          <div className="tb-spacer" />
          <button type="button" className="btn" onClick={onRefresh}>刷新</button>
        </div>
        <main className="content">{children}</main>
      </div>
    </div>
  );
}

/** 页面头（标题 + 描述 + 右侧操作区，原型 page-head/page-act 口径）。 */
export function PageHead({ title, desc, right }: { title: string; desc: string; right?: ReactNode }): ReactNode {
  return (
    <div className="page-head">
      <div>
        <div className="page-title">{title}</div>
        <div className="page-desc">{desc}</div>
      </div>
      {right ? <div className="page-act">{right}</div> : null}
    </div>
  );
}
