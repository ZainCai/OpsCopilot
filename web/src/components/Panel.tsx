import type { ReactNode } from "react";

interface PanelProps {
  title: string;
  sub?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
  banner?: ReactNode; // 顶部横幅位（加载/错误）
}

/** 面板：标题栏 + 体（复刻 console.html .panel 形态）。 */
export function Panel({ title, sub, actions, children, banner }: PanelProps): ReactNode {
  return (
    <section className="panel">
      <header className="panel-h">
        <span className="panel-t">{title}</span>
        {sub ? <span className="panel-sub">{sub}</span> : null}
        {actions ? <div className="panel-x">{actions}</div> : null}
      </header>
      {banner}
      {children}
    </section>
  );
}

/** 行内状态横幅。 */
export function Banner({ kind, children }: { kind: "err" | "warn"; children: ReactNode }): ReactNode {
  if (!children) return null;
  return (
    <div style={{ margin: "12px var(--pad)" }}>
      <div className={`banner ${kind}`}>{kind === "err" ? "⚠ " : "⚠ "}{children}</div>
    </div>
  );
}

export function Loading({ text }: { text: string }): ReactNode {
  return (
    <div style={{ margin: "12px var(--pad)" }}>
      <div className="banner warn"><span className="spin" />{text}</div>
    </div>
  );
}
