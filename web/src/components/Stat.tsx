import type { ReactNode } from "react";

interface StatCardProps {
  label: string;
  value: ReactNode;
  unit?: string;
  sub?: ReactNode;
  color?: string; // CSS 变量或色值
}

/** KPI 卡（原型 stat 形态：label / 25px mono 大数 / 单位 / 副文本）。 */
export function StatCard({ label, value, unit, sub, color }: StatCardProps): ReactNode {
  return (
    <div className="stat">
      <div className="stat-l">{label}</div>
      <div className="stat-v">
        <span className="stat-n" style={color ? { color } : undefined}>{value}</span>
        {unit ? <span className="stat-u">{unit}</span> : null}
      </div>
      {sub ? <div className="stat-s">{sub}</div> : null}
    </div>
  );
}

/** 分段过滤控件（原型 Seg 交互）。 */
export function Seg<T extends string>({
  options, value, onChange,
}: {
  options: { v: T; label: string }[];
  value: T;
  onChange: (v: T) => void;
}): ReactNode {
  return (
    <div className="seg">
      {options.map((o) => (
        <button
          key={o.v}
          type="button"
          className={`seg-i${o.v === value ? " on acc" : ""}`}
          onClick={() => onChange(o.v)}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}
