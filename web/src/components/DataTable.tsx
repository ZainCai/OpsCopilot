import type { ReactNode } from "react";

export interface Column<T> {
  key: string;
  label: string;
  className?: string; // "num" 右对齐等
  render: (row: T, index: number) => ReactNode;
}

interface DataTableProps<T> {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T, index: number) => string;
  onRowClick?: (row: T, index: number) => void;
  selectedKey?: string;
  /** rows 为空时的占位（标题 + 说明），沿用原型 Empty 形态。 */
  empty?: { title: string; desc: string };
}

/** 通用表格：复刻 console.html 的 .tbl 形态（44px 行高、hover brand 6%）。 */
export function DataTable<T>({
  columns, rows, rowKey, onRowClick, selectedKey, empty,
}: DataTableProps<T>): ReactNode {
  if (rows.length === 0 && empty) {
    return (
      <div className="tbl-wrap">
        <table className="tbl">
          <thead>
            <tr>{columns.map((c) => <th key={c.key} className={c.className}>{c.label}</th>)}</tr>
          </thead>
          <tbody>
            <tr>
              <td colSpan={columns.length}>
                <div className="empty">
                  <div className="e-t">{empty.title}</div>
                  <div className="e-d">{empty.desc}</div>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    );
  }
  return (
    <div className="tbl-wrap">
      <table className="tbl">
        <thead>
          <tr>{columns.map((c) => <th key={c.key} className={c.className}>{c.label}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
            const key = rowKey(row, i);
            const cls = [
              onRowClick ? "rowlink" : "",
              selectedKey && selectedKey === key ? "sel" : "",
            ].filter(Boolean).join(" ");
            return (
              <tr
                key={key}
                className={cls || undefined}
                onClick={onRowClick ? () => onRowClick(row, i) : undefined}
              >
                {columns.map((c) => <td key={c.key} className={c.className}>{c.render(row, i)}</td>)}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
