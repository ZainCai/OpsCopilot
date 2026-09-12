import { PageHead } from "../components/Layout";
import { Panel } from "../components/Panel";
import { ENDPOINTS } from "../api/endpoints";

/**
 * 视图六 · 审计（骨架，TODO）。
 * 现状：审计只随事件走 GET /api/v1/incidents/{id}/audit（事件详情内已实装，
 * 见 IncidentsView 的 IncidentDetail）。独立全局审计面等后端提供列表端点
 * （ADR-005 审计分离，append-only + 哈希链）后再实装。
 */
export function AuditView(): React.ReactElement {
  return (
    <>
      <PageHead title="审计" desc="人工与自动动作统一留痕（二期：全局审计列表端点待后端提供）" />
      <Panel title="审计轨迹">
        <div className="panel-b">
          <div className="todo-box">
            骨架：单事件审计已实装于事件详情（<code>GET {ENDPOINTS.incidentAudit("…")}</code>）。
            全局审计检索需后端新增列表端点——先提 issue 再排期。
          </div>
        </div>
      </Panel>
    </>
  );
}
