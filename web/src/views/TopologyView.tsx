import { PageHead } from "../components/Layout";
import { Panel } from "../components/Panel";
import { ENDPOINTS } from "../api/endpoints";

/**
 * 视图四 · 拓扑（骨架，TODO）。
 * 数据源：GET /api/v1/topology（?node_key=&depth=&as_of=）与
 * GET /api/v1/changes（节点 24h 变更查询）。
 */
export function TopologyView(): React.ReactElement {
  return (
    <>
      <PageHead title="拓扑" desc="观测事实（high）/ 聚合声明（medium）/ low 不进因果推理（ADR-007）" />
      <Panel title="拓扑图">
        {/* TODO(W9 迁移)：
            1. GET /api/v1/topology → 环形布局 SVG（照抄 console.html renderGraph 的
               nodes/edges 口径：node_key/node_type/confidence + src_key/dst_key）；
            2. 节点 >300 时按 node_type 分桶聚合渲染（AGG_THRESHOLD 同口径）；
            3. 节点点击 → 邻域 depth=1 + as_of 时间切片 + 24h 变更列表
               （GET /api/v1/changes?node_key=&window_start=）。 */}
        <div className="panel-b">
          <div className="todo-box">
            骨架待实装。端点：<code>GET {ENDPOINTS.topology}</code> ·{" "}
            <code>GET {ENDPOINTS.changes}</code>
          </div>
        </div>
      </Panel>
    </>
  );
}
