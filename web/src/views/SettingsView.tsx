import { PageHead } from "../components/Layout";
import { Panel } from "../components/Panel";
import { ENDPOINTS } from "../api/endpoints";

/**
 * 视图五 · 设置 · 通知渠道（骨架，TODO）。
 * enforce 模式通知出口配置：generic / 飞书 / 企业微信，按最低严重级路由，
 * 内置 console 兜底渠道不可删（需 OPS_DB_DSN）。
 */
export function SettingsView(): React.ReactElement {
  return (
    <>
      <PageHead
        title="设置 · 通知渠道"
        desc="降噪转正（enforce）后的通知出口 · 保存即生效 · 内置 console 兜底渠道不可删"
      />
      <Panel title="渠道列表">
        {/* TODO(W9 迁移)：
            1. GET /api/v1/notify/channels 列表（name/kind/min_severity/url/enabled/updated_at）；
            2. 新增/覆盖 POST /api/v1/notify/channels（写路径带 Token）；
            3. 启停 POST /api/v1/notify/channels/{name}/enabled；删除 DELETE 同路径；
            4. 表单校验：名称限字母数字._-，URL 必填。 */}
        <div className="panel-b">
          <div className="todo-box">
            骨架待实装。端点：<code>GET/POST {ENDPOINTS.notifyChannels}</code> ·{" "}
            <code>POST {ENDPOINTS.notifyChannelEnabled("…")}</code> ·{" "}
            <code>DELETE {ENDPOINTS.notifyChannel("…")}</code>
          </div>
        </div>
      </Panel>
    </>
  );
}
