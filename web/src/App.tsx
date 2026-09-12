import { useCallback, useState } from "react";
import { Layout } from "./components/Layout";
import { navigate, useHashRoute } from "./router/hashRouter";
import { AlertsView } from "./views/AlertsView";
import { AuditView } from "./views/AuditView";
import { IncidentsView } from "./views/IncidentsView";
import { OverviewView } from "./views/OverviewView";
import { SettingsView } from "./views/SettingsView";
import { TopologyView } from "./views/TopologyView";

/** 路由表：hash 路径 → 视图。未注册路径回落实心视图（告警中心）。 */
export function App(): React.ReactElement {
  const route = useHashRoute();
  const [conn, setConn] = useState<"ok" | "bad" | "unknown">("unknown");
  const [lastRefresh, setLastRefresh] = useState("");

  const onConn = useCallback((ok: boolean) => {
    setConn(ok ? "ok" : "bad");
    setLastRefresh("刷新于 " + new Date().toLocaleTimeString());
  }, []);

  const view = (() => {
    switch (route.path) {
      case "overview": return <OverviewView onConn={onConn} />;
      case "incidents": return <IncidentsView onConn={onConn} />;
      case "topology": return <TopologyView />;
      case "settings": return <SettingsView />;
      case "audit": return <AuditView />;
      case "alerts":
      default: return <AlertsView onConn={onConn} />;
    }
  })();

  return (
    <Layout
      conn={conn}
      lastRefresh={lastRefresh}
      onRefresh={() => {
        // 简单口径：重进当前路由触发视图重新拉取（各视图 mount 即加载）
        navigate(route.path);
        window.location.reload();
      }}
    >
      {view}
    </Layout>
  );
}
