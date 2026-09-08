-- 000001_init.down.sql
DROP INDEX IF EXISTS idx_audit_time;
DROP TABLE IF EXISTS audit_index;
DROP INDEX IF EXISTS idx_alert_event_time;
DROP TABLE IF EXISTS alert_event;
DROP INDEX IF EXISTS idx_cluster_state;
DROP TABLE IF EXISTS alert_cluster;
DROP INDEX IF EXISTS idx_change_node_time;
DROP TABLE IF EXISTS change_record;
DROP INDEX IF EXISTS idx_topo_edge_dst;
DROP INDEX IF EXISTS idx_topo_edge_time;
DROP TABLE IF EXISTS topo_edge;
DROP INDEX IF EXISTS idx_topo_node_key_time;
DROP TABLE IF EXISTS topo_node;
DROP TABLE IF EXISTS tenant;
