// 告警知识库（D12=B 配套；对齐原型 alerts.jsx 的"可操作告警"理念）。
//
// 定位：把降噪判决流（alert_event）合成为运维可快速行动的结构化告警——
// 标题/级别/错误码/影响范围/初步处置措施。键为 alertname（payload.summary，
// 与 Prometheus labels["alertname"] 同源）。
//
// 口径说明：指标阈值与处置措施是**运营知识**，以本表为准；告警原文里没有
// 实时指标值（Prometheus /alerts 不携带 series 值），展示的阈值来自本表、
// 实际值需看对应指标图表。未知告警名走 generic 兜底，保证结构统一。
package main

import (
	"fmt"
	"strings"
)

// alertKnowledge 单类告警的运维知识。
type alertKnowledge struct {
	Title     string   // 告警标题（不含实例）
	Level     string   // 默认级别：紧急 / 重要 / 一般（severity=critical 时升为紧急）
	ErrorCode string   // 错误码（OPS-<域>-<序号>）
	Metric    string   // 关联指标名
	Threshold string   // 告警阈值（口径）
	Impact    string   // 影响范围（%s 会替换为实例/节点）
	Steps     []string // 建议的初步处理措施（按序）
}

// alertKB 告警知识表。新增告警类型时在此登记即可获得统一结构。
var alertKB = map[string]alertKnowledge{
	"HighDiskUsage": {
		Title: "磁盘使用率过高", Level: "重要", ErrorCode: "OPS-DISK-001",
		Metric: "node_filesystem_used_percent", Threshold: "85%",
		Impact: "节点 %s 磁盘写满将导致日志/数据库写入失败，进而引发服务不可用",
		Steps: []string{
			"df -h 定位高使用率挂载点",
			"du -sh /* 逐层定位大目录（优先查日志与临时目录）",
			"清理或轮转日志；必要时扩容磁盘",
		},
	},
	"InodeExhaustion": {
		Title: "磁盘 inode 耗尽", Level: "紧急", ErrorCode: "OPS-INODE-002",
		Metric: "node_files_inodes_used_percent", Threshold: "90%",
		Impact: "节点 %s inode 耗尽后新建文件将失败，即使磁盘空间充足",
		Steps: []string{
			"df -i 确认 inode 占用",
			"find 定位海量小文件目录（常见：会话/缓存/邮件队列）",
			"清理小文件；评估应用的小文件治理方案",
		},
	},
	"HighLatency": {
		Title: "网络延迟过高", Level: "紧急", ErrorCode: "OPS-NET-101",
		Metric: "node_network_rtt_ms", Threshold: "100ms",
		Impact: "节点 %s 所在同交换机域的服务间调用超时率上升，可能触发级联重试",
		Steps: []string{
			"ping/mtr 定位丢包与延迟发生的链路段",
			"检查交换机端口计数器与光模块状态",
			"比对同网段其他节点，区分单机与共享链路问题",
		},
	},
	"CertExpiringSoon": {
		Title: "证书即将过期", Level: "重要", ErrorCode: "OPS-CERT-201",
		Metric: "x509_days_to_expire", Threshold: "30 天",
		Impact: "节点 %s 的 TLS 服务将在到期后拒绝连接，造成对外服务中断",
		Steps: []string{
			"openssl x509 -enddate 确认证书链与到期日",
			"更换证书并 reload 对应服务",
			"验证新证书生效与到期监控已重置",
		},
	},
	"BackupFailed": {
		Title: "备份任务失败", Level: "重要", ErrorCode: "OPS-BAK-202",
		Metric: "backup_success_ratio", Threshold: "100%",
		Impact: "节点 %s 关联数据的灾备 RPO 拉长，故障时可能丢失最近一次备份后的数据",
		Steps: []string{
			"查看备份作业日志定位失败步骤",
			"核对备份存储端空间与访问凭证",
			"修复后手动触发一次全量备份并校验可恢复性",
		},
	},
}

// genericKnowledge 未知告警的兜底（保证结构统一，不给出臆测的处置建议）。
func genericKnowledge(alertname string) alertKnowledge {
	name := alertname
	if name == "" {
		name = "未知告警"
	}
	return alertKnowledge{
		Title:     name,
		Level:     "一般",
		ErrorCode: "OPS-GEN-000",
		Metric:    "—",
		Threshold: "—",
		Impact:    "未登记的告警类型：请依据告警来源与摘要人工判断影响范围",
		Steps: []string{
			"确认告警来源（labels/annotations）与触发时间线",
			"在对应服务日志中检索同时间窗的异常",
			"将该告警类型登记进本知识库（cmd/opscopilot/alert_knowledge.go）",
		},
	}
}

// knowledgeFor 取告警知识（未登记走兜底）。
func knowledgeFor(alertname string) alertKnowledge {
	if k, ok := alertKB[strings.TrimSpace(alertname)]; ok {
		return k
	}
	return genericKnowledge(alertname)
}

// levelOf 最终级别：severity=critical 一律升为"紧急"（口径：critical 的
// 即时性优先于知识库的默认档位）。
func levelOf(kbLevel, severity string) string {
	if severity == "critical" {
		return "紧急"
	}
	switch kbLevel {
	case "紧急", "重要", "一般":
		return kbLevel
	}
	return "一般"
}

// enrichTitle 标题 = 知识库标题 +（实例/节点）。
func enrichTitle(k alertKnowledge, nodeKey string) string {
	if nodeKey != "" {
		return fmt.Sprintf("%s（%s）", k.Title, nodeKey)
	}
	return k.Title
}
