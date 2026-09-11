package main

import "testing"

// TestKnowledgeFor 第七轮后告警中心富化：已知告警名出结构化知识，未知兜底。
func TestKnowledgeFor(t *testing.T) {
	k := knowledgeFor("HighDiskUsage")
	if k.Title != "磁盘使用率过高" || k.ErrorCode != "OPS-DISK-001" || len(k.Steps) == 0 {
		t.Fatalf("known alert KB wrong: %+v", k)
	}
	if levelOf(k.Level, "critical") != "紧急" {
		t.Fatal("critical must escalate to 紧急")
	}
	if levelOf(k.Level, "warning") != "重要" {
		t.Fatalf("warning must keep KB level, got %q", levelOf(k.Level, "warning"))
	}
	g := knowledgeFor("SomethingUnknown")
	if g.ErrorCode != "OPS-GEN-000" || len(g.Steps) == 0 {
		t.Fatalf("generic fallback wrong: %+v", g)
	}
	if got := enrichTitle(k, "prometheus://nodes/n1"); got != "磁盘使用率过高（prometheus://nodes/n1）" {
		t.Fatalf("title enrich: %q", got)
	}
}
