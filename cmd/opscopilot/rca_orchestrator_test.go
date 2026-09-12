// #12/ADR-014 编排器单测（无 DB 内存形态）：表驱动 fake incident.Store /
// fake AuditLog + 真实 sink/变更库/SemanticModel/NoiseEngine 组件，锁定
// "取单 → 故障域 → as_of 取证 → 变更窗口 → 六步 → 落审计"的链路语义。
// REST 门面契约在 rest_rca_test.go；PG 端到端在 rca_e2e_pg_test.go。
package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"opscopilot/internal/config"
	"opscopilot/internal/connector"
	"opscopilot/internal/incident"
	"opscopilot/internal/rca"
	"opscopilot/internal/topology"
)

// errDBDown Store 故障哨兵（表驱动 wantErr 与 prepare 共享同一值，
// errors.Is 按指针相等命中）。
var errDBDown = errors.New("fake db down")

// fakeIncidentStore 只实现 Get/Persistence 的事件 Store（其余方法经内嵌
// 接口，误用即 panic——本测试面只允许这两个被触达）。
type fakeIncidentStore struct {
	incident.Store
	byID map[string]incident.Incident
	err  error // 非 nil：Get 一律失败（模拟 Store 故障）
}

func (f *fakeIncidentStore) Get(id string) (incident.Incident, error) {
	if f.err != nil {
		return incident.Incident{}, f.err
	}
	inc, ok := f.byID[id]
	if !ok {
		return incident.Incident{}, incident.ErrNotFound
	}
	return inc, nil
}

func (f *fakeIncidentStore) Persistence() string { return "fake" }

// fakeAudit 捕获审计追加（append-only 语义在实现侧，这里验证调用）。
type fakeAudit struct {
	entries []AuditEntry
}

func (f *fakeAudit) Append(e AuditEntry) { f.entries = append(f.entries, e) }
func (f *fakeAudit) List(string) ([]AuditEntry, error) {
	return f.entries, nil
}

// rcaEnv 内存形态的编排器测试环境（节点 n1..n3，边 n1->n2->n3，medium）。
type rcaEnv struct {
	orch    *RCAOrchestrator
	inc     *fakeIncidentStore
	audit   *fakeAudit
	sink    *TopologySink
	changes topology.ChangeBackend
	noise   *NoiseEngine
}

func newRCAEnv(t *testing.T, mutate func(*config.RCASection)) *rcaEnv {
	t.Helper()
	cfg := config.Defaults()
	if mutate != nil {
		mutate(&cfg.RCA)
	}
	builder := topology.NewBuilder()
	sink, err := NewTopologySink(builder, newQuietLogger())
	if err != nil {
		t.Fatalf("sink: %v", err)
	}
	t0 := time.Now()
	nodes := make([]connector.ResourceNode, 0, 3)
	for _, n := range []string{"n1", "n2", "n3"} {
		nodes = append(nodes, connector.ResourceNode{
			Key: "prometheus://nodes/" + n, Type: "host",
			Labels: map[string]string{"instance": n}, ObservedAt: t0,
		})
	}
	if err := sink.IngestDiscover(context.Background(), &connector.DiscoverResult{
		Nodes: nodes, TenantID: cfg.Tenant}); err != nil {
		t.Fatalf("discover: %v", err)
	}
	for _, e := range [][2]string{{"n1", "n2"}, {"n2", "n3"}} {
		if err := builder.AddEdge(topology.EdgeInput{
			SrcKey: "prometheus://nodes/" + e[0], DstKey: "prometheus://nodes/" + e[1],
			Relation: "depends_on", ObservedAt: t0, Confidence: topology.ConfidenceMedium}); err != nil {
			t.Fatalf("add edge: %v", err)
		}
	}
	changes := topology.NewChangeStore(sink.HasNode)
	sem := NewSemanticModelServer(sink, changes)
	noise := NewNoiseEngine(sink, newQuietLogger(), cfg.Tenant, cfg.Noise, cfg.MemLimit)
	inc := &fakeIncidentStore{byID: map[string]incident.Incident{}}
	audit := &fakeAudit{}
	orch := NewRCAOrchestrator(cfg.RCA, inc, sem, noise, audit, cfg.Tenant, NewAppMetrics(), nil)
	return &rcaEnv{orch: orch, inc: inc, audit: audit, sink: sink, changes: changes, noise: noise}
}

// seedIncident 造一条带簇的事件；簇由真实告警注入形成（instance 反查节点）。
// 返回簇 Key。
func (e *rcaEnv) seedIncident(t *testing.T, id string, instances ...string) string {
	t.Helper()
	now := time.Now()
	alerts := make([]connector.Alert, 0, len(instances))
	for i, ins := range instances {
		alerts = append(alerts, connector.Alert{
			Fingerprint: "fp-" + ins,
			Labels:      map[string]string{"alertname": "DiskFull", "instance": ins, "severity": "critical"},
			Severity:    "critical", StartsAt: now.Add(time.Duration(i) * time.Second),
		})
	}
	e.noise.ProcessAlerts(alerts)
	active := e.noise.shadow.Clusterer().ActiveClusters()
	if len(active) == 0 {
		t.Fatal("no cluster formed from seeded alerts")
	}
	key := active[len(active)-1].Key
	e.inc.byID[id] = incident.Incident{
		ID: id, Title: "磁盘告警", Severity: "critical", State: incident.StateOpen,
		ClusterKeys: []string{key}, CreatedAt: now,
	}
	return key
}

func (e *rcaEnv) recordChange(t *testing.T, ev topology.ChangeEvent) {
	t.Helper()
	if _, err := e.changes.Record(ev); err != nil {
		t.Fatalf("record change: %v", err)
	}
}

func TestRCAOrchestratorTable(t *testing.T) {
	const changeID = "deploy-42"

	tests := []struct {
		name     string
		prepare  func(*testing.T, *rcaEnv)
		id       string
		wantErr  error // errors.Is 目标；nil = 期望成功
		wantRoot bool
		wantRef  string
		checkOK  func(*testing.T, *rcaEnv, *RCAResult)
	}{
		{
			name: "变更命中故障域节点 → high 根因",
			prepare: func(t *testing.T, e *rcaEnv) {
				e.seedIncident(t, "INC-hit", "n1")
				e.recordChange(t, topology.ChangeEvent{
					ID: changeID, NodeKey: "prometheus://nodes/n1", Type: topology.ChangeDeploy,
					Source: "jenkins", Author: "alice", Summary: "v2",
					OccurredAt: time.Now().Add(-5 * time.Minute),
					Confidence: topology.ConfidenceHigh,
				})
			},
			id:       "INC-hit",
			wantRoot: true,
			wantRef:  changeID,
		},
		{
			name: "窗口内无变更 → 证据不足报告（无根因、建议兜底）",
			prepare: func(t *testing.T, e *rcaEnv) {
				e.seedIncident(t, "INC-noc", "n1")
			},
			id: "INC-noc",
			checkOK: func(t *testing.T, e *rcaEnv, res *RCAResult) {
				if len(res.Report.RootCauses) != 0 {
					t.Fatalf("must not invent root cause: %+v", res.Report.RootCauses)
				}
				var recs []rca.Finding
				for _, f := range res.Report.Findings {
					if f.Step == rca.StepRecommend {
						recs = append(recs, f)
					}
				}
				if len(recs) != 1 || recs[0].Confidence != "low" {
					t.Fatalf("fallback recommend missing: %+v", recs)
				}
			},
		},
		{
			name:    "事件不存在 → ErrRCAIncidentNotFound",
			prepare: func(t *testing.T, e *rcaEnv) {},
			id:      "INC-missing",
			wantErr: ErrRCAIncidentNotFound,
		},
		{
			name: "Store 故障 → 原样上抛（REST 侧脱敏 500），不写审计",
			prepare: func(t *testing.T, e *rcaEnv) {
				e.inc.err = errDBDown
			},
			id:      "INC-any",
			wantErr: errDBDown,
		},
		{
			name: "OPS_RCA=off → ErrRCADisabled",
			prepare: func(t *testing.T, e *rcaEnv) {
				e.orch.spec.Enabled = false
			},
			id:      "INC-any",
			wantErr: ErrRCADisabled,
		},
		{
			name: "簇不在内存视图（重启/淘汰）→ 证据不足但链路跑通",
			prepare: func(t *testing.T, e *rcaEnv) {
				e.inc.byID["INC-ghost"] = incident.Incident{
					ID: "INC-ghost", Title: "幽灵簇", State: incident.StateOpen,
					ClusterKeys: []string{"c:gone@999"}, CreatedAt: time.Now(),
				}
			},
			id: "INC-ghost",
			checkOK: func(t *testing.T, e *rcaEnv, res *RCAResult) {
				if len(res.Report.Input.AlertedNodes) != 0 {
					t.Fatalf("ghost cluster must yield empty domain, got %v", res.Report.Input.AlertedNodes)
				}
				if len(res.Report.Input.Evidence.Nodes) != 0 {
					t.Fatalf("empty domain must not pull whole graph as evidence: %d nodes",
						len(res.Report.Input.Evidence.Nodes))
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newRCAEnv(t, nil)
			tc.prepare(t, e)
			res, err := e.orch.Analyze(context.Background(), tc.id, "tester")
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				for _, au := range e.audit.entries {
					if au.Action == AuditRCA {
						t.Fatalf("failed analysis must not write rca audit: %+v", au)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("analyze: %v", err)
			}
			if tc.checkOK != nil {
				tc.checkOK(t, e, res)
			}
			// 默认断言：六步齐、conclude pending（本期无 LLM）、审计已写。
			if len(res.Report.Steps) != 6 {
				t.Fatalf("steps = %d, want 6", len(res.Report.Steps))
			}
			for _, sr := range res.Report.Steps {
				if sr.Name == rca.StepConclude && sr.Status != rca.StatusPending {
					t.Fatalf("conclude = %s, want pending (no llm-gateway yet)", sr.Status)
				}
			}
			var last *AuditEntry
			for i := range e.audit.entries {
				if e.audit.entries[i].Action == AuditRCA {
					last = &e.audit.entries[i]
				}
			}
			if last == nil {
				t.Fatalf("audit must record rca action, entries = %+v", e.audit.entries)
			}
			if last.IncidentID != tc.id || last.Actor != "tester" {
				t.Fatalf("audit entry = %+v, want incident %s actor tester", last, tc.id)
			}
			if d, ok := last.Detail["steps_done"]; !ok || d == nil {
				t.Fatalf("audit detail missing steps_done: %+v", last.Detail)
			}
			if tc.wantRoot {
				if len(res.Report.RootCauses) == 0 {
					t.Fatalf("want root cause, findings = %+v", res.Report.Findings)
				}
				if tc.wantRef != "" && res.Report.RootCauses[0].Ref != tc.wantRef {
					t.Fatalf("root ref = %q, want %q", res.Report.RootCauses[0].Ref, tc.wantRef)
				}
			}
		})
	}
}

// TestRCAOrchestratorTimeout 预算耗尽/请求取消 → 阶段边界检查立刻放弃
// （已取消的请求不烧取证 CPU）。用"预取消父 ctx + 短暂等待传播"构造，
// 不靠纳秒计时器竞争——deadline 微秒级窗口在同一 goroutine 连续指令间
// 是否已过不确定，取消传播落定后则完全确定。
func TestRCAOrchestratorTimeout(t *testing.T) {
	e := newRCAEnv(t, nil)
	e.seedIncident(t, "INC-to", "n1")
	pctx, cancel := context.WithCancel(context.Background())
	cancel()
	time.Sleep(5 * time.Millisecond) // propagateCancel 的异步落定
	_, err := e.orch.Analyze(pctx, "INC-to", "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestRCAOrchestratorMetrics 计数/直方图随 Analyze 落账（ok 与 error 分桶）。
func TestRCAOrchestratorMetrics(t *testing.T) {
	e := newRCAEnv(t, nil)
	e.seedIncident(t, "INC-m", "n1")
	if _, err := e.orch.Analyze(context.Background(), "INC-m", ""); err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if _, err := e.orch.Analyze(context.Background(), "INC-absent", ""); !errors.Is(err, ErrRCAIncidentNotFound) {
		t.Fatalf("want not-found, got %v", err)
	}
	rec := httptest.NewRecorder()
	e.orch.m.Handler()(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`opscopilot_rca_requests_total{outcome="ok"} 1`,
		`opscopilot_rca_requests_total{outcome="error"} 1`,
		"opscopilot_rca_duration_seconds_count 2",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q", want)
		}
	}
}

// TestRCAOrchestratorSummarizerHook 二期挂点行为验证：注入 Summarizer 后
// conclude done、结论进报告；未注入保持 pending（默认用例已锁）。
func TestRCAOrchestratorSummarizerHook(t *testing.T) {
	e := newRCAEnv(t, nil)
	e.seedIncident(t, "INC-llm", "n1")
	e.recordChange(t, topology.ChangeEvent{
		ID: "dep-llm", NodeKey: "prometheus://nodes/n1", Type: topology.ChangeDeploy,
		Source: "jenkins", Author: "alice", OccurredAt: time.Now().Add(-3 * time.Minute),
		Confidence: topology.ConfidenceHigh,
	})
	e.orch.SetSummarizer(fakeSummarizerStub{})
	res, err := e.orch.Analyze(context.Background(), "INC-llm", "")
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	var found bool
	for _, f := range res.Report.Findings {
		if f.Step == rca.StepConclude {
			found = f.Summary == "llm-conclusion" && f.Confidence == "high"
		}
	}
	if !found {
		t.Fatalf("conclude finding missing/wrong: %+v", res.Report.Findings)
	}
}

type fakeSummarizerStub struct{}

func (fakeSummarizerStub) Summarize(rca.Input, []rca.Finding) (string, error) {
	return "llm-conclusion", nil
}

// seedManyChanges 在故障域节点 n1 上记 n 条 high 置信 deploy 变更（各自
// 独立 ID），每条产 hypothesize+verify+attribution 三行证据 → 撑高 findings
// 数以触发 #6 截断。
func (e *rcaEnv) seedManyChanges(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		e.recordChange(t, topology.ChangeEvent{
			ID:      "dep-" + string(rune('a'+i)) + string(rune('A'+i)), // 稳定唯一 ID
			NodeKey: "prometheus://nodes/n1", Type: topology.ChangeDeploy,
			Source: "jenkins", Author: "ci",
			OccurredAt: time.Now().Add(-time.Duration(i+1) * time.Minute),
			Confidence: topology.ConfidenceHigh,
		})
	}
}

// TestRCAOrchestratorFindingsTruncation 二期池波二 #6：报告按上限截断、
// 全量留在 RCAResult、审计记截断版口径、根因（独立切片）不随截断丢失。
func TestRCAOrchestratorFindingsTruncation(t *testing.T) {
	// 6 条变更 → 6*(hypothesize+verify+attribution) + collect + recommend = 20 findings。
	const nChange, maxF = 6, 5
	e := newRCAEnv(t, func(s *config.RCASection) { s.MaxFindings = maxF })
	e.seedIncident(t, "INC-trunc", "n1")
	e.seedManyChanges(t, nChange)

	res, err := e.orch.Analyze(context.Background(), "INC-trunc", "tester")
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	full := len(res.FindingsFull)
	if full <= maxF {
		t.Fatalf("fixture too small to truncate: full=%d max=%d", full, maxF)
	}
	if len(res.Report.Findings) != maxF {
		t.Fatalf("report findings = %d, want %d (truncated view)", len(res.Report.Findings), maxF)
	}
	if res.Report.FindingsTruncated != full-maxF {
		t.Fatalf("findings_truncated = %d, want %d", res.Report.FindingsTruncated, full-maxF)
	}
	// 根因（attribution high）来自全量口径，独立于被截的 findings 列表，必须仍在。
	if len(res.Report.RootCauses) != nChange {
		t.Fatalf("root causes = %d, want %d (survive truncation)", len(res.Report.RootCauses), nChange)
	}
	// 审计存截断版计数：findings=截断后条数，findings_truncated=被裁条数。
	var last *AuditEntry
	for i := range e.audit.entries {
		if e.audit.entries[i].Action == AuditRCA {
			last = &e.audit.entries[i]
		}
	}
	if last == nil {
		t.Fatal("audit must record rca entry")
	}
	if got := last.Detail["findings"]; got != maxF {
		t.Fatalf("audit findings = %v, want %d", got, maxF)
	}
	if got := last.Detail["findings_truncated"]; got != full-maxF {
		t.Fatalf("audit findings_truncated = %v, want %d", got, full-maxF)
	}
}

// TestRCAOrchestratorFindingsDefaultNoTruncate 默认上限 200：小规模链路不
// 触发截断（行为与转正前逐字节一致——findings_truncated=0、全量=截断版）。
func TestRCAOrchestratorFindingsDefaultNoTruncate(t *testing.T) {
	e := newRCAEnv(t, nil) // MaxFindings = 默认 200
	if e.orch.spec.MaxFindings != config.DefaultRCAMaxFindings {
		t.Fatalf("spec default MaxFindings = %d, want %d", e.orch.spec.MaxFindings, config.DefaultRCAMaxFindings)
	}
	e.seedIncident(t, "INC-small", "n1")
	e.recordChange(t, topology.ChangeEvent{
		ID: "dep-s", NodeKey: "prometheus://nodes/n1", Type: topology.ChangeDeploy,
		Source: "jenkins", Author: "ci", OccurredAt: time.Now().Add(-2 * time.Minute),
		Confidence: topology.ConfidenceHigh,
	})
	res, err := e.orch.Analyze(context.Background(), "INC-small", "")
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if res.Report.FindingsTruncated != 0 {
		t.Fatalf("small chain must not truncate: findings_truncated = %d", res.Report.FindingsTruncated)
	}
	if len(res.Report.Findings) != len(res.FindingsFull) {
		t.Fatalf("untruncated report/full mismatch: %d vs %d", len(res.Report.Findings), len(res.FindingsFull))
	}
}
