// rca_eval RCA 评测集（golden set）跑分管线——二期池 #8，W12 "LLM 对外
// 转正 ≥85%" 门禁的载体工具。
//
// 组织方式抄 tools/evaluate.py（影子降噪评测）的范式：ground truth 不在本
// 工具里二次定义——以 golden.json 为准；golden.json 逐场景抄自
// tools/faultinjector/main.go（行号内联标注），启动时再对照注入器
// /answerbook 做 drift 校验（段数/场景码/expected_new_incidents 不一致
// 直接退出码 2，防"剧本改了答案没改"的第二处定义漂移）。
//
// 管线（对 golden 每个场景段）：
//
//	① 注入：POST /api/v1/changes 打变更证据（root + 干扰项；occurred_at 按
//	  golden 的 occurred_lead_sec 钉在段起点之前——T0=事件创建时刻晚于段起点，
//	  必然落进 [T0-30m, T0] 取证窗）。告警不在这里注入：告警的已知答案就是
//	  faultinjector 剧本本身，由被测 app 的拉取链路成簇（ProcessAlerts）+
//	  建单（ingest_queue → UpsertExternal）。
//	② 等待成单：PG 轮询 alert_event（影子判决 → cluster_key）与 incident
//	  （origin='prometheus'，source_ref=promFingerprint(labels)，算法与
//	  cmd/opscopilot/pull_alerts.go promFingerprint() L140-158 逐字节一致）。
//	③ 挂簇：生产自动挂簇路径优先（W10-6 OPS_AUTOATTACH=on+enforce，评测接线
//	  默认如此）——轮询 incident_cluster 验证簇已挂到持域事件；被测 app 未开
//	  开关时回退按 AttachCluster SQL 语义直写桥接（兼容旧形态，note 显式标注
//	  "生产挂簇路径未被验证"）。
//	④ 判分：GET /api/v1/incidents/{id}/rca → 比对 root_causes[].ref 是否
//	  命中场景标注根因（top-1 / top-3 两档）；直读 incident_audit
//	  (action='rca') 校验 #4 审计落库形态与 duration_ms。
//	⑤ 产出：逐评测单元 JSONL + 汇总（命中率、延迟分布、证据完整率）+
//	  report.md（含 ≥85% 转正门禁章节；--no-llm 证据版基线在报告中显式标注）。
//
// W12 LLM 转正三门禁（gates.go；仅 LLM 模式即 --no-llm=false 时判定，
// 三道各自独立计数、报告 §7 分节呈现）：
//
//	G1 归因不回退：LLM 只改写叙述不动规则链归因——root_causes top1 仍命中
//	   golden，全单元分母、≥阈值（85%）；
//	G2 结论产出：llm_used=true ∧ conclusion 非空 ∧ steps[conclude]=done，
//	   配置了 LLM 就必须出结论 → 100% 硬线；
//	G3 结论-证据一致性（启发式，不接第二个 LLM 当裁判）：矛盾话术（证据
//	   不足/待定/pending…）= 硬性失败一票否决；conclusion 归一化（去分隔
//	   符、折大小写）后必须包含首要 root_cause 的锚点（ref 或其 node_key），
//	   未命中不判死、进"存疑清单"（conclusion 摘要 + ref）人工终审。
//
//	总判定：G1 ≥85% ∧ G2 100% ∧ G3 无硬性失败 ∧ 静默守护全过 → PASS=可转正。
//
// C（静默）段不产事件 → 转为"防误报守护"：段窗内影子判决必须为 0、
// 不得新建本剧本之外的 Incident。
//
// 零依赖 internal/config（另一 agent 在改 config，此处只 flags+env 读
// DSN/端点，取值口径同 tools/verdicts：flag 缺省回退 env）。
//
// 用法（编排见 scripts/run_rca_eval.sh）：
//
//	OPS_DB_DSN=postgres://opscopilot:opscopilot@127.0.0.1:5432/opscopilot?sslmode=disable \
//	OPS_TENANT=rca-eval-<runid> OPS_WEBHOOK_TOKEN=dev \
//	go run ./tools/rca_eval -golden tools/rca_eval/golden.json \
//	    -out-dir .rca-eval -report-dir docs/reviews/rca-eval-<date> --no-llm
//
// 退出码：0 = top1 命中率 ≥ --threshold 且守护全过（门禁语义同 evaluate.py）；
// 1 = 未达标；2 = 运行失败（drift/连接/装配问题）。
//
// golden 维护协议（详见 golden.json._maintenance 与报告 §7）：改注入器
// 剧本 → 同步 golden.json（连行号）→ drift 校验兜底。干扰项 role 语义：
//   - root                 场景标注根因（期望 top1 命中其 change id）
//   - distractor_same_node 同节点更早高置信（考"贴 T0 优先"排名）
//   - distractor_neighbor  域内他节点中置信（允许进 top3，不应进 top1）
//   - distractor_outside   故障域外变更（特异性哨兵：进根因 = 误归因 bug）
//   - note                 不注入，仅文档说明
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ===================== 数据模型：golden =====================

type goldenDoc struct {
	About       string            `json:"_about"`
	Maintenance []string          `json:"_maintenance"`
	Source      map[string]string `json:"source"`
	Segments    []goldenSegment   `json:"segments"`
}

type goldenSegment struct {
	Idx         int             `json:"idx"`
	Code        string          `json:"code"`
	Name        string          `json:"name"`
	SrcLines    string          `json:"src_lines"`
	DurationSec int             `json:"duration_sec"`
	ExpectedNew int             `json:"expected_new_incidents"`
	Silence     bool            `json:"silence_guard"`
	Expected    string          `json:"expected"`
	Alerts      []goldenAlert   `json:"alerts"`
	Clusters    []goldenCluster `json:"clusters"`
	Changes     []goldenChange  `json:"changes"`
}

type goldenAlert struct {
	Fingerprint string `json:"fingerprint"`
	Alertname   string `json:"alertname"`
	Instance    string `json:"instance"`
	Job         string `json:"job"`
	Severity    string `json:"severity"`
	SrcLine     int    `json:"src_line"`
}

type goldenCluster struct {
	ID         string   `json:"id"`
	Nodes      []string `json:"nodes"`
	Members    []string `json:"member_fingerprints"`
	Represent  string   `json:"representative"`
	RootChange string   `json:"root_cause_change"`
	Expected   string   `json:"expected"`
	SrcLines   string   `json:"src_lines"`
}

type goldenChange struct {
	LogicalID  string `json:"logical_id"`
	Role       string `json:"role"`
	Node       string `json:"node"`
	Type       string `json:"type"`
	Confidence string `json:"confidence"`
	LeadSec    int    `json:"occurred_lead_sec"`
	Summary    string `json:"summary"`
}

// ===================== 数据模型：注入器 answerbook =====================

type answerbook struct {
	StartedAt     time.Time   `json:"started_at"`
	PlaybookStart time.Time   `json:"playbook_start"`
	WarmupSec     int         `json:"warmup_sec"`
	CycleSec      int         `json:"cycle_sec"`
	Segments      []abSegment `json:"segments"`
}

type abSegment struct {
	Scenario    string    `json:"scenario"`
	Name        string    `json:"name"`
	DurationSec int       `json:"duration_sec"`
	Expected    string    `json:"expected"`
	ExpectedNew int       `json:"expected_new_incidents"`
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
}

// ===================== 数据模型：RCA REST 契约（rest_rca.go） =====================

type rcaResp struct {
	IncidentID   string    `json:"incident_id"`
	T0           time.Time `json:"t0"`
	ClusterKeys  []string  `json:"cluster_keys"`
	AlertedNodes []string  `json:"alerted_nodes"`
	Evidence     struct {
		Nodes   int `json:"nodes"`
		Edges   int `json:"edges"`
		Changes int `json:"changes"`
	} `json:"evidence"`
	Steps []struct {
		Name       string `json:"name"`
		Status     string `json:"status"`
		DurationMS int64  `json:"duration_ms"`
		Error      string `json:"error"`
	} `json:"steps"`
	RootCauses []struct {
		Step       string `json:"step"`
		Summary    string `json:"summary"`
		Confidence string `json:"confidence"`
		Ref        string `json:"ref"`
	} `json:"root_causes"`
	Conclusion  *string `json:"conclusion"`
	LLMUsed     bool    `json:"llm_used"`
	Persistence string  `json:"persistence"`
}

// ===================== 数据模型：评测单元与汇总 =====================

type unit struct {
	Kind    string `json:"kind"` // rca | silence_guard
	SegIdx  int    `json:"seg_idx"`
	SegCode string `json:"seg_code"`
	SegName string `json:"seg_name"`

	SegStart time.Time `json:"seg_start"`
	SegEnd   time.Time `json:"seg_end"`

	ClusterID  string    `json:"cluster_id,omitempty"`
	ClusterKey string    `json:"cluster_key,omitempty"`
	IncidentID string    `json:"incident_id,omitempty"`
	T0         time.Time `json:"t0,omitempty"`

	ExpectedChange string   `json:"expected_change,omitempty"`
	ExpectedDomain []string `json:"expected_domain,omitempty"`
	AlertedNodes   []string `json:"alerted_nodes,omitempty"`
	RootRefs       []string `json:"root_refs,omitempty"`
	RootConfidence []string `json:"root_confidence,omitempty"`
	// ForeignRefs：根因链里混入的其他评测 run 的变更。回放已按装配租户
	// 过滤（internal/topology/change_pg.go LoadSince，评测 0129078 实锤后
	// 收口），本字段常态应为空——非空即租户过滤回归（漏装配租户/回滚），
	// 不判 FAIL（top1 仍须正确），但进 findings 响亮暴露。
	ForeignRefs []string `json:"foreign_refs,omitempty"`

	Status           string `json:"status"` // OK / ALERT_MISS / CLUSTER_SPLIT / CLUSTER_MERGED / INCIDENT_MISS / ATTACH_FAIL / RCA_HTTP_ERR / RCA_UNPARSEABLE / GUARD_PASS / GUARD_FAIL
	Top1             bool   `json:"top1"`
	Top3             bool   `json:"top3"`
	EvidenceComplete bool   `json:"evidence_complete"`
	AuditPresent     bool   `json:"audit_present"`
	LLMUsed          bool   `json:"llm_used"`

	// LLM 转正三门禁（gates.go）：G2 看 Conclusion/ConcludeDone/LLMUsed，
	// G3 看 G3Status（fail=矛盾话术硬失败，suspect=进存疑清单人工终审）。
	Conclusion   string   `json:"conclusion,omitempty"` // 全文（判分用）；报告只回显摘要
	ConcludeDone bool     `json:"conclude_done"`
	G3Anchors    []string `json:"g3_anchors,omitempty"`
	G3Status     string   `json:"g3_status,omitempty"` // pass | suspect | fail | ""(无结论)
	G3Matched    string   `json:"g3_matched,omitempty"`
	G3Phrases    []string `json:"g3_phrases,omitempty"`

	DetectLagMS     int64 `json:"detect_lag_ms"`     // T0 - 段起点（告警→成单可见延迟）
	RCAHTTPMS       int64 `json:"rca_http_ms"`       // GET /rca 端到端
	AuditDurationMS int64 `json:"audit_duration_ms"` // 审计 detail.duration_ms（编排层耗时）
	StepsDone       int   `json:"steps_done"`
	StepsPending    int   `json:"steps_pending"`
	StepsFailed     int   `json:"steps_failed"`

	ShadowAlerts int `json:"shadow_alerts_in_window,omitempty"` // 守护：段窗影子判决数
	NewIncidents int `json:"new_incidents_in_window,omitempty"` // 守护：段窗外来新建事件数

	Notes []string `json:"notes,omitempty"`
}

type summary struct {
	RunID       string    `json:"run_id"`
	Tenant      string    `json:"tenant"`
	Mode        string    `json:"mode"` // evidence(--no-llm) | llm
	GeneratedAt time.Time `json:"generated_at"`
	Threshold   float64   `json:"threshold"`

	UnitsTotal     int     `json:"units_total"`     // RCA 评测单元数
	UnitsCompleted int     `json:"units_completed"` // 跑到 RCA 且解析成功
	Top1Hits       int     `json:"top1_hits"`
	Top3Hits       int     `json:"top3_hits"`
	EvidenceOK     int     `json:"evidence_complete"`
	GuardsTotal    int     `json:"silence_guards_total"`
	GuardsPass     int     `json:"silence_guards_pass"`
	Top1Rate       float64 `json:"top1_rate"`
	Top3Rate       float64 `json:"top3_rate"`
	EvidenceRate   float64 `json:"evidence_complete_rate"`

	DetectLagMS quantiles `json:"detect_lag_ms"`
	RCAHTTPMS   quantiles `json:"rca_http_ms"`
	AuditDurMS  quantiles `json:"audit_duration_ms"`
	Findings    []string  `json:"findings"`
	GatePassed  bool      `json:"gate_passed"`
	// LLM 转正三门禁（--no-llm 时 Applicable=false，仅 LLM 模式承载转正判定）。
	Promotion promotion `json:"promotion"`
}

type quantiles struct {
	N   int   `json:"n"`
	Min int64 `json:"min"`
	P50 int64 `json:"p50"`
	P95 int64 `json:"p95"`
	Max int64 `json:"max"`
}

// computeQuantiles 最近秩法（评测样本量个位数，无需插值）。
func computeQuantiles(vs []int64) quantiles {
	q := quantiles{}
	if len(vs) == 0 {
		return q
	}
	s := append([]int64(nil), vs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	q.N = len(s)
	q.Min = s[0]
	q.Max = s[q.N-1]
	q.P50 = s[(q.N*50+99)/100-1]
	q.P95 = s[(q.N*95+99)/100-1]
	return q
}

// ===================== main =====================

func main() {
	var (
		appURL  = flag.String("app", envOr("OPS_APP_URL", "http://127.0.0.1:8080"), "被测 app 基址")
		injURL  = flag.String("injector", envOr("OPS_INJECTOR_URL", "http://127.0.0.1:19090"), "faultinjector 基址")
		dsn     = flag.String("dsn", os.Getenv("OPS_DB_DSN"), "PG DSN（缺省读 OPS_DB_DSN）")
		token   = flag.String("token", envOr("OPS_WEBHOOK_TOKEN", "dev"), "写路径共享密钥（X-OpsCopilot-Token）")
		tenant  = flag.String("tenant", os.Getenv("OPS_TENANT"), "评测租户（必须与被测 app 同租户；评测纪律：独立租户，杜绝历史数据污染）")
		goldenP = flag.String("golden", "tools/rca_eval/golden.json", "golden 定义文件")
		outDir  = flag.String("out-dir", ".rca-eval", "JSONL/summary 产物目录（.gitignore 已覆盖）")
		repDir  = flag.String("report-dir", "", "report.md 输出目录（空 = 不写，只出控制台与 JSONL）")
		noLLM   = flag.Bool("no-llm", true, "true=证据版基线（断言 conclude pending / llm_used=false）；false=LLM 模式，启用 G1/G2/G3 转正三门禁（gates.go）")
		thr     = flag.Float64("threshold", 0.85, "W12 转正门禁线（作用于 top1 命中率；证据版基线跑分时仅作参考并如实报告）")
		lead    = flag.Duration("inject-lead", 6*time.Second, "段起点前多少秒注入变更")
		grace   = flag.Duration("incident-grace", 40*time.Second, "周期结束后等待成单落库的宽限（阶段 2 起始门槛）")
		pollTO  = flag.Duration("unit-timeout", 180*time.Second, "阶段 2 单单元轮询上限（成单/审计）")
		totalTO = flag.Duration("timeout", 25*time.Minute, "整体超时")
	)
	flag.Parse()

	if *dsn == "" {
		fatal(2, "no -dsn (set OPS_DB_DSN)")
	}
	// 第七轮 H3 教训（tools/verdicts）：空租户查询静默 0 行 → 假 100%。
	// 评测纪律：必须显式独立租户，不做 "default" 兜底。
	if strings.TrimSpace(*tenant) == "" {
		fatal(2, "no -tenant（评测纪律：独立租户，避免与常驻影子数据互污并杜绝历史建单幂等挡住新建）")
	}
	runSuffix := strings.TrimPrefix(*tenant, "rca-eval-")

	ctx, cancel := context.WithTimeout(context.Background(), *totalTO)
	defer cancel()

	g := mustLoadGolden(*goldenP)
	for _, seg := range g.Segments {
		for _, a := range seg.Alerts {
			goldenRefsCache = append(goldenRefsCache, promFingerprint(map[string]string{
				"alertname": a.Alertname, "instance": a.Instance, "job": a.Job, "severity": a.Severity,
			}))
		}
	}
	pool := mustPool(ctx, *dsn)
	defer pool.Close()
	ensureTenant(ctx, pool, *tenant)

	ab := fetchAnswerbook(ctx, *injURL)
	driftCheck(g, ab)

	waitTopologyReady(ctx, *appURL, *token, g)

	// ---------- 阶段 1：严格按剧本时间线注入变更 ----------
	injectPhase(ctx, g, ab, *appURL, *token, runSuffix, *lead)

	// ---------- 阶段 2：全周期 + 宽限后统一取证判分（无时间压力） ----------
	lastEnd := ab.Segments[len(g.Segments)-1].End
	waitUntil(ctx, lastEnd.Add(*grace))

	units := scorePhase(ctx, g, ab, pool, *tenant, *appURL, *token, runSuffix, *pollTO, *noLLM)

	s := buildSummary(units, runSuffix, *tenant, *noLLM, *thr)
	writeOutputs(s, units, *outDir, *repDir, g, ab, *noLLM)
	printConsole(s, units)

	if !s.GatePassed {
		os.Exit(1)
	}
}

// ===================== golden 与 drift =====================

func mustLoadGolden(path string) *goldenDoc {
	raw, err := os.ReadFile(path)
	if err != nil {
		fatal(2, "read golden: %v", err)
	}
	var g goldenDoc
	if err := json.Unmarshal(raw, &g); err != nil {
		fatal(2, "parse golden: %v", err)
	}
	if len(g.Segments) == 0 {
		fatal(2, "golden has no segments")
	}
	return &g
}

// driftCheck golden vs 注入器 answerbook：段序（场景码）与 expected_new 必须
// 逐段一致；duration 允许多级缩放（-scale）但一个周期内必须齐步。
// 不一致 = 答案漂移，评测作废（退出码 2），这正是"golden 维护"的自动化防线。
func driftCheck(g *goldenDoc, ab *answerbook) {
	if len(ab.Segments) < len(g.Segments) {
		fatal(2, "answerbook has %d segments, golden expects %d", len(ab.Segments), len(g.Segments))
	}
	var problems []string
	for i := range g.Segments {
		seg, abs := g.Segments[i], ab.Segments[i]
		if seg.Code != abs.Scenario {
			problems = append(problems, fmt.Sprintf("段 %d：golden 场景码 %s ≠ answerbook %s", i, seg.Code, abs.Scenario))
		}
		if seg.ExpectedNew != abs.ExpectedNew {
			problems = append(problems, fmt.Sprintf("段 %d(%s)：golden expected_new_incidents=%d ≠ answerbook=%d", i, seg.Code, seg.ExpectedNew, abs.ExpectedNew))
		}
		if abs.DurationSec <= 0 || seg.DurationSec%abs.DurationSec != 0 {
			problems = append(problems, fmt.Sprintf("段 %d(%s)：answerbook 时长 %ds 不是 golden %ds 的整约（缩放异常？）", i, seg.Code, abs.DurationSec, seg.DurationSec))
		}
	}
	if len(problems) > 0 {
		fmt.Fprintln(os.Stderr, "GOLDEN DRIFT —— golden.json 与 faultinjector 剧本已漂移：")
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "  - "+p)
		}
		fmt.Fprintln(os.Stderr, "修复：同步 tools/rca_eval/golden.json（来源行号见文件头）后重跑。")
		os.Exit(2)
	}
	fmt.Printf("drift check: OK（%d 段与 answerbook 对齐，缩放 %d→%ds/段）\n",
		len(g.Segments), g.Segments[0].DurationSec, ab.Segments[0].DurationSec)
}

// ===================== 阶段 1：注入 =====================

func injectPhase(ctx context.Context, g *goldenDoc, ab *answerbook, appURL, token, runSuffix string, lead time.Duration) {
	for i := range g.Segments {
		seg := g.Segments[i]
		if seg.Silence || len(seg.Changes) == 0 {
			continue
		}
		abs := ab.Segments[i]
		at := abs.Start.Add(-lead)
		waitUntil(ctx, at)
		for _, c := range seg.Changes {
			if c.Role == "note" || c.Node == "" {
				continue
			}
			id := "chg-" + runSuffix + "-" + c.LogicalID
			body := map[string]any{
				"id":          id,
				"node_key":    "prometheus://nodes/" + c.Node,
				"type":        c.Type,
				"source":      "rca-eval",
				"author":      "rca-eval",
				"summary":     c.Summary,
				"occurred_at": abs.Start.Add(-time.Duration(c.LeadSec) * time.Second).UTC().Format(time.RFC3339Nano),
				"confidence":  c.Confidence,
			}
			if err := postJSON(ctx, appURL+"/api/v1/changes", token, body); err != nil {
				fatal(2, "注入变更 %s（段 %s）失败：%v（节点可能未入拓扑——warmup 不足？）", id, seg.Code, err)
			}
			fmt.Printf("inject  seg=%s role=%-22s id=%s node=%s conf=%s\n", seg.Code, c.Role, id, c.Node, c.Confidence)
		}
	}
}

// warnIfAlertSetDrifted 指纹级漂移由阶段 2 的 alert_event 对账兜底
// （ALERT_MISS/CLUSTER_SPLIT 状态即答案漂移的暴露形态）——注入前探测会
// 与段起点的注入时序抢跑，不做。

// ===================== 阶段 2：取证与判分 =====================

func scorePhase(ctx context.Context, g *goldenDoc, ab *answerbook, pool *pgxpool.Pool,
	tenant, appURL, token, runSuffix string, unitTO time.Duration, noLLM bool) []unit {
	var units []unit
	for i := range g.Segments {
		seg := g.Segments[i]
		abs := ab.Segments[i]
		if seg.Silence {
			units = append(units, runSilenceGuard(ctx, pool, tenant, seg, abs))
			continue
		}
		units = append(units, scoreFaultSegment(ctx, pool, tenant, appURL, token, runSuffix, seg, abs, unitTO, noLLM)...)
	}
	return units
}

func scoreFaultSegment(ctx context.Context, pool *pgxpool.Pool, tenant, appURL, token, runSuffix string,
	seg goldenSegment, abs abSegment, unitTO time.Duration, noLLM bool) []unit {
	fmt.Printf("\n== 段 %s（%s）%s ~ %s ==\n", seg.Code, seg.Name,
		abs.Start.Format("15:04:05"), abs.End.Format("15:04:05"))

	// ① 每个指纹实际落入的 cluster_key 集合。窗口尾 = 段末 + 45s 固定松弛：
	// 覆盖采集（10s）+ 判决落库节拍，同时远离下一周期同指纹的新簇键
	// （scale=0.1 全周期 180s）——松弛若取轮询超时会把第二遍剧本卷进来，
	// 误判 CLUSTER_SPLIT。
	const winSlack = 45 * time.Second
	fpKeys := map[string][]string{}
	for _, a := range seg.Alerts {
		keys := pollStrings(ctx, pool, unitTO, func(qctx context.Context) ([]string, error) {
			rows, err := pool.Query(qctx, `SELECT DISTINCT cluster_key FROM alert_event
WHERE tenant_id=$1 AND fingerprint=$2 AND occurred_at >= $3 AND occurred_at < $4`,
				tenant, a.Fingerprint, abs.Start.Add(-2*time.Second), abs.End.Add(winSlack))
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			var out []string
			for rows.Next() {
				var k string
				if err := rows.Scan(&k); err != nil {
					return nil, err
				}
				out = append(out, k)
			}
			return out, nil
		})
		fpKeys[a.Fingerprint] = keys
		if len(keys) == 0 {
			fmt.Printf("  ALERT_MISS fp=%s 窗口内无影子判决\n", a.Fingerprint)
		}
	}

	var units []unit
	seenKey := map[string]string{} // cluster_key → 首个占用它的 cluster id（跨簇合并检测）
	for _, cl := range seg.Clusters {
		u := unit{Kind: "rca", SegIdx: seg.Idx, SegCode: seg.Code, SegName: seg.Name,
			SegStart: abs.Start, SegEnd: abs.End, ClusterID: cl.ID,
			ExpectedDomain: fullNodeKeys(cl.Nodes), ExpectedChange: "chg-" + runSuffix + "-" + cl.RootChange}
		// 成员指纹 → 唯一簇键。
		keySet := map[string]bool{}
		split := false
		for _, fp := range cl.Members {
			ks := fpKeys[fp]
			if len(ks) == 0 {
				u.Status = "ALERT_MISS"
				u.Notes = append(u.Notes, "指纹 "+fp+" 无 alert_event 判决（采集缺口或剧本漂移）")
			} else if len(ks) > 1 {
				split = true
			}
			for _, k := range ks {
				keySet[k] = true
			}
		}
		if u.Status == "ALERT_MISS" {
			units = append(units, u)
			continue
		}
		if split {
			u.Status = "CLUSTER_SPLIT"
			u.Notes = append(u.Notes, "同一指纹在窗口内落入多个簇（聚类不稳定）")
		}
		keys := sortedKeys(keySet)
		if len(keys) != 1 {
			// 期望 1 簇拿到 0 或 ≥2 个键：0 已在 ALERT_MISS；≥2 = 期望簇被拆分或多键。
			u.Status = firstNonEmpty(u.Status, "CLUSTER_SPLIT")
			u.Notes = append(u.Notes, fmt.Sprintf("期望单簇，实际 cluster_key 集合=%v", keys))
			units = append(units, u)
			continue
		}
		key := keys[0]
		if owner, ok := seenKey[key]; ok {
			u.Status = "CLUSTER_MERGED"
			u.Notes = append(u.Notes, fmt.Sprintf("期望独立簇 %s 与他簇 %s 同键（故障域过度聚合）", cl.ID, owner))
			units = append(units, u)
			continue
		}
		seenKey[key] = cl.ID
		u.ClusterKey = key

		// ② representative 指纹对应的 prometheus 链路事件。
		rep := fpBy(seg, cl.Represent)
		ref := promFingerprint(map[string]string{
			"alertname": rep.Alertname, "instance": rep.Instance, "job": rep.Job, "severity": rep.Severity,
		})
		rowID, incID, t0 := pollIncident(ctx, pool, unitTO, tenant, ref)
		if incID == "" {
			u.Status = "INCIDENT_MISS"
			u.Notes = append(u.Notes, "incident 未按 (origin=prometheus, source_ref="+ref+") 建成（拉取/消费链路断）")
			units = append(units, u)
			continue
		}
		u.IncidentID = incID
		u.T0 = t0
		u.DetectLagMS = t0.Sub(abs.Start).Milliseconds()

		// ③ 挂簇：生产自动挂簇路径优先（W10-6 OPS_AUTOATTACH=on +
		// OPS_NOISE_MODE=enforce，run_rca_eval.sh 评测接线默认如此）——
		// new-incident 判决已联动 UpsertExternal+AttachCluster，轮询
		// incident_cluster 等到簇挂上即证明生产路径；评测对象 app 未开
		// 开关时回退按 AttachCluster SQL 语义直写（兼容旧形态，note 显式
		// 标注"回退桥接"，报告 §5 据此判定生产路径未被验证）。
		ownerID, attached := pollClusterOwner(ctx, pool, tenant, key, 30*time.Second)
		switch {
		case attached && ownerID == incID:
			u.Notes = append(u.Notes, "挂簇走生产自动路径（OPS_AUTOATTACH new-incident 联动，W10-6）")
		case attached:
			// 一簇一事件：簇挂在首单（簇创建者的事件）而非代表指纹选中的
			// 事件——按生产口径给持域事件判分（多指纹共簇仅首单有域）。
			t0o, oerr := incidentT0(ctx, pool, tenant, ownerID)
			if oerr != nil {
				u.Status = "ATTACH_FAIL"
				u.Notes = append(u.Notes, fmt.Sprintf("簇持有者 %s 回查失败: %v", ownerID, oerr))
				units = append(units, u)
				continue
			}
			u.Notes = append(u.Notes, fmt.Sprintf(
				"簇由首单 %s 持有（代表指纹选中 %s；一簇一事件，评分跟随持域事件）", ownerID, incID))
			incID, t0 = ownerID, t0o
			u.IncidentID, u.T0, u.DetectLagMS = incID, t0, t0.Sub(abs.Start).Milliseconds()
		default:
			if err := attachCluster(ctx, pool, rowID, key); err != nil {
				u.Status = "ATTACH_FAIL"
				u.Notes = append(u.Notes, err.Error())
				units = append(units, u)
				continue
			}
			u.Notes = append(u.Notes, "回退桥接挂簇：被测 app 未开 OPS_AUTOATTACH（生产挂簇路径未被验证）")
		}
		// 非代表指纹的事件同簇存在但无法再挂（一簇一事件约束）——如实注记。
		if len(cl.Members) > 1 {
			u.Notes = append(u.Notes, fmt.Sprintf("成员 %v 中仅 %s 的事件挂了簇（一簇一事件约束，见报告 §5）",
				cl.Members, cl.Represent))
		}

		// ④ GET /rca 判分。
		reqStart := time.Now()
		var rr rcaResp
		code, err := getJSON(ctx, appURL+"/api/v1/incidents/"+incID+"/rca?actor=rca-eval", token, &rr)
		u.RCAHTTPMS = time.Since(reqStart).Milliseconds()
		if err != nil {
			u.Status = fmt.Sprintf("RCA_HTTP_ERR(%d)", code)
			u.Notes = append(u.Notes, err.Error())
			units = append(units, u)
			continue
		}
		u.AlertedNodes = rr.AlertedNodes
		for _, rc := range rr.RootCauses {
			u.RootRefs = append(u.RootRefs, rc.Ref)
			u.RootConfidence = append(u.RootConfidence, rc.Confidence)
		}
		u.StepsDone, u.StepsPending, u.StepsFailed = countSteps(rr)
		// LLM 三门禁字段（G1 复用上面 Top1；这里填 G2/G3 输入）。
		myPrefix := "chg-" + runSuffix + "-"
		logicalNode := map[string]string{}
		for _, c := range seg.Changes {
			if c.Node != "" {
				logicalNode[c.LogicalID] = "prometheus://nodes/" + c.Node
			}
		}
		hydrateUnit(&u, &rr, myPrefix, logicalNode)
		if len(u.RootRefs) > 0 && u.RootRefs[0] == u.ExpectedChange {
			u.Top1 = true
		}
		for j, r := range u.RootRefs {
			if j < 3 && r == u.ExpectedChange {
				u.Top3 = true
				break
			}
		}
		// 跨 run 变更证据泄漏回归探测（回放已按装配租户过滤——change_pg.go
		// LoadSince，见 unit 字段注释）——top1 判定与此无关，非空即响亮记账。
		for _, r := range u.RootRefs {
			if !strings.HasPrefix(r, myPrefix) {
				u.ForeignRefs = append(u.ForeignRefs, r)
			}
		}
		if len(u.ForeignRefs) > 0 {
			u.Notes = append(u.Notes, fmt.Sprintf("根因链混入他 run 变更 %v（回放租户过滤回归——§5 暴露项）", u.ForeignRefs))
		}

		// ⑤ 审计直读（#4 落库形态）：action='rca' 行的 detail。
		auditDetail := pollAuditRCA(ctx, pool, unitTO, tenant, incID)
		if auditDetail != nil {
			u.AuditPresent = true
			u.AuditDurationMS = jsonInt(auditDetail, "duration_ms")
			// 审计与响应互校（一致性证据，不计分但记注记）。
			if int(jsonInt(auditDetail, "steps_done")) != u.StepsDone ||
				int(jsonInt(auditDetail, "root_causes")) != len(u.RootRefs) {
				u.Notes = append(u.Notes, "WARNING: 审计 detail 与 REST 响应不一致（steps_done/root_causes）")
			}
		} else {
			u.Notes = append(u.Notes, "incident_audit 无 action='rca' 行（#4 落库失效？）")
		}

		// 证据完整率口径（报告 §1 明示）：域一致 + 取证非空 + 六步形态 +
		// 审计落库 + （--no-llm 时）conclusion 恒 null 且 llm_used=false。
		ownChanges := countInjectable(seg.Changes)
		domainMatch := sameStringSet(u.AlertedNodes, u.ExpectedDomain)
		stepsOK := len(rr.Steps) == 6 && u.StepsFailed == 0 &&
			((noLLM && u.StepsDone == 5 && u.StepsPending == 1 && rr.Conclusion == nil && !rr.LLMUsed) ||
				(!noLLM && u.StepsDone == 6 && u.StepsPending == 0))
		u.EvidenceComplete = u.Status == "" && domainMatch && rr.Evidence.Nodes >= len(u.ExpectedDomain) &&
			rr.Evidence.Changes >= ownChanges && stepsOK && u.AuditPresent
		if u.Status == "" {
			u.Status = "OK"
		}
		if !domainMatch {
			u.Notes = append(u.Notes, fmt.Sprintf("故障域不一致：RCA=%v 期望=%v", u.AlertedNodes, u.ExpectedDomain))
		}
		fmt.Printf("  unit=%-7s inc=%s top1=%v top3=%v evid=%v refs=%v\n",
			cl.ID, incID, u.Top1, u.Top3, u.EvidenceComplete, u.RootRefs)
		units = append(units, u)
	}
	return units
}

// runSilenceGuard 静默段守护：段窗内影子判决必须为 0；新建事件不得出现
// 剧本指纹之外的行（上一段迟到的建单幂等刷进本窗是正常行为，不算违规）。
func runSilenceGuard(ctx context.Context, pool *pgxpool.Pool, tenant string,
	seg goldenSegment, abs abSegment) unit {
	u := unit{Kind: "silence_guard", SegIdx: seg.Idx, SegCode: seg.Code, SegName: seg.Name,
		SegStart: abs.Start, SegEnd: abs.End}
	// 影子判决：本段窗口内任何 alert_event 行都是"无告警时段产生判决"。
	var alerts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM alert_event
WHERE tenant_id=$1 AND occurred_at >= $2 AND occurred_at < $3`, tenant, abs.Start, abs.End).Scan(&alerts); err != nil {
		u.Status = "GUARD_FAIL"
		u.Notes = append(u.Notes, "alert_event 计数失败: "+err.Error())
		return u
	}
	u.ShadowAlerts = alerts
	// 新建事件：窗口内 created_at 且 (origin, source_ref) 不属于任何剧本指纹。
	var incs int
	q := `SELECT count(*) FROM incident WHERE tenant_id=$1 AND created_at >= $2 AND created_at < $3`
	args := []any{tenant, abs.Start, abs.End}
	if len(goldenRefsCache) > 0 {
		q += ` AND NOT (source_ref = ANY($4))`
		args = append(args, goldenRefsCache)
	}
	if err := pool.QueryRow(ctx, q, args...).Scan(&incs); err != nil {
		u.Status = "GUARD_FAIL"
		u.Notes = append(u.Notes, "incident 计数失败: "+err.Error())
		return u
	}
	u.NewIncidents = incs
	u.ExpectedChange = ""
	if alerts == 0 && incs == 0 {
		u.Status = "GUARD_PASS"
	} else {
		u.Status = "GUARD_FAIL"
		u.Notes = append(u.Notes, fmt.Sprintf("静默段出现 %d 条影子判决 / %d 个剧本外新建事件（误报）", alerts, incs))
	}
	fmt.Printf("== 段 %s（%s）guard=%s（verdicts=%d new_incidents=%d）\n", seg.Code, seg.Name, u.Status, alerts, incs)
	return u
}

// goldenRefsCache 剧本全部告警指纹的 prom source_ref（main 启动时预计算）——
// 静默段守护据此把"上一段迟到落库的合法建单"排除出误报计数。
var goldenRefsCache []string

// ===================== 汇总 =====================

func buildSummary(units []unit, runID, tenant string, noLLM bool, thr float64) summary {
	s := summary{RunID: runID, Tenant: tenant, Threshold: thr, GeneratedAt: time.Now().UTC()}
	if noLLM {
		s.Mode = "evidence(--no-llm)"
	} else {
		s.Mode = "llm"
	}
	var lags, https, durs []int64
	for _, u := range units {
		switch {
		case u.Kind == "silence_guard":
			s.GuardsTotal++
			if u.Status == "GUARD_PASS" {
				s.GuardsPass++
			} else {
				s.Findings = append(s.Findings, fmt.Sprintf("静默守护 FAIL（段 %d %s）：%v", u.SegIdx, u.SegName, u.Notes))
			}
		case u.Status == "OK":
			s.UnitsTotal++
			s.UnitsCompleted++
			lags = append(lags, u.DetectLagMS)
			https = append(https, u.RCAHTTPMS)
			durs = append(durs, u.AuditDurationMS)
			if u.Top1 {
				s.Top1Hits++
			} else {
				s.Findings = append(s.Findings, fmt.Sprintf("归因 FAIL top1（%s/%s）：期望 %s，实际根因序 %v（置信 %v）——%v",
					u.SegCode, u.ClusterID, u.ExpectedChange, u.RootRefs, u.RootConfidence, u.Notes))
			}
			if u.Top3 {
				s.Top3Hits++
			} else if !u.Top1 {
				s.Findings = append(s.Findings, fmt.Sprintf("归因 FAIL top3（%s/%s）：期望 %s 未进前三——根因漏报或证据链断裂",
					u.SegCode, u.ClusterID, u.ExpectedChange))
			}
			if u.EvidenceComplete {
				s.EvidenceOK++
			} else {
				s.Findings = append(s.Findings, fmt.Sprintf("证据不完整（%s/%s）：%v", u.SegCode, u.ClusterID, u.Notes))
			}
			if len(u.ForeignRefs) > 0 {
				s.Findings = append(s.Findings, fmt.Sprintf("跨 run 变更证据泄漏回归（%s/%s）：根因链含他 run 变更 %v——LoadSince 回放应按装配租户过滤（internal/topology/change_pg.go，评测 0129078 实锤后收口），非空即过滤失守/回滚，转正前必须查",
					u.SegCode, u.ClusterID, u.ForeignRefs))
			}
		default:
			s.UnitsTotal++
			s.Findings = append(s.Findings, fmt.Sprintf("单元 %s/%s 未完成（status=%s）：%v",
				u.SegCode, u.ClusterID, u.Status, u.Notes))
		}
	}
	if s.UnitsCompleted > 0 {
		s.Top1Rate = float64(s.Top1Hits) / float64(s.UnitsCompleted)
		s.Top3Rate = float64(s.Top3Hits) / float64(s.UnitsCompleted)
		s.EvidenceRate = float64(s.EvidenceOK) / float64(s.UnitsCompleted)
	}
	s.DetectLagMS = computeQuantiles(lags)
	s.RCAHTTPMS = computeQuantiles(https)
	s.AuditDurMS = computeQuantiles(durs)
	s.Promotion = evalPromotion(units, noLLM, thr, s.GuardsTotal, s.GuardsPass)
	if noLLM {
		// 证据版基线：规则链参考读数 + 守护，转正判定不适用（报告 §7 明示）。
		s.GatePassed = s.UnitsTotal > 0 && s.UnitsTotal == s.UnitsCompleted &&
			s.Top1Rate >= s.Threshold && s.GuardsTotal == s.GuardsPass
	} else {
		// LLM 模式：总判定 = 三门禁（G1 全单元 ≥阈值 ∧ G2 100% ∧ G3 无硬性
		// 失败）∧ 静默守护全过。未跑通的单元在 G1/G2 分母里挂账，不存在
		//"从分母消失刷通过率"的通道。
		s.GatePassed = s.UnitsTotal > 0 && s.Promotion.Pass
		for _, g := range s.Promotion.G2Fails {
			s.Findings = append(s.Findings, fmt.Sprintf("G2 结论产出 FAIL（%s/%s，status=%s）：缺 %v",
				g.SegCode, g.ClusterID, g.Status, g.Missing))
		}
		for _, h := range s.Promotion.HardFails {
			s.Findings = append(s.Findings, fmt.Sprintf("G3 硬性失败（%s/%s，ref=%s）：conclusion 出现矛盾话术 %v——%q",
				h.SegCode, h.ClusterID, h.Ref, h.Phrases, truncateRunes(h.Conclusion, 120)))
		}
		for _, su := range s.Promotion.Suspects {
			s.Findings = append(s.Findings, fmt.Sprintf("G3 存疑（%s/%s，ref=%s）：锚点未命中，进 §7 存疑清单人工终审",
				su.SegCode, su.ClusterID, su.Ref))
		}
	}
	return s
}

// ===================== PG/HTTP 小工具 =====================

func mustPool(ctx context.Context, dsn string) *pgxpool.Pool {
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		fatal(2, "pg pool: %v", err)
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		fatal(2, "pg unreachable: %v", err)
	}
	return p
}

func ensureTenant(ctx context.Context, pool *pgxpool.Pool, tenant string) {
	if _, err := pool.Exec(ctx, `INSERT INTO tenant (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING`, tenant); err != nil {
		fatal(2, "ensure tenant %s: %v（评测需独立租户）", tenant, err)
	}
}

// pollStrings 轮询查询直至非空或超时（超时也返回空——状态判定在调用方）。
func pollStrings(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration,
	q func(context.Context) ([]string, error)) []string {
	deadline := time.Now().Add(timeout)
	for {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		out, err := q(cctx)
		cancel()
		if err == nil && len(out) > 0 {
			return out
		}
		if time.Now().After(deadline) {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
}

func pollIncident(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration,
	tenant, sourceRef string) (int64, string, time.Time) {
	deadline := time.Now().Add(timeout)
	for {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var rowID int64
		var incID string
		var t0 time.Time
		err := pool.QueryRow(cctx, `SELECT id, incident_id, created_at FROM incident
WHERE tenant_id=$1 AND origin='prometheus' AND source_ref=$2
ORDER BY created_at DESC LIMIT 1`, tenant, sourceRef).Scan(&rowID, &incID, &t0)
		cancel()
		if err == nil {
			return rowID, incID, t0
		}
		if err != pgx.ErrNoRows {
			fatal(2, "incident 查询失败: %v", err)
		}
		if time.Now().After(deadline) {
			return 0, "", time.Time{}
		}
		time.Sleep(2 * time.Second)
	}
}

// pollClusterOwner 轮询 incident_cluster：簇是否已被**生产自动挂簇路径**
// （W10-6 OPS_AUTOATTACH）挂上——挂上则返回现主 incident_id。超时未挂返回
// ("", false)，由调用方回退桥接直写。租户过滤经 incident 表 join
// （incident_cluster 本身无 tenant 列，簇键全局唯一）。
func pollClusterOwner(ctx context.Context, pool *pgxpool.Pool, tenant, clusterKey string,
	timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	for {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var owner string
		err := pool.QueryRow(cctx, `SELECT i.incident_id FROM incident_cluster ic
JOIN incident i ON i.id = ic.incident_row_id
WHERE ic.cluster_key=$1 AND i.tenant_id=$2`, clusterKey, tenant).Scan(&owner)
		cancel()
		if err == nil {
			return owner, true
		}
		if err != pgx.ErrNoRows {
			fatal(2, "incident_cluster 查询失败: %v", err)
		}
		if time.Now().After(deadline) {
			return "", false
		}
		time.Sleep(2 * time.Second)
	}
}

// incidentT0 按 incident_id 回查创建时刻（簇持有者改判时重算 detect_lag 用）。
func incidentT0(ctx context.Context, pool *pgxpool.Pool, tenant, incID string) (time.Time, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var t0 time.Time
	err := pool.QueryRow(cctx, `SELECT created_at FROM incident
WHERE tenant_id=$1 AND incident_id=$2`, tenant, incID).Scan(&t0)
	return t0, err
}

// attachCluster 与 internal/incident PGStore.AttachCluster 同语义的**回退**
// 桥接（W10-6 起生产自动挂簇已有产品化挂点 OPS_AUTOATTACH；被测 app 未开
// 开关的旧形态评测才走到这里——note 显式标注，报告 §5 据此判定生产路径未验证）。
func attachCluster(ctx context.Context, pool *pgxpool.Pool, rowID int64, clusterKey string) error {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var prev string
	err := pool.QueryRow(cctx, `SELECT i.incident_id FROM incident_cluster ic
JOIN incident i ON i.id = ic.incident_row_id WHERE ic.cluster_key=$1`, clusterKey).Scan(&prev)
	switch {
	case err == nil:
		if prev != "" {
			return fmt.Errorf("cluster %s 已挂事件 %s（评测租户应为空）", clusterKey, prev)
		}
	case err != pgx.ErrNoRows:
		return err
	}
	if _, err := pool.Exec(cctx, `INSERT INTO incident_cluster (incident_row_id, cluster_key)
VALUES ($1, $2) ON CONFLICT DO NOTHING`, rowID, clusterKey); err != nil {
		return fmt.Errorf("挂簇: %w", err)
	}
	return nil
}

func pollAuditRCA(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration,
	tenant, incID string) map[string]any {
	deadline := time.Now().Add(timeout)
	for {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var detail []byte
		err := pool.QueryRow(cctx, `SELECT detail FROM incident_audit
WHERE tenant_id=$1 AND incident_id=$2 AND action='rca' ORDER BY occurred_at DESC LIMIT 1`,
			tenant, incID).Scan(&detail)
		cancel()
		if err == nil {
			var m map[string]any
			if json.Unmarshal(detail, &m) == nil {
				return m
			}
			return map[string]any{}
		}
		if err != pgx.ErrNoRows {
			fatal(2, "incident_audit 查询失败: %v", err)
		}
		if time.Now().After(deadline) {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
}

func fetchAnswerbook(ctx context.Context, injURL string) *answerbook {
	var ab answerbook
	if _, err := getJSON(ctx, injURL+"/answerbook", "", &ab); err != nil {
		fatal(2, "注入器 /answerbook 不可达：%v（先跑 scripts/run_rca_eval.sh 或手起 faultinjector）", err)
	}
	return &ab
}

func waitTopologyReady(ctx context.Context, appURL, token string, g *goldenDoc) {
	want := map[string]bool{}
	for _, seg := range g.Segments {
		for _, cl := range seg.Clusters {
			for _, n := range cl.Nodes {
				want["prometheus://nodes/"+n] = false
			}
		}
	}
	deadline := time.Now().Add(150 * time.Second)
	for {
		body, err := getRaw(ctx, appURL+"/api/v1/topology", token)
		missing := []string{}
		if err == nil {
			for k := range want {
				if !strings.Contains(body, k) {
					missing = append(missing, k)
				} else {
					want[k] = true
				}
			}
		}
		if err == nil && len(missing) == 0 {
			fmt.Printf("topology ready: %d 节点全部入图（发现建图完成）\n", len(want))
			return
		}
		if time.Now().After(deadline) {
			fatal(2, "拓扑就绪超时，缺: %v（app 未起/OPS_PROM_URL 未指注入器/warmup 不足？）", missing)
		}
		time.Sleep(2 * time.Second)
	}
}

func postJSON(ctx context.Context, url, token string, body any) error {
	raw, _ := json.Marshal(body)
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, url, strings.NewReader(string(raw)))
	if err != nil {
		return err
	}
	if token != "" {
		req.Header.Set("X-OpsCopilot-Token", token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

func getJSON(ctx context.Context, url, token string, out any) (int, error) {
	body, code, err := getRawCode(ctx, url, token)
	if err != nil {
		return code, err
	}
	if perr := json.Unmarshal(body, out); perr != nil {
		return code, fmt.Errorf("decode: %w", perr)
	}
	return code, nil
}

func getRaw(ctx context.Context, url, token string) (string, error) {
	b, _, err := getRawCode(ctx, url, token)
	return string(b), err
}

func getRawCode(ctx context.Context, url, token string) ([]byte, int, error) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	if token != "" {
		req.Header.Set("X-OpsCopilot-Token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return b, resp.StatusCode, fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return b, resp.StatusCode, nil
}

// promFingerprint 与 cmd/opscopilot/pull_alerts.go promFingerprint()
// （L140-158）逐字节一致：sha256(sorted "k=v\n") 前 16B hex，"prom:" 前缀。
// 漂移即 INCIDENT_MISS——golden 维护协议第 5 条要求两处同步核对。
func promFingerprint(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
		b.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "prom:" + hex.EncodeToString(sum[:16])
}

// ===================== 杂项 =====================

func fpBy(seg goldenSegment, fingerprint string) goldenAlert {
	for _, a := range seg.Alerts {
		if a.Fingerprint == fingerprint {
			return a
		}
	}
	return goldenAlert{}
}

// fullNodeKeys golden 里域节点用裸实例名（n1，与 faultinjector 源码一致），
// RCA 回显的是全键（prometheus://nodes/n1）——比对前统一补前缀（已全键的原样过）。
func fullNodeKeys(ns []string) []string {
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		if strings.Contains(n, "://") {
			out = append(out, n)
			continue
		}
		out = append(out, "prometheus://nodes/"+n)
	}
	return out
}

func countInjectable(cs []goldenChange) int {
	n := 0
	for _, c := range cs {
		if c.Role != "note" && c.Node != "" {
			n++
		}
	}
	return n
}

func countSteps(rr rcaResp) (done, pending, failed int) {
	for _, s := range rr.Steps {
		switch s.Status {
		case "done":
			done++
		case "pending":
			pending++
		case "failed":
			failed++
		}
	}
	return
}

func jsonInt(m map[string]any, k string) int64 {
	if v, ok := m[k]; ok {
		if f, ok := v.(float64); ok {
			return int64(f)
		}
		if s, ok := v.(string); ok {
			var i int64
			fmt.Sscanf(s, "%d", &i)
			return i
		}
	}
	return 0
}

func sameStringSet(a, b []string) bool {
	sa, sb := append([]string(nil), a...), append([]string(nil), b...)
	sort.Strings(sa)
	sort.Strings(sb)
	if len(sa) != len(sb) {
		return false
	}
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func waitUntil(ctx context.Context, t time.Time) {
	for {
		d := time.Until(t)
		if d <= 0 {
			return
		}
		if d > time.Second {
			d = time.Second
		}
		select {
		case <-ctx.Done():
			fatal(2, "超时/取消：等待时刻 %s 未完成", t.Format(time.RFC3339))
		case <-time.After(d):
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func fatal(code int, format string, args ...any) {
	fmt.Fprintf(os.Stderr, "rca_eval: "+format+"\n", args...)
	os.Exit(code)
}
