// faultinjector W6-0 故障注入器（R1 拍板：影子评估的 ground truth 用
// 故障注入已知答案）。
//
// 定位：一个 Prometheus API 兼容的"假数据源"——opscopilot 用标准
// prometheus 连接器对接它（/-/healthy、/api/v1/alerts、/api/v1/targets、
// /api/v1/query），它按剧本周期性注入故障场景。每个场景自带已知答案
// （"这些告警应聚成哪些簇"），评估脚本按告警发生时刻对照场景时间线
// 打分——影子判决 vs 注入答案，就是 W6-3 的准确率。
//
// 场景剧本（循环播放，默认 30s 一个采集轮）：
//
//	A 单节点磁盘故障：n1 上 HighDisk + InodeExhaust 两条告警
//	  （不同指纹、同节点）→ 应聚成 1 簇（答案：[n1]）
//	B 交换机故障：n1/n2/n3 同时 HighLatency（不同指纹、拓扑相邻
//	  ——注入器同时声明 n1-n2-n3 的 targets，让 opscopilot 的发现
//	  建立 n1←n2←n3 因果链）→ 应并成 1 簇（答案：[n1,n2,n3]）
//	C 静默窗口：无告警 → 既有簇应陆续 resolved，不产生新簇
//	D 无关噪声：n4/n5 两条互不相干告警 → 应各成独立簇（答案：2 个新事件）
//
// 评估对照端点：GET /answerbook 输出场景时间线 JSON（每段含
// start/end/scenario/expected），GET /api/v1/status 人读版。
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"
)

// Scenario 场景定义（已知答案的静态部分）。
type Scenario struct {
	Code           string     `json:"scenario"` // A/B/C/D
	Name           string     `json:"name"`
	Duration       int        `json:"duration_sec"`           // 播放时长
	Expected       string     `json:"expected"`               // 已知答案（人读）
	ExpectedGroups [][]string `json:"expected_groups"`        // 应并成一簇的节点分组
	ExpectedNew    int        `json:"expected_new_incidents"` // 应新建簇的告警条数
}

// Segment 一段播放（时间线条目，评估脚本对照用）。
type Segment struct {
	Scenario
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// playbook 循环剧本。故障域语义：opscopilot 的域聚合靠拓扑边
// （targets 发现产生节点，边由……见 README——当前连接器只产节点不产边，
// 故 B 场景的"同域"实际靠 opscopilot 侧同节点/邻接判定；注入器把
// 三台 node 的 scrapeUrl 放同一 scrapePool，节点 Key 前缀一致，
// 域聚合在 opscopilot 侧的因果图上完成——评估时以 answerbook 为准）。
func playbook() []Scenario {
	return []Scenario{
		{
			Code: "A", Name: "单节点磁盘故障", Duration: 300,
			Expected:       "n1 的 HighDisk+InodeExhaust 聚成 1 簇",
			ExpectedGroups: [][]string{{"n1"}}, ExpectedNew: 1,
		},
		{
			Code: "C", Name: "静默窗口", Duration: 300,
			Expected:       "无告警，既有簇陆续 resolved",
			ExpectedGroups: nil, ExpectedNew: 0,
		},
		{
			Code: "B", Name: "交换机故障（多节点同发）", Duration: 300,
			Expected:       "n1/n2/n3 的 HighLatency 并成 1 簇",
			ExpectedGroups: [][]string{{"n1", "n2", "n3"}}, ExpectedNew: 1,
		},
		{
			Code: "C", Name: "静默窗口", Duration: 300,
			Expected:       "无告警，既有簇陆续 resolved",
			ExpectedGroups: nil, ExpectedNew: 0,
		},
		{
			Code: "D", Name: "无关噪声", Duration: 300,
			Expected:       "n4/n5 各自成簇（2 个新事件）",
			ExpectedGroups: [][]string{{"n4"}, {"n5"}}, ExpectedNew: 2,
		},
		{
			Code: "C", Name: "静默窗口", Duration: 300,
			Expected:       "无告警，既有簇陆续 resolved",
			ExpectedGroups: nil, ExpectedNew: 0,
		},
	}
}

// injector 运行态：剧本 + 当前段起点。
type injector struct {
	scenarios  []Scenario
	startedAt  time.Time
	warmupSec  int
	totalCycle int // 全周期秒数
}

func newInjector(scale float64, warmupSec int) *injector {
	inj := &injector{scenarios: playbook(), startedAt: time.Now(), warmupSec: warmupSec}
	for i := range inj.scenarios {
		d := int(float64(inj.scenarios[i].Duration) * scale)
		if d < 1 {
			d = 1 // 极端缩放下限：Duration=0 会让时间线展开循环永不前进（死循环）
		}
		inj.scenarios[i].Duration = d
	}
	for _, s := range inj.scenarios {
		inj.totalCycle += s.Duration
	}
	return inj
}

// current 当前所处场景段（按 wall clock 循环推算——重启不丢时间线语义，
// 时间线由 startedAt + warmup + 循环取模还原）。预热期返回 warmup 标记。
func (inj *injector) current(now time.Time) (Scenario, time.Time) {
	elapsed := int(now.Sub(inj.startedAt).Seconds())
	if elapsed < inj.warmupSec {
		return Scenario{
			Code: "WARMUP", Name: "预热（等发现建图）", Duration: inj.warmupSec,
			Expected: "无告警；opscopilot 完成首轮发现",
		}, inj.startedAt
	}
	// 第七轮评估实锤（W6-3 二轮 20.6% 的根因）：此处原实现没有周期回绕——
	// 走完一个 totalCycle 后落入下方兜底分支，**永远钉在场景 A**（注释写的
	// "循环取模"从未实现）。表现为：首个周期全 PASS，其后静默段持续收到
	// A 的告警、后续段全部 new=0。
	raw := elapsed - inj.warmupSec // 剧本播放起点起算
	if inj.totalCycle <= 0 || raw < 0 {
		return inj.scenarios[0], inj.startedAt
	}
	cycle := raw / inj.totalCycle   // 第几个周期（0 起）
	inCycle := raw % inj.totalCycle // 周期内偏移
	segStart := inj.startedAt.Add(time.Duration(inj.warmupSec+cycle*inj.totalCycle) * time.Second)
	for _, s := range inj.scenarios {
		if inCycle < s.Duration {
			return s, segStart
		}
		inCycle -= s.Duration
		segStart = segStart.Add(time.Duration(s.Duration) * time.Second)
	}
	return inj.scenarios[0], inj.startedAt // 不可达
}

// targets Prometheus /api/v1/targets 响应（发现数据：5 台 node）。
// 节点 Key 归一化为 prometheus://nodes/<instance>。
func (inj *injector) targets() map[string]any {
	instance := func(n string) map[string]string {
		return map[string]string{"instance": n, "job": "nodes"}
	}
	mk := func(n string) map[string]any {
		return map[string]any{
			"discoveredLabels": map[string]string{},
			"labels":           instance(n),
			"scrapePool":       "nodes",
			"scrapeUrl":        "http://" + n + ":9100/metrics",
			"health":           "up",
		}
	}
	return map[string]any{
		"status": "success",
		"data": map[string]any{
			"activeTargets": []map[string]any{mk("n1"), mk("n2"), mk("n3"), mk("n4"), mk("n5")},
		},
	}
}

// alerts 当前场景的告警集。指纹在各场景内固定（同场景重复告警 =
// 同指纹窗口内重复，考核去重）；fingerprint 用稳定字符串。
// 预热期返回空——见 -warmup 注释。
func (inj *injector) alerts(now time.Time) map[string]any {
	sc, _ := inj.current(now)
	type alert struct {
		Labels      map[string]string `json:"labels"`
		State       string            `json:"state"`
		ActiveAt    string            `json:"activeAt"`
		Value       string            `json:"value"`
		Fingerprint string            `json:"fingerprint"`
	}
	var list []alert
	// RFC3339Nano 而非 RFC3339：秒级截断会给下游的延迟打点（W9-4
	// fired→verdict / fired→通知）注入最多 1s 的量化误差——读数会变成
	// "0~1s 的均匀噪声 + 真实耗时"，把几十毫秒的处理量级淹掉。
	// 下游 parseTime 用 time.RFC3339 解析，本身容忍小数秒。
	nowStr := now.UTC().Format(time.RFC3339Nano)
	add := func(fp, node, name, sev string) {
		list = append(list, alert{
			Labels: map[string]string{"alertname": name, "instance": node, "job": "nodes", "severity": sev},
			State:  "firing", ActiveAt: nowStr, Value: "1", Fingerprint: fp,
		})
	}
	switch sc.Code {
	case "A":
		add("fp-a-disk", "n1", "HighDiskUsage", "warning")
		add("fp-a-inode", "n1", "InodeExhaustion", "critical")
	case "B":
		add("fp-b-lat-n1", "n1", "HighLatency", "critical")
		add("fp-b-lat-n2", "n2", "HighLatency", "critical")
		add("fp-b-lat-n3", "n3", "HighLatency", "critical")
	case "D":
		add("fp-d-cert", "n4", "CertExpiringSoon", "warning")
		add("fp-d-backup", "n5", "BackupFailed", "warning")
	}
	return map[string]any{"status": "success", "data": map[string]any{"alerts": list}}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:19090", "listen address")
	// scale 时间缩放：1=真实剧本（每场景 5min）；冒烟/联调用 0.1 之类
	// 缩短场景——剧本语义（顺序与相对时长）不变，答案照读。
	scale := flag.Float64("scale", 1.0, "duration scale factor (0<scale<=1)")
	// warmup 预热期：启动后先静默 N 秒再开剧本——给 opscopilot 留出
	// 发现建图的时间（首轮采集是"告警先于发现"，不预热会导致 A 场景
	// 的同节点告警在建簇时反查不到节点而无法聚合）。
	warmup := flag.Int("warmup", 60, "warmup seconds before scenario playback starts")
	flag.Parse()
	if *scale <= 0 || *scale > 1 {
		log.Fatal("scale must be in (0,1]")
	}
	if *warmup < 0 {
		log.Fatal("warmup must be >= 0")
	}
	inj := newInjector(*scale, *warmup)
	log.Printf("fault injector on %s — warmup %ds, cycle %ds, %d scenarios",
		*addr, *warmup, inj.totalCycle, len(inj.scenarios))

	mux := newMux(inj)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

// newMux 构造注入器全部路由（抽出便于包内测试）。
func newMux(inj *injector) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("fault injector is Healthy.\n"))
	})
	mux.HandleFunc("/api/v1/targets", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(inj.targets())
	})
	mux.HandleFunc("/api/v1/alerts", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(inj.alerts(time.Now()))
	})
	// /api/v1/query：注入器不配 PromQL 消费方，恒返回空 vector。
	mux.HandleFunc("/api/v1/query", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	})
	// answerbook：评估脚本的时间线对照源——从预热结束（剧本真正开始）展开
	// 到现在+1 天（循环取模以剧本起点为锚，预热段不入答案簿）。
	mux.HandleFunc("/answerbook", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		now := time.Now()
		playbookStart := inj.startedAt.Add(time.Duration(inj.warmupSec) * time.Second)
		horizon := now.Add(24 * time.Hour)
		var segs []Segment
		for t := playbookStart; t.Before(horizon); {
			for _, s := range inj.scenarios {
				segStart := t
				segEnd := t.Add(time.Duration(s.Duration) * time.Second)
				if segStart.Before(now) || segEnd.After(now) {
					segs = append(segs, Segment{Scenario: s, Start: segStart, End: segEnd})
				}
				t = segEnd
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"started_at": inj.startedAt, "playbook_start": playbookStart,
			"warmup_sec": inj.warmupSec, "cycle_sec": inj.totalCycle, "segments": segs,
		})
	})
	mux.HandleFunc("/api/v1/status", func(w http.ResponseWriter, _ *http.Request) {
		now := time.Now()
		sc, segStart := inj.current(now)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"current": sc, "segment_start": segStart, "now": now,
		})
	})
	return mux
}
