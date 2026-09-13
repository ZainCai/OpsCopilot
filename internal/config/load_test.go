package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// envMap 把 map 适配成 LookupFunc（测试不碰进程环境）。
func envMap(m map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

// redisEnv 最小合法环境：只给必填的双 Redis 地址。
func redisEnv(alertAddr, cacheAddr string) map[string]string {
	return map[string]string{
		EnvRedisAlertAddr: alertAddr,
		EnvRedisCacheAddr: cacheAddr,
	}
}

// TestLoadFromDefaults 只给必填项时，其余字段必须等于 Defaults() 基线
// （即"缺失取默认、默认值与现状一致"）。
func TestLoadFromDefaultsRCA(t *testing.T) {
	c, err := LoadFrom(envMap(redisEnv("127.0.0.1:6380", "127.0.0.1:6381")))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !c.RCA.Enabled || c.RCA.Timeout != DefaultRCATimeout ||
		c.RCA.Window != DefaultRCAWindow || c.RCA.Depth != DefaultRCADepth {
		t.Fatalf("rca defaults = %+v, want on/%s/%s/%d", c.RCA, DefaultRCATimeout, DefaultRCAWindow, DefaultRCADepth)
	}
	if c.RCA.MaxFindings != DefaultRCAMaxFindings {
		t.Fatalf("OPS_RCA_MAX_FINDINGS default = %d, want %d", c.RCA.MaxFindings, DefaultRCAMaxFindings)
	}
}

// TestRCAMaxFindingsKey OPS_RCA_MAX_FINDINGS（二期池波二 #6）表驱动：
// 缺失 = 200（与转正前 rest_rca.go 硬编码上限一致，默认行为零漂移）、
// 合法覆盖生效、非正整数汇入统一聚合报错（fail-fast）。
func TestRCAMaxFindingsKey(t *testing.T) {
	t.Run("override", func(t *testing.T) {
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvRCAMaxFindings] = "50"
		c, err := LoadFrom(envMap(env))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if c.RCA.MaxFindings != 50 {
			t.Fatalf("override not applied: %+v", c.RCA)
		}
	})
	t.Run("invalid-aggregate", func(t *testing.T) {
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvRCAMaxFindings] = "0" // 非正 → 拒绝（0 不承诺"不限"语义，防手滑关掉上限）
		env[EnvRCAWindow] = "abc"    // 陪衬项：确认汇入同一聚合报错
		_, err := LoadFrom(envMap(env))
		var agg *Error
		if !errors.As(err, &agg) || len(agg.Errs) != 2 {
			t.Fatalf("want 2 aggregated errors, got %v", err)
		}
		for _, key := range []string{EnvRCAMaxFindings, EnvRCAWindow} {
			if !strings.Contains(err.Error(), key) {
				t.Errorf("aggregate must mention %s: %v", key, err)
			}
		}
	})
}

func TestLoadFromDefaultsRCAAuto(t *testing.T) {
	// 默认 off：保持 ADR-014"只做按需"现状。
	c, err := LoadFrom(envMap(redisEnv("127.0.0.1:6380", "127.0.0.1:6381")))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.RCA.Auto {
		t.Fatalf("OPS_RCA_AUTO default must be off, got on")
	}
	// 显式 on（在 Enabled 默认 on 之下）合法解析。
	env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
	env[EnvRCAAuto] = "on"
	c2, err := LoadFrom(envMap(env))
	if err != nil {
		t.Fatalf("load auto=on: %v", err)
	}
	if !c2.RCA.Auto || !c2.RCA.Enabled {
		t.Fatalf("auto=on parse = %+v, want Auto on / Enabled on", c2.RCA)
	}
}

func TestValidateRCAAutoRequiresEnabled(t *testing.T) {
	// OPS_RCA_AUTO=on 而 OPS_RCA=off：配置矛盾必须 fail-fast 拒绝启动。
	_, err := LoadFrom(envMap(func() map[string]string {
		e := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		e[EnvRCAEnabled] = "off"
		e[EnvRCAAuto] = "on"
		return e
	}()))
	if err == nil || !strings.Contains(err.Error(), EnvRCAAuto) {
		t.Fatalf("RCA_AUTO=on with RCA=off must be rejected mentioning %s, got %v", EnvRCAAuto, err)
	}
}

func TestLoadFromDefaultsSession(t *testing.T) {
	// 二期池 #7：OPS_SESSION 默认 off（全链路零行为变化）。
	c, err := LoadFrom(envMap(redisEnv("127.0.0.1:6380", "127.0.0.1:6381")))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Session.Enabled {
		t.Fatal("OPS_SESSION default must be off")
	}
	env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
	env[EnvSessionEnabled] = "on" // RCA 默认 on，合法组合
	c2, err := LoadFrom(envMap(env))
	if err != nil || !c2.Session.Enabled {
		t.Fatalf("session=on with rca=on must load: %v (%+v)", err, c2.Session)
	}
}

func TestValidateSessionRequiresRCAEnabled(t *testing.T) {
	// 二期池 #7（拍板④）：会话 prompt 必携带 RCA findings，OPS_SESSION=on 而
	// OPS_RCA=off 是配置矛盾——与 RCA_AUTO 同款 fail-fast。
	_, err := LoadFrom(envMap(func() map[string]string {
		e := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		e[EnvRCAEnabled] = "off"
		e[EnvSessionEnabled] = "on"
		return e
	}()))
	if err == nil || !strings.Contains(err.Error(), EnvSessionEnabled) {
		t.Fatalf("SESSION=on with RCA=off must be rejected mentioning %s, got %v", EnvSessionEnabled, err)
	}
}

// TestLoadFromDefaultsAutoAttach W10-6：OPS_AUTOATTACH 默认 off（零行为），
// enforce+on 合法。
func TestLoadFromDefaultsAutoAttach(t *testing.T) {
	c, err := LoadFrom(envMap(redisEnv("127.0.0.1:6380", "127.0.0.1:6381")))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Noise.AutoAttach {
		t.Fatal("OPS_AUTOATTACH default must be off")
	}
	env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
	env[EnvNoiseMode] = "enforce"
	env[EnvAutoAttach] = "on"
	c2, err := LoadFrom(envMap(env))
	if err != nil || !c2.Noise.AutoAttach {
		t.Fatalf("autoattach=on with enforce must load: %v (%+v)", err, c2.Noise)
	}
}

func TestValidateAutoAttachRequiresEnforce(t *testing.T) {
	// W10-6：挂点在 enforce 判决 new-incident 联动路径——shadow 或降噪关闭
	// 时永不触发，同款 RCA_AUTO/SESSION 的 fail-fast。
	_, err := LoadFrom(envMap(func() map[string]string {
		e := redisEnv("127.0.0.1:6380", "127.0.0.1:6381") // Mode 默认 shadow
		e[EnvAutoAttach] = "on"
		return e
	}()))
	if err == nil || !strings.Contains(err.Error(), EnvAutoAttach) {
		t.Fatalf("AUTOATTACH=on with shadow must be rejected mentioning %s, got %v", EnvAutoAttach, err)
	}
	_, err = LoadFrom(envMap(func() map[string]string {
		e := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		e[EnvNoiseMode] = "enforce"
		e[EnvNoiseShadow] = "off" // 降噪整体关闭：同样无挂点可触发
		e[EnvAutoAttach] = "on"
		return e
	}()))
	if err == nil || !strings.Contains(err.Error(), EnvAutoAttach) {
		t.Fatalf("AUTOATTACH=on with noise disabled must be rejected, got %v", err)
	}
}

// TestValidateAutoCreateAutoAttachMatrix W10-4（ADR-016）配置矩阵：
// OPS_INCIDENT_AUTOCREATE × OPS_AUTOATTACH × OPS_NOISE_MODE 的联动合法性——
// AUTOCREATE 自身无 Validate 约束（worker 消费开关，任何组合都不构成"配置矛盾"）；
// AUTOATTACH 的 enforce 前置（fail-fast）不因 AUTOCREATE 开启而放松。
// 行为侧的"两路同开收敛一单"联动断言见 cmd 层
// TestAutoCreateAutoAttachConvergeSingleIncident（autocreate_matrix_test.go）。
func TestValidateAutoCreateAutoAttachMatrix(t *testing.T) {
	cases := []struct {
		name                     string
		autoCreate, attach, mode string
		wantErr                  string // "" = 合法装载；否则错误须含该子串
	}{
		{"转正组合 AUTOCREATE+AUTOATTACH+enforce 合法", "on", "on", "enforce", ""},
		{"AUTOCREATE=on 不放松 AUTOATTACH+shadow 矛盾", "on", "on", "shadow", EnvAutoAttach},
		{"AUTOCREATE=on 单独（AUTOATTACH off、shadow）合法", "on", "off", "shadow", ""},
		{"AUTOATTACH=on+enforce 与 AUTOCREATE=off 组合合法", "off", "on", "enforce", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
			env[EnvIngestAutoCreate] = tc.autoCreate
			env[EnvAutoAttach] = tc.attach
			env[EnvNoiseMode] = tc.mode
			c, err := LoadFrom(envMap(env))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want error mentioning %s, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if c.Ingest.AutoCreate != (tc.autoCreate == "on") {
				t.Fatalf("Ingest.AutoCreate = %v, want %s", c.Ingest.AutoCreate, tc.autoCreate)
			}
			if c.Noise.AutoAttach != (tc.attach == "on") {
				t.Fatalf("Noise.AutoAttach = %v, want %s", c.Noise.AutoAttach, tc.attach)
			}
		})
	}
}

func TestLoadFromDefaults(t *testing.T) {
	got, err := LoadFrom(envMap(redisEnv("127.0.0.1:6380", "127.0.0.1:6381")))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	want := Defaults()
	if *got != *want {
		t.Fatalf("loaded config differs from defaults:\n got %+v\nwant %+v", *got, *want)
	}
	// 逐组再比一遍（结构体含 time.Duration/int64，== 足够；分组报错更友好）。
	table := []struct {
		name      string
		got, want any
	}{
		{"redis", got.Redis, want.Redis},
		{"db", got.DB, want.DB},
		{"security", got.Security, want.Security},
		{"connector", got.Connector, want.Connector},
		{"noise", got.Noise, want.Noise},
		{"pull", got.Pull, want.Pull},
		{"ingest", got.Ingest, want.Ingest},
		{"leader", got.Leader, want.Leader},
		{"notify", got.Notify, want.Notify},
		{"retention", got.Retention, want.Retention},
		{"topology", got.Topology, want.Topology},
		{"memlimit", got.MemLimit, want.MemLimit},
		{"metrics", got.Metrics, want.Metrics},
	}
	for _, c := range table {
		if c.got != c.want {
			t.Errorf("section %s: got %+v want %+v", c.name, c.got, c.want)
		}
	}
}

// TestLoadFromAggregatesAllErrors 多个非法值必须**一次性全部列出**——
// 这是本次收敛唯一被批准的语义变化（fail-fast + 聚合报错）。
func TestLoadFromAggregatesAllErrors(t *testing.T) {
	env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
	env[EnvDBMaxConns] = "many"          // 原：WARNING 后静默取 pgx 默认
	env[EnvPullInterval] = "-5s"         // 原：WARNING 后静默取 30s
	env[EnvEscalationAfter] = "tomorrow" // 原：WARNING 后静默取 15m
	env[EnvNoiseWindow] = "not-a-duration"
	env[EnvNoiseMode] = "yolo"
	env[EnvAllowUnauthenticated] = "yes"
	env[EnvIncidentRetention] = "seven-days"
	env[EnvChangePruneInterval] = "0s"
	env[EnvCORSOrigin] = "*"
	env[EnvIngestBatch] = "-3"
	env[EnvLeaderElection] = "yes"     // #11：on/off 开关只认 on|off
	env[EnvLeaderRetryInterval] = "0s" // 非正 duration 同样拒绝
	env[EnvRCAEnabled] = "true"        // #12：开关只认 on|off
	env[EnvRCAAuto] = "yes"            // #4 二期：自动触发开关同样只认 on|off
	env[EnvRCASTimeout] = "-1s"        // #12：非正 duration 拒绝
	env[EnvRCAWindow] = "soon"         // #12：非法 duration 拒绝
	env[EnvRCADepth] = "0"             // #12：跳数须为正整数
	env[EnvRCAMaxFindings] = "-7"      // #6 二期：findings 上限须为正整数
	env[EnvSessionEnabled] = "true"    // #7 二期：复盘会话开关同样只认 on|off
	env[EnvAutoAttach] = "yes"         // W10-6：自动挂簇开关同样只认 on|off

	_, err := LoadFrom(envMap(env))
	var agg *Error
	if !errors.As(err, &agg) {
		t.Fatalf("want *Error, got %T: %v", err, err)
	}
	if len(agg.Errs) != 20 {
		t.Fatalf("want 20 aggregated errors, got %d: %v", len(agg.Errs), err)
	}
	for _, key := range []string{
		EnvDBMaxConns, EnvPullInterval, EnvEscalationAfter, EnvNoiseWindow,
		EnvNoiseMode, EnvAllowUnauthenticated, EnvIncidentRetention,
		EnvChangePruneInterval, EnvCORSOrigin, EnvIngestBatch,
		EnvLeaderElection, EnvLeaderRetryInterval,
		EnvRCAEnabled, EnvRCAAuto, EnvRCASTimeout, EnvRCAWindow, EnvRCADepth,
		EnvRCAMaxFindings, EnvSessionEnabled, EnvAutoAttach,
	} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("aggregated error must mention %s, got:\n%v", key, err)
		}
	}
}

// TestLoadFromRedisRequired 双 Redis 地址必填（G1"缺一拒绝启动"不因收敛放松）：
// 缺失即报错，且两个缺失报两条。
func TestLoadFromRedisRequired(t *testing.T) {
	env := map[string]string{EnvRedisAlertAddr: " "}
	_, err := LoadFrom(envMap(env))
	var agg *Error
	if !errors.As(err, &agg) {
		t.Fatalf("want *Error, got %v", err)
	}
	if len(agg.Errs) != 2 {
		t.Fatalf("want 2 required-address errors, got %d: %v", len(agg.Errs), err)
	}
	if !strings.Contains(err.Error(), EnvRedisAlertAddr) || !strings.Contains(err.Error(), EnvRedisCacheAddr) {
		t.Fatalf("errors must mention both env keys: %v", err)
	}
}

// TestLoadFromRedisSameInstance 同址（含 localhost 别名）必须在解析全绿后
// 被结构性校验拒绝——双实例物理隔离不放松。
func TestLoadFromRedisSameInstance(t *testing.T) {
	_, err := LoadFrom(envMap(redisEnv("localhost:6380", "127.0.0.1:6380")))
	if err == nil || !strings.Contains(err.Error(), "physically separate") {
		t.Fatalf("same-instance addresses must be rejected, got %v", err)
	}
}

// TestLoadFromValidOverrides 合法覆盖逐项解析（类型化：duration/int/枚举/开关）。
func TestLoadFromValidOverrides(t *testing.T) {
	env := redisEnv("redis-alert:6379", "redis-cache:6379")
	env[EnvTenant] = " eval "
	env[EnvDBDSN] = "postgres://u:p@db:5432/ops"
	env[EnvDBMaxConns] = "24"
	env[EnvListenAddr] = "0.0.0.0:9090"
	env[EnvAllowUnauthenticated] = "ON"
	env[EnvCORSOrigin] = "https://ops.example.com"
	env[EnvPromURL] = "http://prom:9090"
	env[EnvPromToken] = "tok"
	env[EnvAzureSubID] = "sub"
	env[EnvAzureToken] = "arm"
	env[EnvConnInterval] = "1m"
	env[EnvConnTimeout] = "45s"
	env[EnvNoiseShadow] = "on"
	env[EnvNoiseWindow] = "30m"
	env[EnvNoiseMode] = "ENFORCE"
	env[EnvDedupWindow] = "1h"
	env[EnvPullAlerts] = "on"
	env[EnvPullInterval] = "10s"
	env[EnvIngestAutoCreate] = "on"
	env[EnvIngestInterval] = "2s"
	env[EnvIngestBatch] = "100"
	env[EnvIngestRateLimit] = "10"
	env[EnvIngestRateWindow] = "10m"
	env[EnvIngestBatchPerItem] = "20s"
	env[EnvIngestAlertBodyLimit] = "8388608"
	env[EnvIngestLeaseDuration] = "45s"
	env[EnvEscalation] = "on"
	env[EnvEscalationAfter] = "1h"
	env[EnvEscalationInterval] = "5m"
	env[EnvSLACriticalMinutes] = "30"
	env[EnvSLAWarningMinutes] = "120"
	env[EnvSLAInfoMinutes] = "480"
	env[EnvKPIWindow] = "24h"
	env[EnvIncidentRetention] = "30d"
	env[EnvTopologyEdges] = "n1->n2,n2->n3"
	env[EnvChangeRetention] = "off"
	env[EnvChangePruneInterval] = "30m"
	env[EnvRCAEnabled] = "off"
	env[EnvRCASTimeout] = "30s"
	env[EnvRCAWindow] = "1h"
	env[EnvRCADepth] = "3"
	env[EnvRCAMaxFindings] = "500"

	c, err := LoadFrom(envMap(env))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	checks := []struct {
		name string
		ok   bool
	}{
		{"tenant trimmed", c.Tenant == "eval"},
		{"db", c.DB.DSN == env[EnvDBDSN] && c.DB.MaxConns == 24},
		{"listen", c.Security.ListenAddr == "0.0.0.0:9090"},
		{"allow-unauth case-insensitive", c.Security.AllowUnauthenticated},
		{"cors", c.Security.CORSOrigin == "https://ops.example.com"},
		{"connector", c.Connector.PromURL == "http://prom:9090" && c.Connector.Interval == time.Minute && c.Connector.OpTimeout == 45*time.Second},
		{"noise", c.Noise.Enabled && c.Noise.Window == 30*time.Minute && c.Noise.Mode == NoiseModeEnforce && c.Noise.DedupWindow == time.Hour},
		{"pull", c.Pull.Enabled && c.Pull.Interval == 10*time.Second},
		{"ingest", c.Ingest.AutoCreate && c.Ingest.Interval == 2*time.Second && c.Ingest.Batch == 100 &&
			c.Ingest.RateLimit == 10 && c.Ingest.RateWindow == 10*time.Minute &&
			c.Ingest.BatchTimeoutPerItem == 20*time.Second && c.Ingest.AlertBodyLimit == 8388608 &&
			c.Ingest.LeaseDuration == 45*time.Second},
		{"notify", c.Notify.EscalationEnabled && c.Notify.EscalationAfter == time.Hour && c.Notify.EscalationInterval == 5*time.Minute},
		{"sla minutes", c.SLA.CriticalMinutes == 30 && c.SLA.WarningMinutes == 120 && c.SLA.InfoMinutes == 480},
		{"kpi window", c.KPI.Window == 24*time.Hour},
		{"retention 30d", c.Retention.IncidentWindow == 30*24*time.Hour && c.Retention.IncidentEnabled},
		{"edges raw", c.Topology.Edges == "n1->n2,n2->n3"},
		{"change off", !c.Topology.ChangeEnabled && c.Topology.ChangePruneInterval == 30*time.Minute},
		{"redis", c.Redis.Alert.Addr == "redis-alert:6379" && c.Redis.Cache.Addr == "redis-cache:6379"},
		{"rca", !c.RCA.Enabled && c.RCA.Timeout == 30*time.Second && c.RCA.Window == time.Hour && c.RCA.Depth == 3},
	}
	for _, ck := range checks {
		if !ck.ok {
			t.Errorf("check %s failed; config: %+v", ck.name, *c)
		}
	}
}

// TestLoadFromTokenNotTrimmed 写密钥原样承接（含首尾空白的令牌是合法值，
// Trim 会造成"配了密钥却对不上"的隐性故障）。
func TestLoadFromTokenNotTrimmed(t *testing.T) {
	env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
	env[EnvWebhookToken] = " a b "
	c, err := LoadFrom(envMap(env))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Security.WebhookToken != " a b " {
		t.Fatalf("token must be taken verbatim, got %q", c.Security.WebhookToken)
	}
}

// TestParseRetention 保留窗解析（工单/变更两路共用）：表驱动。
func TestParseRetention(t *testing.T) {
	const def = 7 * 24 * time.Hour
	cases := []struct {
		raw    string
		window time.Duration
		on     bool
		bad    bool
	}{
		{"", def, true, false},
		{"-", def, true, false},
		{"off", 0, false, false},
		{"0", 0, false, false},
		{"never", 0, false, false},
		{"7d", def, true, false},
		{"168h", def, true, false},
		{"90d", 90 * 24 * time.Hour, true, false},
		{"bogus", 0, false, true},
		{"-5d", 0, false, true},
	}
	for _, c := range cases {
		w, on, err := ParseRetention(c.raw, def)
		if c.bad {
			if err == nil {
				t.Errorf("ParseRetention(%q): want error", c.raw)
			}
			continue
		}
		if err != nil || w != c.window || on != c.on {
			t.Errorf("ParseRetention(%q) = %v,%v,%v; want %v,%v,nil", c.raw, w, on, err, c.window, c.on)
		}
	}
}

// TestMemLimitKeys OPS_MEMLIMIT_* 组（优化方案 #6）表驱动：
// 缺失 = 保守默认（演示规模不可能触发）、合法覆盖逐项生效、
// 非法值汇入统一聚合报错。
func TestMemLimitKeys(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		c, err := LoadFrom(envMap(redisEnv("127.0.0.1:6380", "127.0.0.1:6381")))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		want := Defaults().MemLimit
		if c.MemLimit != want {
			t.Fatalf("memlimit defaults drifted:\n got %+v\nwant %+v", c.MemLimit, want)
		}
		if want.TopologyNodes < 100000 || want.Incidents < 10000 {
			t.Fatal("默认上限必须保守到演示规模不可能触发（#6 要求）")
		}
	})
	t.Run("overrides", func(t *testing.T) {
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvMemLimitWarnRatio] = "0.5"
		env[EnvMemLimitTopologyNodes] = "1000"
		env[EnvMemLimitTopologyEdges] = "4000"
		env[EnvMemLimitIncidents] = "50"
		env[EnvMemLimitEscalationLedger] = "60"
		env[EnvMemLimitNoiseDedup] = "70"
		env[EnvMemLimitNoiseClusters] = "80"
		env[EnvMemLimitNoiseSigCache] = "90"
		env[EnvMemLimitAudit] = "42"
		c, err := LoadFrom(envMap(env))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		ok := c.MemLimit.WarnRatio == 0.5 && c.MemLimit.TopologyNodes == 1000 &&
			c.MemLimit.TopologyEdges == 4000 && c.MemLimit.Incidents == 50 &&
			c.MemLimit.EscalationLedger == 60 && c.MemLimit.NoiseDedup == 70 &&
			c.MemLimit.NoiseClusters == 80 && c.MemLimit.NoiseSigCache == 90 &&
			c.MemLimit.Audit == 42
		if !ok {
			t.Fatalf("memlimit overrides not applied: %+v", c.MemLimit)
		}
	})
	t.Run("invalid-aggregate", func(t *testing.T) {
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvMemLimitTopologyNodes] = "0" // 非正 → 拒绝
		env[EnvMemLimitWarnRatio] = "1.5"   // 比例出界 → 拒绝
		env[EnvMemLimitAudit] = "plenty"    // 非数字 → 拒绝
		_, err := LoadFrom(envMap(env))
		var agg *Error
		if !errors.As(err, &agg) || len(agg.Errs) != 3 {
			t.Fatalf("want 3 aggregated errors, got %v", err)
		}
		for _, key := range []string{EnvMemLimitTopologyNodes, EnvMemLimitWarnRatio, EnvMemLimitAudit} {
			if !strings.Contains(err.Error(), key) {
				t.Errorf("aggregate must mention %s: %v", key, err)
			}
		}
	})
}

// TestNoiseSinkKeys OPS_NOISE_SINK_* 组（优化方案 #8 判决异步落库，队满
// 丢弃取向）：缺失 = 保守默认（正常规模几乎不触发队满丢弃路径）、合法
// 覆盖逐项生效、非法值汇入统一聚合报错（fail-fast，#2 通道）。
func TestNoiseSinkKeys(t *testing.T) {
	t.Run("defaults-conservative", func(t *testing.T) {
		c, err := LoadFrom(envMap(redisEnv("127.0.0.1:6380", "127.0.0.1:6381")))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		want := Defaults().Noise
		if c.Noise.SinkQueue != want.SinkQueue || c.Noise.SinkBatch != want.SinkBatch ||
			c.Noise.SinkFlush != want.SinkFlush || c.Noise.SinkDrain != want.SinkDrain {
			t.Fatalf("sink defaults drifted: %+v", c.Noise)
		}
		// 保守性约束（#8 修订）：队列容量至少能缓冲 20 个满批——正常节拍
		// （百条/30s + 单批毫秒级落库）下 DB 短暂停摆也不触"队满丢弃"路径
		// （丢弃是评估数据链缺行，不是通知丢失，但仍应罕见且有告警面）。
		if want.SinkQueue < 20*want.SinkBatch {
			t.Fatalf("SinkQueue(%d) must buffer >=20 full batches (batch=%d) to keep drops rare",
				want.SinkQueue, want.SinkBatch)
		}
	})
	t.Run("overrides", func(t *testing.T) {
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvNoiseSinkQueue] = "50"
		env[EnvNoiseSinkBatch] = "10"
		env[EnvNoiseSinkFlush] = "500ms"
		env[EnvNoiseSinkDrain] = "2s"
		c, err := LoadFrom(envMap(env))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if c.Noise.SinkQueue != 50 || c.Noise.SinkBatch != 10 ||
			c.Noise.SinkFlush != 500*time.Millisecond || c.Noise.SinkDrain != 2*time.Second {
			t.Fatalf("sink overrides not applied: %+v", c.Noise)
		}
	})
	t.Run("invalid-aggregate", func(t *testing.T) {
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvNoiseSinkQueue] = "0"   // 非正 → 拒绝
		env[EnvNoiseSinkBatch] = "abc" // 非整数 → 拒绝
		env[EnvNoiseSinkFlush] = "-3s" // 非正 duration → 拒绝
		env[EnvNoiseSinkDrain] = "0s"  // 非正 duration → 拒绝
		_, err := LoadFrom(envMap(env))
		var agg *Error
		if !errors.As(err, &agg) || len(agg.Errs) != 4 {
			t.Fatalf("want 4 aggregated errors, got %v", err)
		}
		for _, key := range []string{EnvNoiseSinkQueue, EnvNoiseSinkBatch, EnvNoiseSinkFlush, EnvNoiseSinkDrain} {
			if !strings.Contains(err.Error(), key) {
				t.Errorf("aggregate must mention %s: %v", key, err)
			}
		}
	})
}

// TestLLMKeys OPS_LLM_* 组（二期池波二 #3 / ADR-015）表驱动：
// 缺失 = 禁用（Endpoint 空、conclude 维持 pending 现状）+ 保守默认
// （8s/1024，且默认满足 8s < RCA 10s 的预算纪律）；合法覆盖逐项生效；
// 类型非法汇入统一聚合报错；结构性约束（URL 形态 / Model 必填 /
// 超时预算）单独拒启。
func TestLLMKeys(t *testing.T) {
	t.Run("defaults-disabled", func(t *testing.T) {
		c, err := LoadFrom(envMap(redisEnv("127.0.0.1:6380", "127.0.0.1:6381")))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if c.LLM.Endpoint != "" || c.LLM.APIKey != "" || c.LLM.Model != "" {
			t.Fatalf("llm must default to disabled: %+v", c.LLM)
		}
		if c.LLM.Timeout != DefaultLLMTimeout || c.LLM.MaxTokens != DefaultLLMMaxTokens {
			t.Fatalf("llm defaults drifted: %+v", c.LLM)
		}
		// 超时预算纪律的默认值自证：LLM 8s 必须严格小于 RCA 10s。
		if c.LLM.Timeout >= c.RCA.Timeout {
			t.Fatalf("default timeout budget violated: llm=%v rca=%v", c.LLM.Timeout, c.RCA.Timeout)
		}
	})
	t.Run("overrides", func(t *testing.T) {
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvLLMEndpoint] = "http://gw.internal:8000/v1"
		env[EnvLLMAPIKey] = " sk-secret\t " // 容忍尾部空白/回车；首部空白保留（原样凭证）
		env[EnvLLMModel] = "some-openai-compatible-model"
		env[EnvLLMTimeout] = "4s"
		env[EnvLLMMaxTokens] = "512"
		c, err := LoadFrom(envMap(env))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if c.LLM.Endpoint != "http://gw.internal:8000/v1" || c.LLM.APIKey != " sk-secret" ||
			c.LLM.Model != "some-openai-compatible-model" ||
			c.LLM.Timeout != 4*time.Second || c.LLM.MaxTokens != 512 {
			t.Fatalf("llm overrides not applied: %+v", c.LLM)
		}
	})
	t.Run("invalid-type-aggregate", func(t *testing.T) {
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvLLMTimeout] = "-1s" // 非正 duration → 拒绝
		env[EnvLLMMaxTokens] = "0" // 非正整数 → 拒绝
		_, err := LoadFrom(envMap(env))
		var agg *Error
		if !errors.As(err, &agg) || len(agg.Errs) != 2 {
			t.Fatalf("want 2 aggregated errors, got %v", err)
		}
		for _, key := range []string{EnvLLMTimeout, EnvLLMMaxTokens} {
			if !strings.Contains(err.Error(), key) {
				t.Errorf("aggregate must mention %s: %v", key, err)
			}
		}
	})
	t.Run("structural-reject", func(t *testing.T) {
		cases := []struct {
			name string
			env  map[string]string
			want string
		}{
			{"缺 model", map[string]string{EnvLLMEndpoint: "http://gw:8000/v1", EnvLLMTimeout: "2s"}, EnvLLMModel},
			// 预算：LLM + 2s 必须塞进 RCA 超时。默认 RCA=10s → LLM 最大 8s。
			{"预算吃紧-9s", map[string]string{EnvLLMEndpoint: "http://gw:8000/v1", EnvLLMModel: "m", EnvLLMTimeout: "9s"}, EnvLLMTimeout},
			{"预算超限", map[string]string{EnvLLMEndpoint: "http://gw:8000/v1", EnvLLMModel: "m", EnvLLMTimeout: "30s"}, EnvLLMTimeout},
			{"rca 收紧挤爆", map[string]string{EnvLLMEndpoint: "http://gw:8000/v1", EnvLLMModel: "m", EnvRCASTimeout: "5s"}, EnvLLMTimeout},
			{"畸形 endpoint", map[string]string{EnvLLMEndpoint: "gw:8000", EnvLLMModel: "m", EnvLLMTimeout: "2s"}, EnvLLMEndpoint},
			{"endpoint 无 scheme", map[string]string{EnvLLMEndpoint: "://bad", EnvLLMModel: "m"}, EnvLLMEndpoint},
		}
		for _, tc := range cases {
			env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
			for k, v := range tc.env {
				env[k] = v
			}
			_, err := LoadFrom(envMap(env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("%s: want error mentioning %s, got %v", tc.name, tc.want, err)
			}
		}
	})
	t.Run("budget-boundary-passes", func(t *testing.T) {
		// 默认预算组合恰好通过：LLM 8s + 2s 预留 = RCA 10s。
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvLLMEndpoint] = "http://gw:8000/v1"
		env[EnvLLMModel] = "m"
		if _, err := LoadFrom(envMap(env)); err != nil {
			t.Fatalf("default budget (8s+2s ≤ 10s) must pass: %v", err)
		}
	})
	t.Run("disabled-skips-structure", func(t *testing.T) {
		// Endpoint 空 = 禁用：即使 timeout ≥ RCA timeout 也不报错——
		// 未配置组不得新增启动阻力（现状逐字节一致）。
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvRCASTimeout] = "2s"
		env[EnvLLMTimeout] = "8s"
		if _, err := LoadFrom(envMap(env)); err != nil {
			t.Fatalf("disabled llm must not trigger budget check: %v", err)
		}
	})
}

// TestValidateCORSOrigin 跨源白名单规则（原 SetCORSOrigin 校验上移）：
// 空=同源放行、"null" 与 scheme://host 合法、"*"/畸形值拒绝。
func TestValidateCORSOrigin(t *testing.T) {
	for _, ok := range []string{"", "null", "https://ops.example.com", "http://localhost:5173"} {
		if err := ValidateCORSOrigin(ok); err != nil {
			t.Errorf("ValidateCORSOrigin(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"*", "ops.example.com", "https:/broken"} {
		if err := ValidateCORSOrigin(bad); err == nil {
			t.Errorf("ValidateCORSOrigin(%q) must fail", bad)
		}
	}
}

// TestSLAAndKPIKeys W10-2/W10-3 新键（OPS_SLA_* 3 + OPS_KPI_WINDOW）：
// 缺失取默认（critical 60min / warning 240min / info 1440min / 窗 168h）、
// 非法值汇入统一聚合报错（非正整数分钟、非正 duration 一律启动失败）。
func TestSLAAndKPIKeys(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		c, err := LoadFrom(envMap(redisEnv("127.0.0.1:6380", "127.0.0.1:6381")))
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if c.SLA.CriticalMinutes != DefaultSLACriticalMinutes ||
			c.SLA.WarningMinutes != DefaultSLAWarningMinutes ||
			c.SLA.InfoMinutes != DefaultSLAInfoMinutes {
			t.Fatalf("sla defaults = %+v, want %d/%d/%d", c.SLA,
				DefaultSLACriticalMinutes, DefaultSLAWarningMinutes, DefaultSLAInfoMinutes)
		}
		if c.KPI.Window != DefaultKPIWindow {
			t.Fatalf("kpi window = %v, want %v", c.KPI.Window, DefaultKPIWindow)
		}
	})
	t.Run("invalid-aggregate", func(t *testing.T) {
		env := redisEnv("127.0.0.1:6380", "127.0.0.1:6381")
		env[EnvSLACriticalMinutes] = "0" // 非正 → 拒（0 不承诺"无 SLA"语义，防手滑）
		env[EnvSLAWarningMinutes] = "-4" // 非正 → 拒
		env[EnvSLAInfoMinutes] = "many"  // 非整数 → 拒
		env[EnvKPIWindow] = "last-week"  // 非法 duration → 拒
		_, err := LoadFrom(envMap(env))
		var agg *Error
		if !errors.As(err, &agg) || len(agg.Errs) != 4 {
			t.Fatalf("want 4 aggregated errors, got %v", err)
		}
		for _, key := range []string{EnvSLACriticalMinutes, EnvSLAWarningMinutes, EnvSLAInfoMinutes, EnvKPIWindow} {
			if !strings.Contains(err.Error(), key) {
				t.Errorf("aggregate must mention %s: %v", key, err)
			}
		}
	})
}
