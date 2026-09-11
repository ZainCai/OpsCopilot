// Package noise 告警降噪基础原语（M1 W4-1.2）：指纹与时间窗去重。
//
// 定位与边界：
//   - 本包是**纯原语库**：输入只有 labels map 与时间，不 import 任何
//     internal 兄弟模块（模块间禁止互 import，v1.3 §5.2）。Alert → noise
//     的字段翻译发生在 cmd/ 装配层。
//   - 这只是降噪流水线的第一级（去重）。聚类、抑制、影子评估是后续
//     W4-1.3+ 的事，不要把"去重生效"误读为"降噪完成"。
//
// 指纹语义（ADR-001 配套）：
//   - 指纹 = 对 labels 规范化序列化后的 FNV-1a 64 位哈希（16 位小写 hex）。
//     规范化 = 按 key 字典序排列的 "k=v" 列表以 "\x1f" 连接——map 遍历序
//     不稳定，直接序列化会让同一告警在两轮采集里得到不同指纹。
//   - 指纹覆盖**全部**传入 labels。调用方若认为某些标签易变（如时间戳
//     类标签），应在传入前剔除——本包不做黑名单，黑名单是策略，
//     原语只保证"同样的输入永远得到同样的指纹"。
//
// 去重语义（固定窗口，锚点为首次出现）：
//   - 同一指纹在 window 内再次出现 → 抑制（false）；窗口过后重新放行，
//     并以放行时刻为新锚点。不用滑动窗口：30s 轮询下滑动窗口会把
//     持续 firing 的告警永久静音，固定窗口保证周期性重放。
//
// 失败取向（降噪宁漏勿杀）：去重是优化不是闸门。空指纹（调用方 bug
// 或上游未提供）一律放行——宁可多通知，不可静默吞告警。
package noise

import (
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"opscopilot/pkg/memguard"
)

// unitSep 规范化序列化的字段分隔符（ASCII unit separator）。
// 选控制字符而非 ','：label 值里出现逗号很常见，出现 \x1f 几乎不可能。
const unitSep = "\x1f"

// CanonicalLabels 把 labels 规范化为稳定字符串：按 key 字典序的
// "k=v" 列表，以 \x1f 连接。nil/空 map 返回空串（指纹退化为空串哈希，
// 仍是确定性的）。导出仅供调试与测试比对，业务方用 Fingerprint 即可。
func CanonicalLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteString(unitSep)
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return b.String()
}

// Fingerprint 计算 labels 的规范化指纹（FNV-1a 64，16 位小写 hex）。
// 确定性契约：同一 labels 集合（任意插入顺序）永远返回同一指纹；
// 不同集合（含"缺 key"与"key 存在但值为空"）返回不同指纹。
func Fingerprint(labels map[string]string) string {
	const hexDigits = "0123456789abcdef"
	h := fnv.New64a()
	_, _ = h.Write([]byte(CanonicalLabels(labels)))
	return formatHex(h.Sum64(), hexDigits)
}

// formatHex 把 u 格式化为 16 位小写 hex（避免引入 fmt 分配）。
func formatHex(u uint64, digits string) string {
	var buf [16]byte
	for i := 15; i >= 0; i-- {
		buf[i] = digits[u&0xf]
		u >>= 4
	}
	return string(buf[:])
}

// Dedup 固定窗口去重器（并发安全）。
//
// 内存契约：每个活跃指纹一条记录。窗口过后的记录是死重量，靠
// Sweep 或 Allow 内的周期清扫回收；长时间运行且指纹基数持续增长的
// 场景依赖清扫生效（见 sweepEveryN 的取舍说明）。
//
// 内存有界化（优化方案 #6）：清扫是"按时间回收"，挡不住**窗口内**的
// 唯一指纹基数洪峰（每条都新、永远清不掉）。装配护栏后，超上限即淘汰
// **最久未活跃**（锚点最早）的指纹——被逐条恰是"最不可能再来"的，代价
// 是该指纹再出现会被放行一次，与"降噪宁漏勿杀"同向（fail-open）。
type Dedup struct {
	mu     sync.Mutex
	window time.Duration
	// seen 指纹 → 窗口锚点（首次放行时刻）。
	seen map[string]time.Time
	// calls 自 Allow 调用计数，用于触发周期清扫。
	calls uint64
	guard *memguard.Guard
}

// sweepEveryN 每 N 次 Allow 调用做一次惰性清扫（过期记录 O(n) 回收）。
// 不逐调用清扫：百台规模一轮数百条告警，逐条 O(n) 是无谓的平方开销；
// 每 1024 次清一次，均摊接近 O(1)，且单轮内窗口远未到期，清扫无收益。
const sweepEveryN = 1024

// NewDedup 构造去重器（无容量上限）。window <= 0 视为"不去重"（全部放行）——
// 与空指纹放行同一取向：降噪宁漏勿杀。
func NewDedup(window time.Duration) *Dedup {
	return NewDedupWithLimits(window, nil)
}

// NewDedupWithLimits 构造并装配容量护栏（guard nil = 不设限）。
func NewDedupWithLimits(window time.Duration, guard *memguard.Guard) *Dedup {
	d := &Dedup{window: window, seen: make(map[string]time.Time), guard: guard}
	guard.SetSize(d.Len)
	return d
}

// MemGuards 返回装配的护栏（供装配层注册进指标注册表）。
func (d *Dedup) MemGuards() []*memguard.Guard {
	if d.guard == nil {
		return nil
	}
	return []*memguard.Guard{d.guard}
}

// Allow 判定指纹 fp 在时刻 now 是否放行。
//
// 返回 true：窗口外首次出现（记录锚点）或窗口已过（锚点重置为 now）。
// 返回 false：窗口内重复出现。
// 空指纹一律放行（不记录）——见包注释"失败取向"。
func (d *Dedup) Allow(fp string, now time.Time) bool {
	if d.window <= 0 || fp == "" {
		return true
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	d.calls++
	if d.calls%sweepEveryN == 0 {
		d.sweepLocked(now)
	}

	anchor, ok := d.seen[fp]
	if ok && now.Before(anchor.Add(d.window)) {
		return false // 窗口内重复
	}
	// 首次出现或窗口已过：放行并重置锚点。
	d.seen[fp] = now
	d.enforceLocked()
	return true
}

// enforceLocked 超限淘汰（必须持 d.mu）：按锚点最早（=最久未活跃）逐条
// 回收至上限内，条数计入护栏。空 guard（无界）零成本直返。
func (d *Dedup) enforceLocked() {
	if d.guard == nil {
		return
	}
	excess := d.guard.Over(len(d.seen))
	if excess <= 0 {
		return
	}
	type fa struct {
		fp string
		at time.Time
	}
	all := make([]fa, 0, len(d.seen))
	for fp, at := range d.seen {
		all = append(all, fa{fp, at})
	}
	// 决定性：锚点最早优先，同刻按指纹字典序。
	sort.Slice(all, func(i, j int) bool {
		if !all[i].at.Equal(all[j].at) {
			return all[i].at.Before(all[j].at)
		}
		return all[i].fp < all[j].fp
	})
	if excess > len(all) {
		excess = len(all)
	}
	for _, e := range all[:excess] {
		delete(d.seen, e.fp)
	}
	d.guard.Evicted(excess)
}

// Sweep 清扫全部过期记录，返回回收条数。供运维排查与测试直接驱动；
// 运行期由 Allow 周期触发。
func (d *Dedup) Sweep(now time.Time) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sweepLocked(now)
}

func (d *Dedup) sweepLocked(now time.Time) int {
	n := 0
	for fp, anchor := range d.seen {
		if !now.Before(anchor.Add(d.window)) {
			delete(d.seen, fp)
			n++
		}
	}
	return n
}

// Len 当前记录数（含窗口内全部指纹）——容量观察点。
func (d *Dedup) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
