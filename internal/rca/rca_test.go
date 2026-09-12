// M2 主干骨架测试：流水线框架（占位步骤 pending、顺序、缺输入拒绝、根因挑拣）。
package rca

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeStep 可控步骤（测试用）。
type fakeStep struct {
	name string
	fs   []Finding
	err  error
}

func (f *fakeStep) Name() string { return f.name }
func (f *fakeStep) Run(in Input, prev []Finding) ([]Finding, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.fs, nil
}

func TestPipelineRejectsZeroT0(t *testing.T) {
	p := NewPipeline(&fakeStep{name: "collect", err: ErrNotImplemented})
	if _, err := p.Run(Input{}); !errors.Is(err, ErrNoInput) {
		t.Fatalf("zero T0: err = %v, want ErrNoInput", err)
	}
}

func TestPipelinePlaceholderStepsRunThrough(t *testing.T) {
	// 全占位流水线：跑通不 panic，步骤全 pending，产出空。
	p := NewPipeline(
		&fakeStep{name: "collect", err: ErrNotImplemented},
		&fakeStep{name: "hypothesize", err: ErrNotImplemented},
	)
	rep, err := p.Run(Input{TenantID: "default", ClusterKey: "c:fp@1", T0: time.Now()})
	if err != nil {
		t.Fatalf("placeholder pipeline: %v", err)
	}
	if len(rep.Steps) != 2 {
		t.Fatalf("steps = %d, want 2", len(rep.Steps))
	}
	for _, sr := range rep.Steps {
		if sr.Status != StatusPending || !strings.Contains(sr.Err, "placeholder") {
			t.Fatalf("step %s: %s / %s, want pending/placeholder", sr.Name, sr.Status, sr.Err)
		}
	}
	if len(rep.Findings) != 0 {
		t.Fatalf("findings = %d, want 0", len(rep.Findings))
	}
}

func TestPipelineFailureAborts(t *testing.T) {
	p := NewPipeline(
		&fakeStep{name: "collect", err: ErrNotImplemented},
		&fakeStep{name: "hypothesize", err: errors.New("boom")},
		&fakeStep{name: "verify", err: ErrNotImplemented},
	)
	rep, err := p.Run(Input{T0: time.Now()})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expect abort on real failure, got err=%v", err)
	}
	if len(rep.Steps) != 2 {
		t.Fatalf("steps executed = %d, want 2 (aborted before verify)", len(rep.Steps))
	}
}

func TestRootCausePicking(t *testing.T) {
	p := NewPipeline(&fakeStep{name: "attribution", fs: []Finding{
		{Step: "attribution", Summary: "磁盘满导致", NodeKeys: []string{"n1"}, Confidence: "high"},
		{Step: "attribution", Summary: "网络抖动", NodeKeys: []string{"n2"}, Confidence: "low"},
	}})
	rep, err := p.Run(Input{T0: time.Now()})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(rep.RootCauses) != 1 || rep.RootCauses[0].NodeKeys[0] != "n1" {
		t.Fatalf("root causes = %+v, want only high-confidence n1", rep.RootCauses)
	}
}

// TestTruncateFindings 二期池波二 #6 截断策略单测：置信度优先保留、同档
// 保时序（原步序）、保留集按步骤序回排、未触限/非正上限零行为差异。
func TestTruncateFindings(t *testing.T) {
	f := func(step, conf, ref string) Finding {
		return Finding{Step: step, Summary: step + ":" + ref, Confidence: conf, Ref: ref}
	}
	cases := []struct {
		name     string
		in       []Finding
		max      int
		wantKeep []string // 期望保留集（按输出序 = 原步骤序的 Ref）
		wantDrop int
	}{
		{
			name: "未超上限原样返回",
			in:   []Finding{f("collect", "high", "a"), f("verify", "low", "b")},
			max:  5, wantKeep: []string{"a", "b"}, wantDrop: 0,
		},
		{
			name: "上限非正视同不限",
			in:   []Finding{f("collect", "high", "a"), f("verify", "low", "b")},
			max:  0, wantKeep: []string{"a", "b"}, wantDrop: 0,
		},
		{
			name: "置信度优先：low 先出局，保留集仍按步骤序",
			in: []Finding{
				f("collect", "low", "c1"), f("hypothesize", "high", "h1"),
				f("verify", "low", "v1"), f("attribution", "medium", "m1"),
			},
			max: 2, wantKeep: []string{"h1", "m1"}, wantDrop: 2,
		},
		{
			name: "同档保时序：先产出者优先（步骤序）",
			in: []Finding{
				f("hypothesize", "high", "first"), f("hypothesize", "high", "second"),
				f("hypothesize", "high", "third"),
			},
			max: 2, wantKeep: []string{"first", "second"}, wantDrop: 1,
		},
		{
			name: "空/未知置信按 low 保守出局",
			in: []Finding{
				f("collect", "", "blank"), f("verify", "unknown", "weird"),
				f("attribution", "medium", "keepme"),
			},
			max: 1, wantKeep: []string{"keepme"}, wantDrop: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kept, dropped := TruncateFindings(tc.in, tc.max)
			if dropped != tc.wantDrop || len(kept) != len(tc.wantKeep) {
				t.Fatalf("kept=%d dropped=%d, want kept=%d dropped=%d", len(kept), dropped, len(tc.wantKeep), tc.wantDrop)
			}
			for i := range kept {
				if kept[i].Ref != tc.wantKeep[i] {
					t.Fatalf("kept[%s] order wrong, want %v got %+v", t.Name(), tc.wantKeep, kept)
				}
			}
		})
	}
}
