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
