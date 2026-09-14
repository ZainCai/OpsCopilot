// session_mem.go 会话真相层内存实现（原 rca_session.go 拆分，2026-09-14）。
// 无 DB 降级：响亮 WARNING、单实例、重启即丢（升级台账同款纪律）。
package main

import (
	"context"
	"sync"
	"time"

	"opscopilot/internal/sessionstore"
)

// ---------- 真相层内存实现（无 DB 降级：升级台账同款纪律） ----------

type memSessionTruth struct {
	mu   sync.Mutex
	sess map[string]*memSession
}

type memSession struct {
	createdBy    string
	participants []string
	nextSeq      int64
	turns        []SessionTurn
}

func newMemSessionTruth() *memSessionTruth {
	return &memSessionTruth{sess: map[string]*memSession{}}
}

// persistence 恒 "memory"——无 DB 降级形态必须让 REST 消费方看见（R6-4 口径）。
func (m *memSessionTruth) persistence() string { return "memory" }

func (m *memSessionTruth) appendTurn(_ context.Context, incidentID, actor, role, content string,
	llmMeta map[string]any) (SessionTurn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sess[incidentID]
	if s == nil {
		s = &memSession{nextSeq: 1}
		if actor != "" {
			s.createdBy = actor
		}
		m.sess[incidentID] = s
	}
	if actor != "" && role == sessionstore.RoleUser && !containsStr(s.participants, actor) {
		s.participants = append(s.participants, actor)
	}
	t := SessionTurn{Seq: s.nextSeq, Role: role, Content: content, CreatedBy: actor,
		CreatedAt: time.Now().UTC(), LLMMeta: llmMeta}
	s.nextSeq++
	s.turns = append(s.turns, t)
	return t, nil
}

func (m *memSessionTruth) snapshot(_ context.Context, incidentID string) ([]SessionTurn, SessionMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sess[incidentID]
	if s == nil {
		return nil, SessionMeta{}, nil
	}
	out := make([]SessionTurn, len(s.turns))
	copy(out, s.turns)
	return out, SessionMeta{CreatedBy: s.createdBy, Participants: append([]string{}, s.participants...)}, nil
}

func (m *memSessionTruth) meta(_ context.Context, incidentID string) (SessionMeta, error) {
	_, meta, err := m.snapshot(context.Background(), incidentID)
	return meta, err
}

func containsStr(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
