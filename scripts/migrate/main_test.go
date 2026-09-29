// main_test.go 纯逻辑单测（不连库）：splitStatements 语句切分三边界
// （美元引号 / 分号与字符串 / 注释）+ planRun 参数校验（--create-db/--wipe 组合）。
// 第十一轮 P2「scripts/migrate 零测试」补位；splitStatements 与
// cmd/opscopilot/sqlsplit.go 是刻意保留的字节一致双份（SYNC 注记见彼处），
// 本测试只锁 scripts/migrate 这份的行为。
package main

import (
	"strings"
	"testing"
)

func TestSplitStatementsDollarQuoted(t *testing.T) {
	// 000003 实况形态：DO $do$ 块内部含分号，绝不能被切开；块后语句正常切分。
	sql := "CREATE EXTENSION IF NOT EXISTS timescaledb;\n" +
		"DO $do$\nBEGIN\n  IF NOT EXISTS (SELECT 1) THEN\n    RAISE NOTICE 'x; y';\n  END IF;\nEND\n$do$;\n" +
		"SELECT 1;"
	got := splitStatements(sql)
	if len(got) != 3 {
		t.Fatalf("splitStatements 得到 %d 条语句，want 3：%q", len(got), got)
	}
	if !strings.HasPrefix(got[1], "DO $do$") || !strings.HasSuffix(got[1], "$do$") {
		t.Fatalf("美元引号块被切坏：%q", got[1])
	}
	if !strings.Contains(got[1], "RAISE NOTICE 'x; y';") {
		t.Fatalf("块内分号丢失（说明按分号误切）：%q", got[1])
	}
	if got[2] != "SELECT 1" {
		t.Fatalf("块后语句 = %q, want SELECT 1", got[2])
	}
}

func TestSplitStatementsSemicolonAndString(t *testing.T) {
	// 顶层分号切分；单引号字符串内的分号不切；连续/尾部分号不产生空语句。
	sql := "INSERT INTO t (a) VALUES ('x;y'); SELECT 'a''b;z'; ;"
	got := splitStatements(sql)
	if len(got) != 2 {
		t.Fatalf("splitStatements 得到 %d 条语句，want 2：%q", len(got), got)
	}
	if got[0] != "INSERT INTO t (a) VALUES ('x;y')" {
		t.Fatalf("字符串内分号被误切：%q", got[0])
	}
	if got[1] != "SELECT 'a''b;z'" {
		t.Fatalf("连续引号转义（''）处理错误：%q", got[1])
	}
}

func TestSplitStatementsCommentBoundary(t *testing.T) {
	// 行注释与块注释里的分号不切；注释本体保留在所属语句里。
	sql := "-- 头注释; 带分号\nSELECT 1; /* 块; 注释 */ SELECT 2; -- 尾注释; 不闭合"
	got := splitStatements(sql)
	if len(got) != 3 {
		t.Fatalf("splitStatements 得到 %d 条语句，want 3：%q", len(got), got)
	}
	if !strings.Contains(got[0], "-- 头注释; 带分号") || !strings.Contains(got[0], "SELECT 1") {
		t.Fatalf("行注释归属错误：%q", got[0])
	}
	if !strings.Contains(got[1], "/* 块; 注释 */") || !strings.Contains(got[1], "SELECT 2") {
		t.Fatalf("块注释归属错误：%q", got[1])
	}
	if !strings.Contains(got[2], "尾注释") {
		t.Fatalf("尾注释丢失：%q", got[2])
	}
}

func TestPlanRunFlagCombos(t *testing.T) {
	cases := []struct {
		name           string
		dsn            string
		createDB, wipe bool
		wantEnsure     bool
		wantDropFirst  bool
		wantErr        bool
	}{
		{name: "缺 dsn 报错", dsn: "", createDB: true, wipe: true, wantErr: true},
		{name: "裸跑：不建库不删库", dsn: "postgres://x/y", wantEnsure: false, wantDropFirst: false},
		{name: "-create-db：建库不删库", dsn: "postgres://x/y", createDB: true, wantEnsure: true, wantDropFirst: false},
		{name: "-wipe 隐含 -create-db：先删后建", dsn: "postgres://x/y", wipe: true, wantEnsure: true, wantDropFirst: true},
		{name: "两旗同给：等价 -wipe", dsn: "postgres://x/y", createDB: true, wipe: true, wantEnsure: true, wantDropFirst: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ensure, dropFirst, err := planRun(c.dsn, c.createDB, c.wipe)
			if c.wantErr {
				if err == nil {
					t.Fatalf("planRun(%q,%v,%v) 期望报错，实得 nil", c.dsn, c.createDB, c.wipe)
				}
				return
			}
			if err != nil {
				t.Fatalf("planRun(%q,%v,%v) 意外报错：%v", c.dsn, c.createDB, c.wipe, err)
			}
			if ensure != c.wantEnsure || dropFirst != c.wantDropFirst {
				t.Fatalf("planRun(%q,%v,%v) = (%v,%v), want (%v,%v)",
					c.dsn, c.createDB, c.wipe, ensure, dropFirst, c.wantEnsure, c.wantDropFirst)
			}
		})
	}
}
