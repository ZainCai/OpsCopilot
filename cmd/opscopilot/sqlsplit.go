// sqlsplit.go SQL 语句切分（原 version.go 拆分，2026-09-14）。
// SYNC：splitStatements / dollarTag 与 scripts/migrate/main.go 逐字一致，
// 改动任一份必须同步另一份（两侧文件头均有本注记）。
package main

import "strings"

// ---------- 语句切分 ----------
//
// SYNC：splitStatements / dollarTag 逐字复制自 scripts/migrate/main.go——
// upgrade 与建库工具必须按同一规则切语句，否则"migrate 能建、upgrade 不能补"
// 会成最难查的一对孪生 bug。scripts/migrate 是 package main（不可 import），
// 故以复制换一致；改动任何一份都要同步另一份（两边文件头都有本注记）。

// splitStatements 按顶层分号切分 SQL，正确处理：单引号/双引号字符串、
// -- 行注释、/* */ 块注释、$tag$ 美元引号（000003 的 DO $do$ 块内有分号）。
func splitStatements(sql string) []string {
	var out []string
	var cur strings.Builder
	n := len(sql)
	i := 0
	for i < n {
		c := sql[i]
		switch c {
		case '\'', '"':
			cur.WriteByte(c)
			i++
			for i < n {
				if sql[i] == c {
					if i+1 < n && sql[i+1] == c { // 连续引号转义（'' / ""）
						cur.WriteString(sql[i : i+2])
						i += 2
						continue
					}
					cur.WriteByte(c)
					i++
					break
				}
				if c == '\'' && sql[i] == '\\' && i+1 < n && (sql[i+1] == '\'' || sql[i+1] == '\\') {
					// standard_conforming_strings=on 时反斜杠不是转义符，但保守跳过。
					cur.WriteByte(sql[i])
					cur.WriteByte(sql[i+1])
					i += 2
					continue
				}
				cur.WriteByte(sql[i])
				i++
			}
		case '-':
			if i+1 < n && sql[i+1] == '-' {
				for i < n && sql[i] != '\n' {
					cur.WriteByte(sql[i])
					i++
				}
			} else {
				cur.WriteByte(c)
				i++
			}
		case '/':
			if i+1 < n && sql[i+1] == '*' {
				end := strings.Index(sql[i+2:], "*/")
				if end < 0 {
					cur.WriteString(sql[i:])
					i = n
				} else {
					cur.WriteString(sql[i : i+2+end+2])
					i += 2 + end + 2
				}
			} else {
				cur.WriteByte(c)
				i++
			}
		case '$':
			if tag, ok := dollarTag(sql[i:]); ok {
				end := strings.Index(sql[i+len(tag):], tag)
				if end < 0 {
					cur.WriteString(sql[i:])
					i = n
				} else {
					cur.WriteString(sql[i : i+len(tag)+end+len(tag)])
					i += len(tag)*2 + end
				}
			} else {
				cur.WriteByte(c)
				i++
			}
		case ';':
			if s := strings.TrimSpace(cur.String()); s != "" {
				out = append(out, s)
			}
			cur.Reset()
			i++
		default:
			cur.WriteByte(c)
			i++
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}

// dollarTag 识别 $tag$ / $$ 开标记（PG 规则：可跟字母下划线，不能以数字开头）。
func dollarTag(s string) (string, bool) {
	j := 1
	for j < len(s) && s[j] != '$' {
		c := s[j]
		ok := c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(j > 1 && c >= '0' && c <= '9') // 首位不允许数字
		if !ok {
			return "", false
		}
		j++
	}
	if j >= len(s) || s[j] != '$' {
		return "", false
	}
	if j > 1 && s[1] >= '0' && s[1] <= '9' {
		return "", false
	}
	return s[:j+1], true
}
