# 若存在 .env 则载入（供 make migrate 的 OPS_DB_DSN 等使用）
ifneq (,$(wildcard .env))
include .env
endif

.PHONY: help env-up env-down migrate gen check lint test race build smoke

GOPROXY ?= https://goproxy.cn,direct
export GOPROXY

help:
	@echo "env-up    启动本地环境（TimescaleDB + 双 Redis）"
	@echo "env-down  停止并清理本地环境"
	@echo "migrate   执行数据库迁移（需 golang-migrate）"
	@echo "gen       生成 gRPC 契约代码（需 protoc）"
	@echo "check     模块边界检查 + 自测"
	@echo "lint      gofmt -l（须零输出）+ go vet ./...（与 CI 同口径）"
	@echo "test      go test ./...（带真库用例先 bash scripts/reset_test_pg.sh）"
	@echo "race      go test -race ./...（本机 Windows 无 cgo/gcc 跑不了，CI 兜底覆盖）"
	@echo "build     go build ./..."
	@echo "smoke     go-plugin Windows 冒烟"

env-up:
	docker compose up -d

env-down:
	docker compose down -v

migrate:
	migrate -path migrations -database "$${OPS_DB_DSN}" up

gen:
	bash scripts/gen.sh

check:
	python scripts/check_module_boundaries.py --selftest
	python scripts/check_module_boundaries.py

# lint 与 CI build-and-check 的 gofmt/vet 步骤同口径（第十一轮 P2：本地与 CI
# 门禁不同口径，本轮 P0 恰因本地缺门禁才暴露）。gofmt 有输出即失败。
lint:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "以下文件未通过 gofmt（执行 gofmt -w . 修复）："; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	go vet ./...

test:
	go test ./...

# race：本机 Windows 无 cgo/gcc 跑不了 -race（ci.yml 同注记），由 CI
# build-and-check 的 race 步骤兜底覆盖；带真库用例先 bash scripts/reset_test_pg.sh。
race:
	go test -race ./... -count=1

build:
	go build ./...

smoke:
	cd tools/smoke/goplugin && go build -o bin/plugin.exe ./plugin && go run ./host
