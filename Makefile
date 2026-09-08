# 若存在 .env 则载入（供 make migrate 的 DB_DSN 等使用）
ifneq (,$(wildcard .env))
include .env
endif

.PHONY: help env-up env-down migrate gen check test build smoke

GOPROXY ?= https://goproxy.cn,direct
export GOPROXY

help:
	@echo "env-up    启动本地环境（TimescaleDB + 双 Redis）"
	@echo "env-down  停止并清理本地环境"
	@echo "migrate   执行数据库迁移（需 golang-migrate）"
	@echo "gen       生成 gRPC 契约代码（需 protoc）"
	@echo "check     模块边界检查 + 自测"
	@echo "test      go test ./..."
	@echo "build     go build ./..."
	@echo "smoke     go-plugin Windows 冒烟"

env-up:
	docker compose up -d

env-down:
	docker compose down -v

migrate:
	migrate -path migrations -database "$${DB_DSN}" up

gen:
	bash scripts/gen.sh

check:
	python scripts/check_module_boundaries.py --selftest
	python scripts/check_module_boundaries.py

test:
	go test ./...

build:
	go build ./...

smoke:
	cd tools/smoke/goplugin && go build -o bin/plugin.exe ./plugin && go run ./host
