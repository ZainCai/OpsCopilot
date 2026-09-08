#!/usr/bin/env bash
# 生成跨模块 gRPC 契约代码（O13）
#
# 前置（一次性）：
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#   # protoc 本体：https://github.com/protocolbuffers/protobuf/releases
#   #   或用包管理器：winget install -e --id Google.Protobuf / apt install protobuf-compiler
#   export PATH="$PATH:$(go env GOPATH)/bin"
#
# 用法：bash scripts/gen.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="$ROOT/internal/contracts/proto"
OUT_DIR="$ROOT/internal/contracts"

mkdir -p "$OUT_DIR/pb"

protoc \
  --proto_path="$PROTO_DIR" \
  --go_out="$OUT_DIR" --go_opt=paths=source_relative \
  --go-grpc_out="$OUT_DIR" --go-grpc_opt=paths=source_relative \
  "$PROTO_DIR"/*.proto

echo "generated -> $OUT_DIR/pb"
