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
PKG_DIR="$OUT_DIR/pb"

mkdir -p "$PKG_DIR"

# Windows + 中文路径兜底：protoc 是原生 Windows 程序，无法打开含中文的路径
# （Git Bash 传参 / PowerShell Unicode 传参均无效，8.3 短名对 CJK 仍保留汉字）。
# 故将 proto 拷到全 ASCII 临时目录生成，再把 .pb.go 拷回仓库。
case "$(uname -s 2>/dev/null)" in
  MINGW*|MSYS*|CYGWIN*)
    if printf '%s' "$PROTO_DIR" | grep -qP '[^\x00-\x7F]'; then
      TMPBASE="C:/tmp"
      mkdir -p "$TMPBASE"
      TMP="$(mktemp -d "$TMPBASE/protoc.XXXXXX")/proto"
      mkdir -p "$TMP"
      cp "$PROTO_DIR"/*.proto "$TMP/"
      protoc \
        --proto_path="$TMP" \
        --go_out="$(dirname "$TMP")" --go_opt=paths=source_relative \
        --go-grpc_out="$(dirname "$TMP")" --go-grpc_opt=paths=source_relative \
        "$TMP"/*.proto
      cp "$(dirname "$TMP")"/*.pb.go "$PKG_DIR/"
      rm -rf "$(dirname "$TMP")"
      echo "generated -> $PKG_DIR (via ASCII-temp fallback for CJK path)"
      exit 0
    fi
    ;;
esac

# X3：--go_out 必须指向 $PKG_DIR（internal/contracts/pb/）。原指向 $OUT_DIR
# 会在 contracts/ 根生成 topology.pb.go（package pb 位于错误目录），
# 与仓库实际布局及 Windows 兜底分支（生成到临时目录再拷入 $PKG_DIR）不一致。
protoc \
  --proto_path="$PROTO_DIR" \
  --go_out="$PKG_DIR" --go_opt=paths=source_relative \
  --go-grpc_out="$PKG_DIR" --go-grpc_opt=paths=source_relative \
  "$PROTO_DIR"/*.proto

echo "generated -> $PKG_DIR"
