// tools/smoke 独立子模块（全局审查 C7）：
//
// go-plugin 全家桶（go-plugin/go-hclog/yamux/xxhash/oklog-run/…）此前挂在
// 顶层 go.mod，却只被本目录的冒烟代码引用——生产模块被迫携带整套工具链依赖。
// 本模块与顶层 opscopilot 隔离：生产 go.mod 只留 runtime 依赖
// （grpc/protobuf/redis），冒烟想用什么重型库都加在这里，不污染主模块。
//
// 构建/运行（README 与 CI 均在此目录或其子目录执行）：
//
//	cd tools/smoke/goplugin && go build -o bin/plugin.exe ./plugin && go run ./host
module opscopilot/tools/smoke

go 1.23

require (
	github.com/hashicorp/go-plugin v1.6.1
	google.golang.org/grpc v1.66.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/fatih/color v1.7.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/hashicorp/go-hclog v0.14.1 // indirect
	github.com/hashicorp/yamux v0.1.1 // indirect
	github.com/mattn/go-colorable v0.1.4 // indirect
	github.com/mattn/go-isatty v0.0.10 // indirect
	github.com/mitchellh/go-testing-interface v0.0.0-20171004221916-a61a99592b77 // indirect
	github.com/oklog/run v1.0.0 // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240604185151-ef581f913117 // indirect
)
