# 多阶段构建：编译期与运行期分离，镜像只留二进制
# builder 版本必须 >= go.mod 声明的 go 版本（X1：原 1.22 在 go mod download 即失败）
FROM golang:1.23 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/opscopilot ./cmd/opscopilot

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=builder /out/opscopilot /app/opscopilot
# 容器内进程默认绑 127.0.0.1:8080（S1 安全默认），端口映射必须配合
# OPS_LISTEN_ADDR=0.0.0.0:8080（见 docker-compose 的 app 服务）才能从宿主可达——
# 容器隔离本身是安全边界，容器内绑全接口是标准做法。
# 注意：distroless 无 shell/curl，无法在镜像层做 HEALTHCHECK；
# 探活走 compose 层或 K8s 探针（应用已提供 GET /healthz）。
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/opscopilot"]
