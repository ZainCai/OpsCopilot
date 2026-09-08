# 多阶段构建：编译期与运行期分离，镜像只留二进制
FROM golang:1.22 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/opscopilot ./cmd/opscopilot

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=builder /out/opscopilot /app/opscopilot
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/opscopilot"]
