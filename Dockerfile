# ============================================================
# KamaChat 服务器镜像 - 单机 2C4G 容器压测环境
# 多阶段构建：build 阶段编译，runtime 阶段仅保留二进制
# ============================================================

# ---- 构建阶段 ----
FROM golang:1.25-alpine AS build

WORKDIR /src

# 使用 vendor 目录实现离线构建（容器内不依赖 proxy.golang.org 外网）
COPY go.mod go.sum ./
COPY vendor ./vendor

# 拷贝源码并编译
COPY . .
# CGO_ENABLED=0 静态编译，禁用 os/user 依赖，适配 alpine 精简运行时
# -mod=vendor 强制使用本地 vendor，避免构建时访问网络
RUN CGO_ENABLED=0 GOOS=linux go build -mod=vendor -ldflags="-s -w" -o /out/kama_chat_server ./cmd/kama_chat_server

# ---- 运行阶段 ----
FROM alpine:3.20

WORKDIR /app

# 时区：Go 已内嵌 time/tzdata（见 main.go），无需系统 zoneinfo 文件
ENV TZ=Asia/Shanghai

# 拷贝二进制、配置、证书、静态资源
COPY --from=build /out/kama_chat_server /app/kama_chat_server
COPY configs/config.toml /app/configs/config.toml
COPY pkg/ssl/server.crt /app/pkg/ssl/server.crt
COPY pkg/ssl/server.key /app/pkg/ssl/server.key

# 静态资源目录（头像/文件）
RUN mkdir -p /app/static/avatars /app/static/files /app/logs

# 业务端口 + pprof 端口
EXPOSE 8000 8091

# 容器限 2C4G 由 docker-compose 的 deploy.resources 控制
CMD ["/app/kama_chat_server"]
