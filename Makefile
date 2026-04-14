.PHONY: build test lint clean run

# 变量
BINARY_NAME=vlog-tools
IMAGE_REPO?=vlog-tools
PLATFORMS?=linux/amd64,linux/arm64
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo ${COMMIT})
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildTime=${BUILD_TIME}"

# 构建
build:
	go build ${LDFLAGS} -o bin/${BINARY_NAME} ./cmd/vlog-tools

build-all:
	GOOS=linux GOARCH=amd64 go build ${LDFLAGS} -o bin/${BINARY_NAME}-linux-amd64 ./cmd/vlog-tools
	GOOS=darwin GOARCH=amd64 go build ${LDFLAGS} -o bin/${BINARY_NAME}-darwin-amd64 ./cmd/vlog-tools

# 测试
test:
	go test -v -race -cover ./...

test-integration:
	go test -v -tags=integration ./...

# 代码检查
lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

# 运行
run:
	go run ./cmd/vlog-tools

# Docker
docker-build:
	docker build -t vlog-tools:latest -f deployments/docker/Dockerfile.tools .

docker-buildx:
	docker buildx build \
		--platform ${PLATFORMS} \
		-f deployments/docker/Dockerfile.tools \
		--build-arg VERSION=${VERSION} \
		--build-arg COMMIT=${COMMIT} \
		--build-arg BUILD_TIME=${BUILD_TIME} \
		-t ${IMAGE_REPO}:${VERSION} \
		--load .

docker-buildx-push:
	docker buildx build \
		--platform ${PLATFORMS} \
		-f deployments/docker/Dockerfile.tools \
		--build-arg VERSION=${VERSION} \
		--build-arg COMMIT=${COMMIT} \
		--build-arg BUILD_TIME=${BUILD_TIME} \
		-t ${IMAGE_REPO}:${VERSION} \
		--push .

# 清理
clean:
	rm -rf bin/

# 依赖
deps:
	go mod download
	go mod tidy
