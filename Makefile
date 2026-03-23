.PHONY: build build-linux test test-unit run clean docker-build docker-push

# Variables
BINARY_NAME=starrocks-profile-collector
IMAGE_NAME=ghcr.io/trmlabs/starrocks-profile-collector
VERSION?=latest

# Linker flags: strip debug info and symbol table for smaller binaries.
LDFLAGS=-ldflags="-s -w"

# Build for current platform
build:
	go build $(LDFLAGS) -o $(BINARY_NAME) .

# Build for Linux amd64 (required for Docker)
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo $(LDFLAGS) -o $(BINARY_NAME) .

# Build for Linux ARM64
build-linux-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -a -installsuffix cgo $(LDFLAGS) -o $(BINARY_NAME) .

# Run unit tests
test-unit:
	@go test -v -race -short ./... 2>&1 | tee /dev/stderr | awk '/^--- PASS/{pass++} /^--- FAIL/{fail++} END{printf "\n========================================\n  SUMMARY: %d passed, %d failed\n========================================\n", pass+0, fail+0; if(fail>0) exit 1}'

# Run all tests
test:
	@go test -v -race ./... 2>&1 | tee /dev/stderr | awk '/^--- PASS/{pass++} /^--- FAIL/{fail++} END{printf "\n========================================\n  SUMMARY: %d passed, %d failed\n========================================\n", pass+0, fail+0; if(fail>0) exit 1}'

# Run benchmarks
bench:
	go test -bench=. -benchmem ./...

# Run with coverage
test-coverage:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run locally (requires env vars)
run:
	go run .

# Docker build (multi-stage, no local Go toolchain needed)
docker-build:
	docker build -t $(IMAGE_NAME):$(VERSION) .

# Docker build using pre-built host binary (faster for local iteration)
docker-build-fast: build-linux
	docker build -f Dockerfile.host-build -t $(IMAGE_NAME):$(VERSION) .

# Docker build multi-arch (for releases)
docker-build-multiarch:
	docker buildx build --platform linux/amd64,linux/arm64 -t $(IMAGE_NAME):$(VERSION) .

# Docker push
docker-push:
	docker push $(IMAGE_NAME):$(VERSION)

# Docker build multi-arch and push (for releases)
docker-release:
	docker buildx build --platform linux/amd64,linux/arm64 -t $(IMAGE_NAME):$(VERSION) --push .

# Clean
clean:
	rm -f $(BINARY_NAME)
	rm -f coverage.out coverage.html

# Lint
lint:
	golangci-lint run

# Format
fmt:
	go fmt ./...

# Download dependencies
deps:
	go mod download
	go mod tidy

# Help
help:
	@echo "Available targets:"
	@echo "  build                - Build binary for current platform"
	@echo "  build-linux          - Build binary for Linux amd64"
	@echo "  build-linux-arm64    - Build binary for Linux arm64"
	@echo "  test-unit            - Run unit tests"
	@echo "  test                 - Run all tests"
	@echo "  bench                - Run benchmarks"
	@echo "  test-coverage        - Run tests with coverage report"
	@echo "  docker-build         - Build Docker image (multi-stage)"
	@echo "  docker-build-fast    - Build Docker image using host-compiled binary (faster)"
	@echo "  docker-build-multiarch - Build multi-arch Docker image (amd64 + arm64)"
	@echo "  docker-release       - Build and push multi-arch Docker image"
	@echo "  clean                - Clean build artifacts"
	@echo "  lint                 - Run golangci-lint"
	@echo "  fmt                  - Format Go code"
	@echo "  deps                 - Download Go dependencies"
