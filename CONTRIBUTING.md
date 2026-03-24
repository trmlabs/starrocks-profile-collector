# Contributing

We welcome contributions to the StarRocks Profile Collector! This document provides guidelines for contributing.

## Getting Started

### Prerequisites

- Go 1.24+
- Docker (for building container images)
- A StarRocks cluster (for integration testing)

### Build

```bash
# Build for current platform
make build

# Run locally
export CLUSTER_NAME=dev
export FE_HOSTS=localhost:8030
export STORAGE_BACKEND=stdout
make run
```

### Test

```bash
# Unit tests
make test-unit

# All tests with race detection
make test

# Coverage report
make test-coverage
```

### Lint

```bash
# Requires golangci-lint: https://golangci-lint.run/usage/install/
make lint

# Format
make fmt
```

## Making Changes

1. Fork the repository
2. Create a feature branch: `git checkout -b my-feature`
3. Make your changes
4. Run tests: `make test`
5. Run lint: `make lint`
6. Commit with a clear message
7. Push and open a pull request

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- Add tests for new functionality
- Use `log/slog` for structured logging
- Keep the `Writer` interface minimal; new storage backends should implement it

## Adding a Storage Backend

1. Create a new file: `mybackend_writer.go`
2. Implement the `Writer` interface (see `writer.go`)
3. Add configuration fields to `Config` in `config.go`
4. Add a case to `newWriter()` in `main.go`
5. Add tests
6. Update `README.md` with the new backend's configuration

## Reporting Issues

Please open a GitHub issue with:

- What you expected to happen
- What actually happened
- Steps to reproduce
- StarRocks version and deployment environment

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
