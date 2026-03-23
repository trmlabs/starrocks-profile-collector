# Multi-stage build: compile Go binary, then copy into minimal Alpine image.
# For faster local iteration, use `make docker-build` which builds on host first.
FROM golang:1.24 AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /starrocks-profile-collector .

FROM alpine:3.21

LABEL org.opencontainers.image.source="https://github.com/trmlabs/starrocks-profile-collector"
LABEL org.opencontainers.image.description="StarRocks FE query profile collector"
LABEL org.opencontainers.image.licenses="Apache-2.0"

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /starrocks-profile-collector .

RUN adduser -D -u 1000 appuser
USER appuser

EXPOSE 9091

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:9091/health || exit 1

ENTRYPOINT ["./starrocks-profile-collector"]
