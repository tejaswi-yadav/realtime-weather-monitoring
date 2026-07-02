# Multi-stage build for minimal image size
# Stage 1: Build stage
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /build

# Copy go mod files first (for caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY *.go ./

# Build all binaries with optimizations
# -ldflags="-s -w" strips debug info and reduces binary size
# CGO_ENABLED=0 creates static binaries
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o weather-app main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o weather-consumer consumer.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o weather-evaluator evaluator.go

# Stage 2: Runtime stage (minimal)
FROM alpine:3.19

# Install minimal runtime dependencies
RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 1000 weather && \
    adduser -D -u 1000 -G weather weather

# Copy timezone data and certificates from builder
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Set working directory
WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/weather-app /app/
COPY --from=builder /build/weather-consumer /app/
COPY --from=builder /build/weather-evaluator /app/

# Change ownership
RUN chown -R weather:weather /app

# Switch to non-root user
USER weather

# Default command (can be overridden)
CMD ["/app/weather-app"]