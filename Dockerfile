# Multi-stage build for minimal image size
FROM golang:1.25.6-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git protobuf-dev bash

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download Go protobuf plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11 && \
    go mod download

# Ensure protoc-gen-go is on PATH
ENV PATH="/root/go/bin:${PATH}"

# Copy source code
COPY . .

# Generate protobuf files from submodule
RUN bash scripts/generate_proto.sh

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o metroserver -ldflags="-w -s" ./cmd/metroserver

# Final stage - minimal runtime image
FROM alpine:3.20

# Install runtime dependencies (curl for health checks)
RUN apk add --no-cache ca-certificates

# Create non-root user for security
RUN addgroup -g 1000 metrolist && \
    adduser -D -u 1000 -G metrolist metrolist

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/metroserver .

# Change ownership to non-root user
RUN chown -R metrolist:metrolist /app

# Switch to non-root user
USER metrolist

# Expose port (default 8080, can be overridden)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run the application
# PORT can be overridden at runtime using -e PORT=<port>
CMD ["./metroserver"]
