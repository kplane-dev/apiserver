# Multi-stage build for kplane-dev/apiserver
# Builder stage
FROM golang:1.24.6-bookworm AS builder

WORKDIR /workspace

# Enable Go modules and better caching
ENV CGO_ENABLED=0 \
    GO111MODULE=on

# Pre-cache dependencies
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

# Copy the source
COPY . .

# Build the apiserver binary
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/apiserver ./cmd/apiserver

# Runtime stage
# Use distroless base for CA certs and nonroot user
FROM gcr.io/distroless/base-debian12:nonroot

WORKDIR /

COPY --from=builder /out/apiserver /apiserver

EXPOSE 6443

USER nonroot:nonroot

ENTRYPOINT ["/apiserver"]

