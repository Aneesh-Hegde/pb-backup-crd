# Stage 1: Build the Go binary
FROM golang:1.25-alpine AS builder
WORKDIR /workspace

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY cmd/main.go cmd/main.go
COPY api/ api/
COPY internal/ internal/

# Build a statically linked binary (CGO_ENABLED=0 is critical for distroless)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o manager cmd/main.go

# Stage 2: The production image (Zero terminal access)
FROM gcr.io/distroless/static:nonroot
WORKDIR /
# Copy only the compiled binary from the builder stage
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
