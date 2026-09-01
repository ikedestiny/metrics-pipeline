# Stage 1: Build the optimized binary
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Leverage Docker cache layers for dependencies
COPY go.mod ./
RUN go mod download

COPY . .

# Compile optimized static binary without debugging components
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /metrics-server cmd/server/main.go

# Stage 2: Minimal runtime image
FROM alpine:3.19

WORKDIR /

COPY --from=builder /metrics-server /metrics-server

EXPOSE 8080

ENTRYPOINT ["/metrics-server"]
