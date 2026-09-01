# High-Throughput Metrics Ingestion Pipeline

A production-oriented, asynchronous event-driven backend engineering sandbox designed to accept high-frequency metric payloads, decouple ingestion networks from upstream bottlenecks, execute configurable micro-batch allocations, and survive severe load spikes using explicit multi-tiered memory backpressure safeguards.

[![Go Version](https://shields.io)](https://go.dev)
[![Platform](https://shields.io)](https://getfedora.org)
[![Infrastructure](https://shields.io)](https://apache.org)

---

## 🏗️ System Architecture

The pipeline decouples network response constraints from downstream messaging infrastructure I/O tasks using a highly stable bounded memory ring layout:

```text
  ┌──────────────────┐
  │   HTTP Clients   │
  └────────┬─────────┘
           │  POST /api/v1/metrics (High-Frequency Requests)
           ▼
  ┌──────────────────┐
  │   HTTP Handler   │ ──► [ Queue Full ] ──► HTTP 503 Service Unavailable
  └────────┬─────────┘
           │  (Non-blocking select push)
           ▼
  ┌───────────────────────┐
  │  Buffered Go Channel  │
  │   chan MetricFrame    │  (Capacity: 10,000 slots)
  └────────┬──────────────┘
           │
     ┌─────┼─────┐  (Concurrent worker pool routing)
     ▼     ▼     ▼
  ┌──────────────────┐
  │ Worker Array     │  (runtime.NumCPU() concurrent threads)
  │ [Micro-Batcher]  │  (Triggers: BatchSize == 500 OR Timeout == 100ms)
  └────────┬─────────┘
           │
           ▼
  ┌──────────────────┐
  │  Kafka Producer  │ ──► Apache Kafka Cluster (confluentinc/cp-kafka)
  └──────────────────┘
```

---

## 🛠️ Core Engineering Features

* **Decoupled Asynchronous Processing:** Isolates inbound HTTP connections immediately from backend disk or broker operations by offloading parsed domain payloads directly to standard memory rings (`chan domain.MetricFrame`).
* **Strategy A Backpressure Safeguards:** Completely bounds system memory limits. Whenever severe burst traffic patterns saturate the internal queue capacity (10,000 slots), the engine drops the excess load safely and signals callers instantly with an explicit `HTTP 503 Service Unavailable` error boundary.
* **Smart Micro-Batching Mechanics:** Combines memory elements sequentially into granular arrays based on twin configurable thresholds: Maximum Batch Size (500 entries) or Maximum Window Wait Time (100ms Ticker loops), reducing upstream transmission footprints using `segmentio/kafka-go`.
* **Deterministic Graceful Shutdowns:** Intercepts OS signals (`SIGINT`/`SIGTERM`) to coordinate structural thread draining operations: cuts incoming endpoints → seals channel entries → forces background workers to clear out buffered fragments → safely flushes pending batches and releases downstream publishing loops under a strict 5-second hard contextual limit.
* **Granular Diagnostics:** Integrates Go's standard `net/http/pprof` runtime engine on an isolated administrative port (`:6060`) to inspect active allocation graphs, thread traces, and memory growth profiles under stress.

---

## 📦 Directory Structure

```text
├── cmd/
│   └── server/
│       └── main.go         # Application entrypoint & dependency bootstrap
├── internal/
│   ├── config/             # Environment variable loaders with type-safe fallbacks
│   ├── domain/             # Shared operational primitives and data validation layers
│   ├── ingestion/          # Network controllers and HTTP routing definitions
│   ├── worker/             # Concurrency pools, batchers, and Kafka publisher runtimes
└── tests/
    └── load_test.go        # High-stress concurrent orchestration benchmarking script
```

---

## ⚡ Execution and Testing Quickstart

### Native Host Building & Test Execution (Fedora)
Ensure your host machine possesses standard development toolchain frameworks (`gcc` component loops are required to handle advanced tracking parameters):

```bash
# 1. Fetch system layout dependencies
go mod tidy

# 2. Run the complete unit test framework with the race detector enabled
go test -v -race ./internal/...
```

### Containerized Orchestration Engine (Docker)
Build and mount the application inside a multi-stage compilation framework leveraging isolated Alpine Linux environments alongside a KRaft-mode single-node Apache Kafka broker:

```bash
# Initialize background container environment
docker compose up --build -d

# Inspect live cluster logging configurations
docker compose logs -f ingestion-server
```

---

## 📊 Performance Benchmarks & Load Tests

The pipeline was subjected to high-frequency concurrency validation testing to evaluate ring queue drainage velocity and baseline thread allocations.

### Test Specification
* **Total Transmitted Payload Volume:** 15,000 requests
* **Concurrency Boundary Floor:** 100 simultaneous network workers
* **Target Interface Endpoint:** Containerized Go instance via port `8080`

### Measured Operational Results
```text
Launching 15000 stress requests at http://localhost:8080/api/v1/metrics with a concurrency floor of 100...

--- Stress Load Report ---
Total Processing Time: 1.494826577s
Successful Ingestions (202 Accepted): 15000
Backpressure Bounces (503 / Network drops): 0
Throughput Rate: 10034.61 requests/sec
--- PASS: TestHighFrequencyIngestionLoad (1.49s)
```

### Runtime Verification via `pprof`
Navigating to `http://localhost:6060/debug/pprof/goroutine?debug=1` during sustained stress testing confirms strict memory containment bounds:
* **Total Active Goroutines:** Constant at $\sim$6 to 8 active routines (4 dedicated pool threads + networking lifecycles).
* **Memory Growth:** Bounded and stable allocation overhead with zero unmanaged leaks under heavy multi-threaded payload transfers.

---

## 📋 Configurable Variables

The system relies entirely on environment variable overrides with secure, production-tested default configurations:

| Parameter | Type | Default | Operational Purpose |
| :--- | :--- | :--- | :--- |
| `SERVER_PORT` | String | `8080` | Ingestion API network entrypoint |
| `WORKER_COUNT` | Integer | `4` | Number of background worker goroutines |
| `QUEUE_CAPACITY` | Integer | `10000` | Backpressure saturation checkpoint buffer limit |
| `KAFKA_BROKERS` | String | `localhost:9092` | Comma-separated addresses of Kafka clusters |
| `KAFKA_TOPIC` | String | `telemetry.metrics` | Downstream distribution log sink target |
