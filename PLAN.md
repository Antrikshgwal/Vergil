# Real-Time Fraud Decision Pipeline — Implementation Plan

A deliberately generic Go backend whose job is to make you write a lot of Go by hand, drill distributed-systems fundamentals, and give your portfolio a legible "I can run a normal backend in prod" anchor. It is **not** a wow project and we are not trying to make it one. The rigor goes into the hard parts, not the feature list.

**Primary goal: you learn by building it.** You write the code. This document is the map; [GUIDE.md](GUIDE.md) is the working protocol (how we split the work, how reviews happen, how progress is tracked). Read GUIDE.md before you write your first line.

---

## 1. The shape (read this before anything else)

The one non-negotiable design rule: **the decision is computed synchronously in the request path. Kafka comes *after* the decision, not before it.**

```
                    ┌─────────── synchronous, must be single-digit ms ───────────┐
                    │                                                            │
   POST /v1/txn ──► HTTP handler ──► Redis feature lookup ──► rule engine ──► ALLOW/REVIEW/BLOCK ──► return to caller
                                                                                 │
                                                                                 │ publish decision + audit event
                                                                                 ▼
                                                                              Kafka topic (Redpanda)
                                                                                 │
                    ┌──────────── asynchronous, can be slower, absorbs bursts ───┤
                    │                                                            ▼
                    │                                              bounded Go worker pool (yours, by hand)
                    │                                                            │
                    │                                                            ▼
                    └──────────────────────────────────────────────►  Postgres audit trail
```

Why this shape:
- The payment caller gets a real decision **inline**, in milliseconds. That's the whole point of a fraud check. Putting Kafka between request and decision breaks that.
- Kafka's real job here is to **decouple the durable write from the request path.** If Postgres spikes or a burst hits, the request path is unaffected — Kafka buffers, the worker pool drains at its own pace, lag grows and shrinks. That's the correct motivation for event-driven design, and it puts your worker pool at the center where the learning is.

---

## 2. Architecture

### 2.1 Components and contracts

| Component | Package | Responsibility | Talks to |
|-----------|---------|----------------|----------|
| HTTP API | `internal/httpapi` | Validate request, decode/encode JSON, map decision → HTTP response | decision service |
| Decision service | `internal/decision` | Orchestrate: fetch features → score → classify → emit event | feature store, rules, producer |
| Feature store | `internal/feature` | Velocity + amount aggregates per user | Redis |
| Rule engine | `internal/rules` | Pure `features → score` scoring; threshold → classification | nothing (pure) |
| Producer | `internal/events` | Publish `DecisionEvent` to Kafka, async/buffered | Redpanda |
| Consumer + pool | `internal/pipeline` | Fetch events, dispatch to bounded worker pool, write audit | Redpanda, Postgres |
| Audit store | `internal/audit` | Idempotent upsert of decision records | Postgres |
| Metrics | `internal/metrics` | Prometheus registry + `/metrics` | — |

**Rule: each component is an interface at its boundary.** `FeatureStore`, `Rule`, `AuditStore`, `EventPublisher` are all interfaces. Redis/Postgres/Kafka are implementations behind them. This is half the point of the project — clean seams, unit-testable core.

### 2.2 Request path (synchronous, hot)

```
handler.ServeHTTP
  → decode + validate TxnRequest
  → decision.Decide(ctx, txn)
        → featureStore.Velocity(ctx, userID)      // Redis, single round trip ideally
        → featureStore.AmountSum(ctx, userID)      // Redis
        → rules.Score(features) → score
        → classify(score) → ALLOW | REVIEW | BLOCK
        → publisher.Publish(DecisionEvent)          // async, does NOT block on broker ack
  → encode DecisionResponse
```

Budget: everything above the `Publish` line must be single-digit ms p99. `Publish` hands off to a buffered writer and returns immediately.

### 2.3 Audit path (asynchronous, cold)

```
consumer.FetchMessage (manual offset)
  → dispatch chan DecisionEvent  (buffered, size = backpressure knob)
  → worker[N] (fixed pool, sync.WaitGroup)
        → auditStore.Upsert(ctx, event)   // ON CONFLICT DO NOTHING
        → on success: consumer.CommitMessages(offset)   // commit AFTER write = at-least-once
```

### 2.4 Data models

```go
// wire: request
type TxnRequest struct {
    TxnID     string    `json:"txn_id"`
    UserID    string    `json:"user_id"`
    Amount    float64   `json:"amount"`
    Currency  string    `json:"currency"`
    Timestamp time.Time `json:"timestamp"`
}

// wire: response (synchronous)
type DecisionResponse struct {
    TxnID          string  `json:"txn_id"`
    Classification string  `json:"classification"` // ALLOW | REVIEW | BLOCK
    Score          float64 `json:"score"`
    Reasons        []string `json:"reasons"`
}

// internal: what rules consume
type Features struct {
    VelocityCount int     // txns in sliding window
    AmountSum     float64 // summed amount in fixed window
    Amount        float64 // this txn
}

// kafka: audit event
type DecisionEvent struct {
    TxnID          string
    UserID         string
    Amount         float64
    Features       Features
    Score          float64
    Classification string
    DecidedAt      time.Time
}
```

### 2.5 Postgres schema

```sql
CREATE TABLE audit_decisions (
    txn_id         TEXT PRIMARY KEY,          -- idempotency key
    user_id        TEXT NOT NULL,
    amount         NUMERIC(18,2) NOT NULL,
    velocity_count INT NOT NULL,
    amount_sum     NUMERIC(18,2) NOT NULL,
    score          NUMERIC(6,3) NOT NULL,
    classification TEXT NOT NULL,
    decided_at     TIMESTAMPTZ NOT NULL,
    written_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON audit_decisions (user_id, decided_at);
```

### 2.6 Repo layout

```
cmd/
  api/main.go          # wires HTTP server + producer, starts them
  worker/main.go       # wires consumer + pool + Postgres (can be same binary via flag; keep separate for clarity)
internal/
  httpapi/             # handler, routing, request/response types
  decision/            # orchestration, classify()
  feature/             # FeatureStore interface + redis impl
  rules/               # Rule interface, concrete rules, scorer (pure, well-tested)
  events/              # DecisionEvent, EventPublisher interface + kafka impl
  pipeline/            # consumer, dispatch channel, worker pool, shutdown
  audit/               # AuditStore interface + pgx impl
  metrics/             # prometheus collectors
  config/              # env parsing
deploy/
  docker-compose.yml   # redis, postgres, redpanda
  schema.sql
GUIDE.md               # the build-and-learn protocol + progress tracker
PLAN.md                # this file
README.md              # written on day 5 — the trade-off narrative
```

### 2.7 Stack (each choice has a reason; don't swap without one)

- **HTTP**: stdlib `net/http` with Go 1.22 method-routed `ServeMux`. No framework — you want fundamentals, not chi's convenience.
- **Redis**: `redis/go-redis/v9`. Feature store.
- **Kafka**: run **Redpanda** locally (single binary, no Zookeeper/JVM, speaks the Kafka protocol — your Go is identical to real Kafka). Client: `segmentio/kafka-go` for readable Reader/Writer + manual offset control. (`twmb/franz-go` is the more production-grade client; name it in the README as the alternative.)
- **Postgres**: `jackc/pgx/v5` + `pgxpool`. Sizing the pool is one of the things you're here to learn.
- **Config**: plain env vars. No config framework.

---

## 3. Guardrails (protect the timebox)

1. **The fundamentals live in the parts you write by hand.** Docker-composing infra teaches configuration, not engineering. Write these yourself every time: the bounded worker pool, graceful shutdown with in-flight drain, backpressure, offset-commit semantics, pool sizing. If a library offers to hide one of these, that's exactly the part you implement manually.
2. **Timebox and do not gold-plate.** Finish a phase's definition-of-done, stop, even if you see polish.
3. **The deterministic-replay / ML upgrade is off-limits mid-build.** If the wow itch returns, that's insecurity, not a plan. Promote the project on purpose, later, with a clear head.
4. **Each phase ends with something that runs.** If slipping, cut scope from the *bottom* of the phase list, never ship a half-wired system forward.

---

## 4. Build phases

Each phase below is split into concrete tasks in [GUIDE.md](GUIDE.md), each with a hint (not a solution) and an acceptance check. Here is the map and the definition-of-done per phase.

### Phase 1 — The request path, end to end and runnable
**DoD:** `curl` a transaction to `POST /v1/transactions`, get back a real `ALLOW`/`REVIEW`/`BLOCK` decision computed from a Redis-backed velocity counter. No Kafka, no async, no Postgres yet.

- Repo scaffold + package layout. Clean boundaries — half the point.
- `deploy/docker-compose.yml` up with Redis, Postgres, Redpanda now, even though only Redis is used today. Get infra pain out of the way early.
- HTTP handler: validation, JSON decode/encode, sane errors, method-routed mux.
- `FeatureStore` interface + Redis impl. Start with a **fixed-window** velocity (`INCR` + `EXPIRE` on `velocity:{userID}`).
- Minimal rule engine: 2 rules (high amount, velocity over threshold), scorer that sums, thresholds → classification. Synchronous return.

**Fundamentals:** interfaces, package design, Redis atomic ops, stdlib HTTP.
**Interview hook forming:** "the decision is synchronous and inline — here's why."

### Phase 2 — Real scoring logic, tested
**DoD:** decision computed from a proper sliding-window feature store and a clean composable rule engine, rule logic unit-tested. Still synchronous, still no Kafka.

- Upgrade feature store to **sliding-window** velocity via Redis sorted set: `ZADD velocity:{userID} <now> <txnID>`, `ZREMRANGEBYSCORE ... -inf (now-window)`, `ZCARD` for the count. Wrap the read-modify sequence in a pipeline or Lua script so it's atomic; set `EXPIRE` so idle users get cleaned up.
  - Trade-off to bake in and note in README: sliding window is exact for a *count*, but a sliding-window *sum* needs more machinery. Keep amount-sum as fixed-window `INCRBYFLOAT` + `EXPIRE` and write down why. Naming the trade-off is the senior signal.
- Rule engine as a proper abstraction: `Rule` interface (`Evaluate(Features) contribution`), several concrete rules, a scorer that composes them, configurable thresholds. Rules are pure → trivially unit-testable. Write table-driven tests.

**Fundamentals:** Redis data structures, atomicity, testable design, table-driven Go tests.
**Interview hook:** "velocity is a sliding window; here's the exact-vs-approximate trade-off and why."

### Phase 3 — Go async: producer, worker pool, Postgres
**DoD:** full pipeline works end to end. Transaction in → decided synchronously → decision+audit published to Kafka → worker pool consumes → audit trail in Postgres. Verify rows land.

- **Producer**: after the decision returns, publish `DecisionEvent` to a Kafka topic. Buffered/async writer so the request path isn't waiting on the broker.
- **Postgres schema**: `audit_decisions` keyed by txn ID. Set up `pgxpool`.
- **The worker pool — by hand, the centerpiece:**
  - Consumer using `kafka-go`'s `FetchMessage` + `CommitMessages` (manual offsets, **not** auto-commit `ReadMessage`). Manual commit is the whole learning.
  - A **bounded** pool: consumer fetches → buffered channel → fixed number of workers (`sync.WaitGroup`) read and write to Postgres.
  - Commit the offset **only after** the Postgres write succeeds → at-least-once.

**Fundamentals:** goroutines, channels, `WaitGroup`, consumer groups, connection pooling, at-least-once semantics.
**Interview hook:** "here's my worker pool and why I commit offsets after the write, not before."

### Phase 4 — Hardening (the day you become a better Go engineer)
**DoD:** system survives Ctrl-C mid-load without dropping/corrupting records, and behaves correctly when Postgres is slow.

- **Graceful shutdown with in-flight drain**, correct order: `signal.NotifyContext` for SIGINT/SIGTERM → stop HTTP accepting → stop consumer fetching → close dispatch channel → `WaitGroup.Wait()` → commit final offsets → close pools. No message half-processed, none dropped.
- **Backpressure**: when Postgres is slower than intake, the bounded channel fills, the fetch loop blocks, Kafka lag grows. That's the relief valve working. Make it observable, understand it.
- **Idempotency**: a crash between write and commit means a message can be reprocessed → make the Postgres write an idempotent upsert on txn ID (`ON CONFLICT DO NOTHING`). Reprocessing now safe.
- **Worker-death handling**: a panic in one worker shouldn't take the pool down or silently stop consumption. Recover, log, continue; the uncommitted message gets redelivered.

**Fundamentals:** context cancellation, shutdown ordering, backpressure, delivery semantics, idempotency, panic recovery.
**Interview hook:** "here's how I drain in-flight work on shutdown, and why at-least-once + idempotent upsert is the honest choice over pretending to have exactly-once."

### Phase 5 — Prove it and document it
**DoD:** numbers, minimal observability, and a README that tells the story and names the trade-offs.

- **Load test — measure two things separately:**
  1. **Synchronous decision latency** (must be ms): hit `POST /v1/transactions` with `vegeta`/`k6`, read the latency histogram (p50/p95/p99). Headline "decides in X ms."
  2. **Async pipeline throughput**: how fast the pool drains Kafka into Postgres, how lag behaves under burst.
- **pprof** under load: CPU + allocation profiles. Find the hot path, tune one thing, show before/after.
- **Tune** worker count, channel buffer size, `pgxpool` MaxConns under load — and say *why* your numbers are what they are.
- **Minimal observability**: `/metrics` (`prometheus/client_golang`) — decision-latency histogram, decisions-by-class counter, consumer lag, pool utilization.
- **README that names the trade-offs** (your interview script):
  1. Score synchronously, Kafka after the decision — latency vs. audit durability.
  2. At-least-once + idempotent upsert vs. the exactly-once fiction.
  3. Backpressure via bounded channel, Kafka lag as the relief valve.
  4. Transactional outbox as the stricter alternative to publishing the decision directly — what it buys, why you did/didn't use it.
  5. Sliding vs. fixed window in the feature store.

**Interview hook:** the whole README. A generic project becomes senior-sounding through the rigor of these five trade-offs, not through novelty.

---

## 5. Definition of done

- End to end: transaction in → millisecond decision out → audit trail durably in Postgres via the async pipeline.
- Survives shutdown mid-load with zero dropped or corrupted records.
- p99 decision latency and pipeline throughput numbers you can defend.
- README names all five trade-offs and you can talk to each without notes.

If you hit that, it's done. Don't add a sixth thing.
