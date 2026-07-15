# Vergil

A transaction risk-decision service with an asynchronous audit pipeline, built
in Go. A synchronous request path scores each transaction (ALLOW / REVIEW /
BLOCK) against Redis-backed features and a composable rule set, then emits an
audit event to Kafka off the request path. A consumer persists those events to
Postgres through a bounded worker pool.

```
                 POST /v1/transactions
                          │
                          ▼
        ┌───────────────────────────────┐        ┌─────────┐
        │            api                 │  feats │  Redis  │
        │  decode → features → rules →   │◄──────►│ (Z-set  │
        │  classify → respond            │  1 RTT │  + INCR) │
        └───────────────┬────────────────┘        └─────────┘
                        │ async publish (non-blocking)
                        ▼
                    ┌───────┐      ┌──────────────────────────┐    ┌──────────┐
                    │ Kafka │─────►│ consumer                 │───►│ Postgres │
                    │decisions│fetch│ batch → N workers →      │save│ decisions│
                    └───────┘      │ save → commit AFTER write│    └──────────┘
                                   └──────────────────────────┘
```

## Run it

```sh
cd deploy && cp .env.example .env   # set POSTGRES_PASSWORD / PGPASSWORD
docker compose up -d --wait

# api  (also serves /metrics on :8080, pprof on :6060)
go run ./cmd/api

# consumer  (metrics on :6061); reads PG* + PGSSLMODE from the environment
PGPASSWORD=vergil PGSSLMODE=disable PGPORT=5433 go run ./cmd/consumer
```

```sh
curl -s localhost:8080/v1/transactions -d \
  '{"txn_id":"t1","user_id":"u1","amount":6000,"currency":"XRP"}'
# {"txn_id":"t1","classification":"BLOCK","score":1.3}
```

## The five trade-offs

**1. Sliding-window velocity vs fixed-window amount-sum.**
Velocity is a sliding window over a Redis sorted set (ZADD/ZREMRANGEBYSCORE/
ZCARD) — accurate, but stores every event. Amount-sum is a fixed window bucketed
by `now/window` with INCRBYFLOAT — O(1) memory and cheaper, but the window edge
is a hard cliff: a burst straddling the boundary can push ~2x the intended
amount through. Accuracy vs cost, chosen differently per feature.

**2. Async publish off the request path.**
The decision is returned to the caller before the audit event is durably in
Kafka. `KafkaPublisher` uses an async writer, so `WriteMessages` enqueues and
returns without a broker round trip, and a publish failure is *logged, not
returned* — an already-made decision must never fail because the broker is
unavailable. The price: audit delivery is best-effort from the api's point of
view (the consumer side is then at-least-once).

**3. At-least-once + idempotent upsert (commit after write).**
The consumer commits Kafka offsets only *after* Postgres writes land. A crash
between save and commit replays the batch; `INSERT … ON CONFLICT (txn_id) DO
NOTHING` makes reprocessing a no-op. This is deliberately at-least-once rather
than paying for exactly-once — the idempotent key absorbs the duplicates.

**4. Bounded worker pool + micro-batch commit.**
Each iteration fetches a batch, fans it out to N workers over a dispatch
channel, waits on a WaitGroup barrier, then commits. Bounding the batch bounds
memory, so an overwhelmed consumer expresses backpressure as *growing Kafka
lag*, not unbounded RAM. Batch size trades commit amortisation against
redelivery blast radius: bigger batches amortise the commit over more messages
but replay more on failure.

**5. Worker count vs Postgres pool sizing.**
Throughput scales with worker concurrency only until the per-batch commit, the
barrier (wait for the slowest worker), and single-node Postgres write contention
dominate. The pool's `MaxConns` is sized to the worker count — fewer conns than
workers would serialise saves. Doubling workers gave ~1.5x, not 2x (see below).

## Measured numbers

Single dev box (Windows, Docker Desktop, Redis/Kafka/Postgres in containers).
Latency via k6 (50 VUs); pipeline throughput sampled from `/metrics`.

**Decision latency — pprof-driven optimisation (§5.3).**
A CPU profile under load showed the Redis round trip dominating `Decide`, which
made *two* sequential round trips (velocity, then amount-sum). Merging them into
one `TxPipeline` (`feature.Snapshot`) halved the RTT on the hot path:

| | throughput | p50 | p95 | p99 |
|---|---|---|---|---|
| before (2 Redis RTT) | 2,249 rps | 12.8 ms | 42.8 ms | 73.7 ms |
| after (1 Redis RTT)  | **4,582 rps** | **6.0 ms** | **22.2 ms** | **42.0 ms** |

**Pipeline throughput + lag under burst (§5.2, §5.4).**
Producing at ~4,582 events/s outran the consumer, so lag grew (peak observed
**146,900**) and then drained cleanly to 0 once the burst stopped — at-least-once
catch-up with no loss. Tuning the pool width:

| workers (= MaxConns) | batch save time | throughput |
|---|---|---|
| 4 | ~80 ms / 100 | ~1,250 msg/s |
| 8 | ~50 ms / 100 | ~1,905 msg/s |

Sublinear: past the point where INSERT concurrency helps, the batch commit and
Postgres become the limit. `batchSize=100`, `workers=8` is the current setting.

## Observability

- `/metrics` (Prometheus) on the api (`:8080`) and consumer (`:6061`):
  request-latency histogram, decisions-by-classification counter, batch-duration
  histogram, messages-processed counter, Kafka lag, pool width.
- pprof on the api's debug port (`:6060/debug/pprof`).
- Structured JSON logs via `log/slog`; `LOG_LEVEL` controls verbosity.

## Layout

```
cmd/api        decision http service
cmd/consumer   audit pipeline consumer
internal/rules       Rule interface, concrete rules, scorer, Classify
internal/feature     Redis feature store (Snapshot: velocity + amount-sum)
internal/decision    Service.Decide orchestration
internal/event       DecisionEvent + async Kafka publisher
internal/audit       AuditStore + pgxpool Postgres store + schema
internal/pipeline    consumer, worker pool, topic bootstrap
internal/metrics     Prometheus collectors
```
