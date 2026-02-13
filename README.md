# Spark Structured Streaming — Order Processing Pipeline

## My Approach

I designed this pipeline with **correctness first, then performance**. The core idea is simple: read order events from Kafka, clean them, and write three useful outputs — all in a single Spark Structured Streaming application.

**Ingestion & Cleaning:** The pipeline reads JSON events from Kafka using event-time semantics (not processing time), which ensures out-of-order events are handled correctly. I applied a 10-minute watermark to tolerate late-arriving data while keeping state bounded. Deduplication uses `(order_id, event_time)` as the key — if the same order arrives with the same timestamp, it's a duplicate.

**Stateful Processing:** For latest order state, I used `foreachBatch` with `max(struct(...))` — this is a neat trick where putting `event_time` as the first struct field makes Spark automatically pick the most recent event per order. The result is overwritten each batch since only the current state matters.

**Windowed Aggregations:** Revenue and cancellation metrics use 5-minute tumbling windows in append mode. Filters are applied *before* groupBy to minimize shuffle. Revenue excludes cancelled orders; cancellation counts only include them.

**Reliability:** Checkpointing is enabled on all three queries, making the pipeline fully restartable without data loss. On restart, Spark resumes from the last Kafka offset automatically.

---

## Quick Start

```bash
docker compose up -d --build                # Start Kafka + Producer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 spark/main.py   # Run pipeline (~60s)
spark-submit spark/summary.py               # View summary report
docker compose down -v                      # Clean up
```

**Prerequisites:** Docker, Python 3.7+, Spark 4.1.1, Java 8/11

---

## Architecture

```
Kafka (order_events)
    → JSON Parse → Watermark (10 min) → Dedup (order_id, event_time)
        ├→ Latest state per order   → ./data/latest_orders   (overwrite)
        ├→ Revenue per customer/5m  → ./data/order_value     (append, partitioned by customer_id)
        └→ Cancel count per 5m     → ./data/cancel_count    (append)
```

| Script | What it does |
|--------|-------------|
| `spark/main.py` | Streaming pipeline — reads Kafka, writes 3 Parquet outputs |
| `spark/summary.py` | Batch report — reads Parquet, prints analytics to console |

---

## Streaming Semantics

| Aspect | Choice | Why |
|--------|--------|-----|
| **Time model** | Event-time (`event_time` field) | Handles out-of-order events correctly |
| **Watermark** | 10 minutes | Balances late-data tolerance vs memory; state auto-evicted after 10 min |
| **Dedup key** | `(order_id, event_time)` | Same order + same timestamp = duplicate |
| **Latest state** | `max(struct(...))` in `foreachBatch` | Struct comparison picks latest `event_time` per order automatically |
| **Windows** | 5-minute tumbling | As required for revenue and cancel aggregations |

---

## Design Decisions

| Decision | Benefit | Trade-off |
|----------|---------|-----------|
| 10-min watermark | Tolerates late events | Higher state memory |
| 1-min trigger interval | Near-real-time freshness | More frequent I/O |
| Overwrite mode for latest state | Always current, no state growth | No historical states |
| Append mode for windowed queries | Efficient, windows are immutable after watermark | Cannot update past windows |
| Partition by `customer_id` | Fast customer-level queries | Potential skew |
| Parquet output | Columnar, compressed, efficient | Not human-readable |

---

## Optimizations

- **`spark.sql.shuffle.partitions = 2`** — default 200 is overkill for local execution
- **Filter before groupBy** — reduces shuffle volume (e.g., filter `CANCELLED` before revenue aggregation)
- **Struct-based max** — single shuffle to get latest state instead of multiple operations
- **Watermark-bounded state** — dedup state is automatically evicted, preventing unbounded growth

---

## Checkpointing & Recovery

Checkpoints stored in `./checkpoint/{latest, order_value, cancel}/`.

On restart, Spark resumes from the last committed Kafka offset and reconstructs state — **no data loss**. Duplicate Kafka messages are handled by the dedup logic.

```bash
# To reset and reprocess from scratch:
rm -rf ./checkpoint/* ./data/*
```

---

## Data Schema

**Input (Kafka):**
```json
{ "order_id": "str", "customer_id": "str", "product_id": "str",
  "event_type": "CREATED|UPDATED|CANCELLED", "quantity": int,
  "price": double, "event_time": "ISO-8601" }
```

**Outputs:**

| Output | Key Columns |
|--------|------------|
| `latest_orders` | `order_id`, `event_type`, `customer_id`, `product_id`, `quantity`, `price`, `event_time` |
| `order_value` | `window`, `customer_id`, `total_order_value` |
| `cancel_count` | `window`, `cancel_count` |

---

## Project Structure

```
├── docker-compose.yml          # Kafka infra
├── producer/                   # Auto-generates order events
├── consumer/                   # Displays events (optional)
├── spark/
│   ├── main.py                 # Streaming pipeline
│   └── summary.py              # Batch summary report
├── data/                       # Pipeline output (Parquet)
├── checkpoint/                 # Streaming checkpoints
└── TASK.md                     # Assignment spec
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Connection refused to Kafka | `docker compose ps` — ensure Kafka is running on port 9093 |
| `NoClassDefFoundError` | Use `--packages` flag with `spark-submit` |
| Checkpoint corruption | `rm -rf ./checkpoint/*` and restart |
| Out of memory | Add `--driver-memory 4g` to spark-submit |

---

**Author:** Raman Jangu
