# Concurrency invariants

- Producers own their random generators and unpublished `Order` values.
- The bounded queue mutex protects only queue contents, enqueue sequencing, closure, and queue metrics.
- Producers release the queue mutex before the consumer handles an order.
- The engine thread exclusively owns market prices, volume counters, pending pools, execution sequencing, and SQLite.
- No producer reads mutable market state. Generated limits are centered on immutable configured intrinsic prices.
- `stop_requested` and `fatal_error` are the only state shared without the queue mutex; both are atomic.
- The signal handler only stores to `stop_requested`. It never locks, allocates, logs, or calls SQLite.
- Queue closure is permanent. A closed queue rejects pushes but permits the consumer to drain existing entries.
- A successful run ends only after every dequeued order is filled or expired and the final transaction commits.
- A persistence failure invalidates the success guarantees, wakes/stops producers, rolls back the active batch, and returns a nonzero exit code.

