# High-Concurrency AMM Trading Simulator

A local C++20 simulation of many concurrent traders feeding one order engine through a bounded MPSC queue. It is an AMM-trigger model, not a traditional limit order book: market flow changes a bounded reference price and resting limits execute when that price reaches them.

## Build

Requirements: CMake 3.24+, a C++20 compiler, Git, and network access for the pinned SQLite, nlohmann/json, and Catch2 dependencies.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```

Sanitizer options are `AMM_ENABLE_ASAN`, `AMM_ENABLE_UBSAN`, and `AMM_ENABLE_TSAN`. ThreadSanitizer cannot be combined with AddressSanitizer and availability depends on the compiler/platform.

## Run

```sh
./build/amm-sim --config config/example.json
./build/amm-sim --traders 16 --orders-per-trader 50000 --queue-capacity 1024
./build/amm-sim --help
```

Settings use compiled defaults, then the JSON file, then CLI overrides. Ctrl+C stops generation, drains accepted queue entries, expires resting limits, and commits the ledger.

## Pricing and order behavior

Each symbol has independent state:

```text
net_flow = executed_buy_quantity - executed_sell_quantity
price = intrinsic_price × (1 + max_deviation × tanh(net_flow / liquidity))
```

Prices are stored as scaled integers. Market orders apply their flow and fill at the resulting price. A limit that is already eligible fills at the current reference price; otherwise it rests. Buy limits prioritize higher prices and sell limits lower prices, with enqueue sequence breaking ties. Each fill updates price and can trigger a finite cascade on the newly reached side.

The seed reproduces each producer's generated payload stream. Thread scheduling can change global enqueue order and therefore executions.

## SQLite ledger

The engine is SQLite's only owner and writer. `runs` stores the effective JSON configuration and completion status; `orders` stores every consumed order and terminal lifecycle; `executions` stores its fill; and `symbol_summaries` stores final market state. Transactions commit in configurable batches and once more at graceful shutdown. A process crash can lose the current uncommitted batch.

See [the concurrency invariants](docs/concurrency-invariants.md) and [the example scenario](config/example.json).

