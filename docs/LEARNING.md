# High-Concurrency AMM Trading Simulator — Learning Documentation

This document explains the "why" and "how" behind every significant design decision in this
project. It is written as a tutorial for someone with strong competitive-programming-level C++
and algorithms knowledge, but new to systems / concurrency / exchange-style engineering.

The project is **not** a traditional limit-order book with a matching engine. It is an
**AMM-trigger model**: a swarm of producer threads generate a finite stream of orders, push
them through one bounded MPSC queue, and a single engine thread consumes them. Market orders
move a bounded reference price along a `tanh` curve; resting limit orders execute when that
price reaches them, possibly firing a bounded cascade. Every consumed order is persisted to
SQLite and ends in a terminal state (`FILLED` or `EXPIRED`).

Read this alongside:
- [`README.md`](../README.md) — authoritative build/run/config/pricing reference
- [`docs/concurrency-invariants.md`](concurrency-invariants.md) — the synchronization contract
- [`config/example.json`](../config/example.json) — a two-symbol scenario

---

## Table of contents

1. [Big picture: the architecture and the data flow](#1-big-picture-the-architecture-and-the-data-flow)
2. [The build system — `CMakeLists.txt`](#2-the-build-system--cmakeliststxt)
3. [Domain types — `include/amm/types.hpp`](#3-domain-types--includeammtypeshpp)
4. [Configuration — `include/amm/config.hpp` + `src/config.cpp`](#4-configuration--includeammconfighpp--srcconfigcpp)
5. [The bounded MPSC queue — `include/amm/bounded_queue.hpp`](#5-the-bounded-mpsc-queue--includeammbounded_queuehpp)
6. [The pricing model — the `Market` class](#6-the-pricing-model--the-market-class)
7. [The engine — `include/amm/engine.hpp` + `src/engine.cpp`](#7-the-engine--includeammenginehpp--srcenginecpp)
8. [The SQLite ledger — `include/amm/ledger.hpp` + `src/ledger.cpp`](#8-the-sqlite-ledger--includeammledgerhpp--srcledgercpp)
9. [Orchestration — `src/main.cpp`](#9-orchestration--srcmaincpp)
10. [The concurrency invariants document](#10-the-concurrency-invariants-document)
11. [The test suite — `tests/test_core.cpp`](#11-the-test-suite--teststest_corecpp)
12. [Cross-cutting themes and what to study next](#12-cross-cutting-themes-and-what-to-study-next)

---

## 1. Big picture: the architecture and the data flow

**What it does:**
Coordinates *N* producer threads, one bounded queue, one consumer (engine) thread, and one
SQLite connection so that a finite, seeded workload is generated, priced, and durably recorded
with well-defined behavior on both clean completion and Ctrl+C.

**The data flow, end to end:**

```
 config (defaults -> JSON -> CLI)                 [src/config.cpp]
        |
        v
 main() builds: BoundedMpscQueue<Order>, Engine, atomics                [src/main.cpp]
        |
        |  spawn 1 consumer thread                spawn N producer threads
        v                                          v
 Engine::run()  <--- pop() --- BoundedMpscQueue --- push() <--- produce()
        |                       (mutex + 2 CVs)                  (owns its own RNG)
        v
 process(order)
   |  market order  -> execute(): move price along tanh, record fill, cascade
   |  limit  order  -> eligible?  execute()  :  pend() into a priority_queue
        |
        v
 Ledger  ->  SQLite  (prepared statements, BEGIN IMMEDIATE / COMMIT in batches, WAL)
        |
        v
 on shutdown: drain queue, expire_pending(), verify_invariants(),
              write symbol_summaries, final COMMIT, UPDATE runs.status
```

**Why this shape (single-writer engine, many producers):**
The hardest part of any concurrent trading system is keeping *mutable market state*
consistent. This design sidesteps almost all of that difficulty with one rule: **only the
engine thread ever touches market state, execution sequencing, or SQLite.** Producers are
embarrassingly parallel — they own their random generator and the `Order` they are building,
and the *only* thing they share with the engine is the queue (mutex-protected) plus two
atomic flags. There is no lock on a price, no lock on the pending pool, no lock on the DB,
because nothing else can race the engine for them.

This is the **single-writer principle** (a.k.a. "shard by ownership"): instead of making a
resource thread-safe, make it owned by exactly one thread and force everyone else to
communicate through a queue. LMAX Disruptor, Redis, and most exchange matching cores are
built this way.

**Why an AMM-trigger model instead of a real limit-order book:**
A central limit order book needs price-level trees, intrusive FIFO queues per level,
aggressor/resting matching, partial fills, and cancel/replace. That is a large project. The
AMM model keeps the *systems* lessons (bounded queue, backpressure, single-writer,
transactional persistence, graceful shutdown, deterministic seeding) while replacing the
matching core with a closed-form price function. You still get resting orders, price-time
priority, and cascades — just driven by a scalar reference price rather than a book.

**Benefits:**
- The concurrency model fits on one page ([`docs/concurrency-invariants.md`](concurrency-invariants.md)).
- Every subsystem is independently testable (queue, `Market`, config, `Ledger`, full engine, full CLI).
- Determinism is achievable: the same seed reproduces each producer's payload stream.

**Drawbacks / tradeoffs accepted:**
- **Not a real matching engine.** No partial fills, no cancels, no bid/ask spread, no book depth.
- **Global order is nondeterministic.** Thread scheduling decides the interleaving of producers'
  pushes, so the *sequence* of executions varies run to run even at a fixed seed (see §5).
- **One engine thread is the throughput ceiling.** Fine for a simulator; a real venue shards
  by symbol across cores.

**How this connects to the rest of the doc:**
Every following section is one box in the diagram above. Read them in order: types → config →
queue → market → engine → ledger → main ties them together.

**Check your understanding:**
1. Which pieces of state are *only* touched by the engine thread? Which are shared, and how is
   each shared piece protected?
2. Why can producers avoid locking entirely except for the queue push?
3. What does the AMM model keep from a real LOB, and what does it drop?

---

## 2. The build system — `CMakeLists.txt`

**What it does:**
Defines a C++20 build with three fetched dependencies (nlohmann/json, the SQLite amalgamation,
Catch2), a reusable static library `amm_core`, the `amm-sim` executable, an optional test
binary `amm-tests`, and opt-in sanitizer wiring.

**Exact locations:**
- [`CMakeLists.txt`](../CMakeLists.txt) lines 1–80

**Key decisions, line by line:**

**`cmake_minimum_required(VERSION 3.24)` + `project(... LANGUAGES C CXX)`**
3.24 is required because `FetchContent_Declare(URL ...)` with `DOWNLOAD_EXTRACT_TIMESTAMP` and
the modern `FetchContent_MakeAvailable` ergonomics need a recent CMake. `C` is listed as a
language because SQLite ships as a single `.c` file that must be compiled with a C compiler.

**`set(CMAKE_CXX_STANDARD 20)` / `STANDARD_REQUIRED ON` / `EXTENSIONS OFF`**
This trio means "pure ISO C++20 or fail" — no silent fallback to C++17, no `-std=gnu++20`
compiler extensions. `EXTENSIONS OFF` turns `-std=gnu++20` into `-std=c++20`, so the code
cannot accidentally depend on GNU-only behavior and stay portable to MSVC.

**`FetchContent` for nlohmann/json and SQLite**
`FetchContent` downloads and configures dependencies *at configure time* (when you run
`cmake -S . -B build`), not at build time. The project pins exact release URLs:
- `json` v3.11.3 as a `.tar.xz` release archive
- `sqlite-amalgamation-3460100.zip` straight from sqlite.org

The comment in the file is the important lesson: *"SQLite's versioned amalgamation archive is
immutable. We compile the C source directly so every platform uses the same SQLite release."*
Rather than `find_package(SQLite3)` (which picks up whatever the OS ships — could be 3.31 on
an old Ubuntu, 3.45 on a Mac), the build compiles one known SQLite version from source into a
static lib. That eliminates "works on my machine" persistence bugs.

```cmake
add_library(sqlite3 STATIC "${sqlite_amalgamation_SOURCE_DIR}/sqlite3.c")
target_include_directories(sqlite3 PUBLIC "${sqlite_amalgamation_SOURCE_DIR}")
if(UNIX)
  target_link_libraries(sqlite3 PUBLIC ${CMAKE_DL_LIBS} Threads::Threads)
endif()
```
On UNIX, SQLite needs `libdl` (for `dlopen` of extensions) and pthreads; on Windows it needs
neither, hence the `if(UNIX)` guard.

**`amm_core` as a static library, `amm-sim` as a thin executable**
`config.cpp`, `engine.cpp`, `ledger.cpp` compile into `amm_core`; `main.cpp` is the only file
in `amm-sim`. This split exists so the **test binary can link the exact same object code** as
the shipping binary (`amm-tests` links `amm_core` directly). If logic lived in `main.cpp`, the
tests could not reach it without recompiling or refactoring.

**`target_include_directories(amm_core PUBLIC include)`**
`PUBLIC` means "anything linking `amm_core` also gets `include/` on its header path" — so both
`amm-sim` and `amm-tests` can `#include "amm/engine.hpp"` without repeating the path.

**Warning flags**
```cmake
if(MSVC)  /W4 /permissive-
else()    -Wall -Wextra -Wpedantic -Wconversion -Wshadow
```
`-Wconversion` (warn on implicit narrowing, e.g. `int64_t` → `int`) and `-Wshadow` (warn when
a local hides an outer name) are stricter than the usual `-Wall -Wextra` pair and catch real
bugs in numeric code. `/permissive-` forces MSVC toward standard conformance. Note these are
`PRIVATE` — consumers of `amm_core` are not forced to build under the same pedantry, and the
fetched dependencies (which may not be warning-clean) are untouched.

**Sanitizers**
```cmake
option(AMM_ENABLE_ASAN ...)   # AddressSanitizer  — use-after-free, OOB, leaks
option(AMM_ENABLE_UBSAN ...)  # UndefinedBehavior — signed overflow, bad casts, misalignment
option(AMM_ENABLE_TSAN ...)   # ThreadSanitizer   — data races
```
They are appended into a single `-fsanitize=a,b` list and applied `PUBLIC` (so the sanitizer
runtime is linked into the final binary). The build hard-errors if ASan and TSan are both
requested — they use incompatible shadow-memory layouts and cannot coexist in one process.
Sanitizer flags are gated `AND NOT MSVC` because MSVC's sanitizer story is different
(`/fsanitize=address` only, no UBSan/TSan).

For a threaded project, **TSan is the highest-value tool here** — it is how you actually
verify the claims in `concurrency-invariants.md` rather than just asserting them.

**The test section**
```cmake
FetchContent Catch2 v3.7.1 (GIT_SHALLOW)
add_executable(amm-tests tests/test_core.cpp)
target_link_libraries(amm-tests PRIVATE amm_core Catch2::Catch2WithMain)
target_compile_definitions(amm-tests PRIVATE AMM_SIM_PATH="$<TARGET_FILE:amm-sim>")
add_dependencies(amm-tests amm-sim)
include(Catch); catch_discover_tests(amm-tests)
```
Three things worth noticing:
- `AMM_SIM_PATH` is a **generator expression** (`$<TARGET_FILE:amm-sim>`) baked in as a
  preprocessor string. The end-to-end test shells out to the real binary; this is how it finds
  it regardless of build directory or platform suffix (`.exe`).
- `add_dependencies(amm-tests amm-sim)` forces the executable to be built before the tests run,
  since one test executes it.
- `catch_discover_tests` runs the test binary at build time to enumerate `TEST_CASE`s and
  registers each as an individual CTest test, so `ctest -R` filtering and parallelism work.

**Benefits:**
- Fresh-clone reproducibility: `cmake -S . -B build && cmake --build build` works with only a
  compiler + network access.
- Pinned SQLite means the persistence layer behaves identically on every machine.
- Tests exercise shipping object code, not a copy.

**Drawbacks / tradeoffs accepted:**
- First configure needs the network (downloads ~a few MB). Offline builds need a populated
  `FetchContent` cache or vendored sources.
- Compiling `sqlite3.c` from scratch adds a few seconds to a clean build.
- Catch2 is pulled via `GIT_REPOSITORY` (needs `git` + a full clone even with `GIT_SHALLOW`),
  slightly heavier than a release tarball.

**Alternatives considered and rejected:**
1. `find_package(SQLite3)` — rejected: version drift across platforms is the exact bug class
   this project's persistence tests would be flaky against.
2. Vendoring dependencies in `third_party/` — rejected: bloats the repo, complicates updates,
   and `FetchContent` already gives hermetic-enough builds.
3. Header-only single-target build (no `amm_core` lib) — rejected: tests could not link the
   real code.
4. Conan/vcpkg — rejected: three dependencies do not justify a manifest toolchain.

**How this connects to what came before:**
This is the foundation. Everything else is a source file this build compiles.

**Check your understanding:**
1. Why compile `sqlite3.c` from a pinned zip instead of `find_package(SQLite3)`?
2. Why is application logic in a `amm_core` library rather than directly in `main.cpp`?
3. What is `$<TARGET_FILE:amm-sim>` and why can't the test just hardcode `"./amm-sim"`?
4. Why does the build refuse to enable ASan and TSan simultaneously?

---

## 3. Domain types — `include/amm/types.hpp`

**What it does:**
Declares every value type that crosses a subsystem boundary: the numeric aliases `Price` and
`Quantity`, the four enums, and the plain-aggregate structs `Order`, `Execution`,
`SymbolConfig`, `SimulationConfig`, `SymbolMetrics`, `RunMetrics`.

**Exact locations:**
- [`include/amm/types.hpp`](../include/amm/types.hpp) lines 1–102

**Why `Price = std::int64_t` and `Quantity = std::uint64_t` (scaled integers, not `double`):**
Floating point is banned from anything that must be exact and reproducible. `0.1 + 0.2 != 0.3`
in IEEE-754, and float rounding is not associative, so a price computed as
`sum over threads` would depend on *order of summation*, i.e. on thread scheduling. Integers
are exact and their arithmetic is associative.

The convention: a price is stored as `real_price * price_scale`. With `price_scale = 100`, the
value `10'000` means `$100.00`. `parse_price()` converts human input → scaled int once, at the
edge; `display_price()` converts back for output only. Internally the engine only ever sees
integers.

- `Price` is **signed** `int64_t`. Even though every real price is positive, signed integers
  make it safe to compute differences (`new_price - old_price`, used in the cascade to decide
  which side was triggered) without underflow surprises, and SQLite's native integer column is
  signed 64-bit — matching the storage type avoids casts at the DB boundary.
- `Quantity` is **unsigned** `uint64_t`: negative size is meaningless, and the engine
  explicitly checks for overflow before adding to the running volume counters
  (`projected_price` in `engine.cpp`).

**Why bare `using` aliases and not strong wrapper structs:**
This is a deliberate difference from a full production type system. `using Price = int64_t`
gives no compile-time protection against passing a quantity where a price is expected. The
project accepts that risk because (a) the surface is tiny — a handful of functions — and
(b) `parse_price`/`display_price` are free functions that take an explicit `scale`, so the
scaling convention is visible at every call site. A larger codebase would wrap these in
`struct Price { int64_t v; }` to make mixing them a compile error.

**The enums:**
```cpp
enum class OrderSide  { Buy, Sell };
enum class OrderType  { Market, Limit };
enum class OrderStatus { Accepted, Pending, Filled, Expired };
```
`enum class` (scoped) rather than plain `enum`: no implicit int conversion, no name leakage
into the namespace. Each has a `to_string` overload returning a `const char*` — these strings
(`"BUY"`, `"MARKET"`, `"FILLED"`, …) are what land in the SQLite text columns, so the DB is
human-readable without a lookup table. The `OrderStatus` version is a `switch` with **no
`default`**: if someone adds a fifth status, `-Wswitch` (part of `-Wall`) flags every
unhandled `switch` at compile time. The trailing `return "UNKNOWN";` satisfies "all control
paths return".

The `OrderStatus` lifecycle:
```
              accepted for immediate execution
Market  ---->  ACCEPTED  ------------------------------>  FILLED
Limit (eligible now)  -->  ACCEPTED  ------------------->  FILLED
Limit (not eligible)  -->  PENDING  --- price reaches --->  FILLED
                                    \--- run ends --------->  EXPIRED
```
`ACCEPTED` and `PENDING` are *initial* states written by `record_order`; `FILLED` and
`EXPIRED` are *terminal* states written by `update_status`. An invariant checked at the end is
that every consumed order is `FILLED` or `EXPIRED` — nothing is left `ACCEPTED`/`PENDING`.

**`struct Order`:**
```cpp
struct Order {
    std::uint64_t id{};
    std::uint32_t producer_id{};
    std::uint32_t producer_sequence{};
    std::uint64_t enqueue_sequence{};
    std::string symbol;
    OrderSide side{OrderSide::Buy};
    OrderType type{OrderType::Market};
    Quantity quantity{};
    std::optional<Price> limit_price;
};
```
- `id` is globally unique and **constructed, not counted**:
  `((producer_id + 1) << 32) | sequence` (see `main.cpp`). The top 32 bits identify the
  producer, the bottom 32 the per-producer sequence. No shared atomic counter needed, and the
  id is meaningful when you read the DB.
- `producer_id` + `producer_sequence` let you reconstruct exactly which producer emitted the
  order and in what position of *its* stream — the key to determinism analysis.
- `enqueue_sequence` is assigned by the *queue* under its mutex (see §5), so it reflects the
  true global order in which the engine will see orders. It is the tiebreaker for price-time
  priority among resting limits.
- `symbol` is a `std::string` (not an interned id / small-string enum). Simple; the cost is a
  heap allocation per order for long tickers and a `std::string` key hash on every
  `markets_.find`. Acceptable for a simulator, not for a real hot path.
- `limit_price` is `std::optional<Price>`. A market order has **no** limit price, and
  `std::optional` encodes that in the type instead of using a sentinel like `-1` or `0`.
  `eligible()` and the pricing code branch on `.has_value()`. The engine treats "market order
  carrying a limit price" and "limit order without one" as hard errors.

**`struct Execution`** is the fill record: which order, which symbol/side, how much, at what
price, and `engine_sequence` — a strictly increasing counter assigned by the engine when the
fill happens. Because it is engine-assigned under the single-writer rule, it is a total order
over *all* executions in the run, which makes the ledger replayable.

**The config structs (`SymbolConfig`, `SimulationConfig`):**
Every field has a **default member initializer** (`std::uint32_t traders{8};`). That means a
default-constructed `SimulationConfig` is already a valid, runnable scenario — the config
loader only has to overwrite what the JSON/CLI actually specifies (the "layered config"
pattern in §4). Note `std::vector<SymbolConfig> symbols{{}}` — braces-in-braces makes the
default a vector containing **one** default `SymbolConfig`, so "no symbols configured" still
runs one market.

Notable `SymbolConfig` fields and their roles in the price function:
- `intrinsic_price` — the anchor the price oscillates around; also the mean of generated limit prices.
- `liquidity` — how much net volume it takes to move the price meaningfully (the `tanh` denominator).
- `max_deviation` — the hard cap on fractional deviation (`0.5` ⇒ price stays within ±50%).
- `price_scale` — the integer scaling factor for this symbol.

**`RunMetrics`** is the observability bundle: counts (`generated`, `accepted`, `processed`,
`filled`, `expired`, `market_orders`, `limit_orders`, `executions`, `transactions`), queue
health (`queue_high_water`, `blocked_pushes`), timing (`elapsed_seconds`), status flags
(`interrupted`, `failed`, `error`), and per-symbol `SymbolMetrics`. `generated` is filled from
an atomic owned by `main`; everything else is incremented by the engine thread with no
synchronization because only it writes them.

**Complexity:** all types are O(1) to construct/copy; `Order` and `Execution` copies include a
`std::string` copy (one heap allocation for non-SSO tickers).

**Benefits:**
- Exact, reproducible arithmetic (integers) with a single documented scaling convention.
- Self-describing persistence (enum `to_string` values are stored directly).
- Default-constructible configs enable the layered-override loader.
- `std::optional<Price>` makes "no limit price" unrepresentable-as-a-bug.

**Drawbacks / tradeoffs accepted:**
- Bare aliases: no compile-time price/quantity mix protection.
- `std::string symbol` per order: allocation + hashing overhead on the engine's hot path.
- Wide structs passed by value in a few places (`SymbolMetrics`) — trivially cheap here, would
  matter at scale.

**Alternatives considered and rejected:**
1. `double` prices — rejected: non-associative, non-reproducible, fails exact assertions.
2. Strong-typedef wrappers for `Price`/`Quantity` — reasonable, omitted for surface-area reasons.
3. Interned symbol ids (`uint16_t`) — better for a hot path; overkill for a simulator whose
   bottleneck is SQLite, not string hashing.
4. A single shared atomic `next_order_id` — rejected: the bit-packed id needs no coordination
   and encodes provenance.

**How this connects to what came before:**
The build (§2) compiles these declarations into every translation unit. Everything downstream
— config, queue, engine, ledger — is expressed in these types.

**Check your understanding:**
1. Why would `double` prices make the simulation non-deterministic even at a fixed seed?
2. How is a globally unique `Order::id` produced without any shared counter or lock?
3. What is the difference between `producer_sequence` and `enqueue_sequence`, and which one
   drives price-time priority?
4. Why is `limit_price` a `std::optional<Price>` rather than a `Price` with `0` meaning "none"?
5. The `OrderStatus` `to_string` `switch` has no `default:` case. What do you gain by omitting it?

---

## 4. Configuration — `include/amm/config.hpp` + `src/config.cpp`

**What it does:**
Builds a fully validated `SimulationConfig` from three layers — compiled defaults, then an
optional JSON file, then CLI flags — rejecting unknown keys/options and every out-of-range
value before the simulation starts. Also owns the scaled-price conversion helpers and the
`--help` text, and can serialize the *effective* config back to JSON for the ledger.

**Exact locations:**
- [`include/amm/config.hpp`](../include/amm/config.hpp) — the public surface (7 free functions + `CliResult`)
- [`src/config.cpp`](../src/config.cpp) lines 1–197

**The layered-override pattern (the central idea):**
```cpp
CliResult load_configuration(int argc, char** argv) {
    SimulationConfig config;              // layer 1: compiled defaults (member initializers)
    // ... scan argv for --config PATH ...
    if (!config_path.empty()) {
        // ... parse JSON ...
        apply_json(config, root);         // layer 2: JSON overwrites only present keys
    }
    CliResult result{config, false};
    for (int i = 1; i < argc; ++i) { ... result.config.<field> = <parsed arg>; }  // layer 3: CLI wins
    if (!result.show_help) validate(result.config);   // one gate, at the end
    return result;
}
```
Each layer only touches fields it explicitly names, via the `read_if` helper:
```cpp
template <class T> void read_if(const json& j, const char* key, T& value) {
    if (j.contains(key)) value = j.at(key).get<T>();
}
```
So a JSON file with just `{"traders": 4}` changes exactly one field; everything else stays at
its default. CLI flags are applied last, so `--traders 16` always beats the file. This is the
standard precedence order (defaults < file < command line) users expect from real tools.

**`argv` is scanned twice — on purpose.** The first pass looks *only* for `--config` so the
file is loaded before CLI flags are applied; the second pass applies every flag (and skips
`--config`'s value with `else if (option == "--config") ++i;`). If it were one pass, a
`--config` appearing after `--traders` would load the file and clobber the earlier flag.

**`reject_unknown` — fail loud on typos:**
```cpp
void reject_unknown(const json& j, std::initializer_list<std::string_view> allowed, std::string_view where);
```
Called for the root object, the `workload` sub-object, and each `symbols[]` entry. A JSON key
that is not in the allow-list throws `std::invalid_argument("unknown configuration key ...")`.
Without this, `{"treaders": 100}` (typo) would silently run with 8 traders and the user would
never know. The test `configuration rejects unknown keys and invalid markets` locks this in
with `{"mystery": 1}`. The CLI parser does the same via its terminal `else throw
std::invalid_argument("unknown option: " + option)`.

**Scaled-price conversion — the numeric edge:**
```cpp
Price parse_price(double value, std::uint32_t scale) {
    if (!std::isfinite(value) || value <= 0.0 || scale == 0) throw std::invalid_argument(...);
    const long double scaled = static_cast<long double>(value) * scale;
    if (scaled > (long double)std::numeric_limits<Price>::max()) throw std::out_of_range(...);
    return static_cast<Price>(std::llround(scaled));
}
double display_price(Price value, std::uint32_t scale) {
    return double(value) / double(scale);
}
```
`double` is allowed *here* because this is the boundary where human-facing decimal input is
converted to the internal integer, once, before any arithmetic. Points to note:
- `long double` for the intermediate multiply reduces rounding before the round-to-nearest.
- `std::llround` (round half away from zero) rather than a truncating cast, so `9.999 * 100`
  becomes `1000`, not `999`.
- Explicit `isfinite` / `<= 0` / overflow checks — a `NaN` or `inf` in the config would
  otherwise propagate into the price function and blow past every bound check silently.

**`validate` — the single gate:**
One function, ~25 lines, throws `std::invalid_argument` / `std::out_of_range` on the first bad
value. Highlights that show the intent:
- `orders_per_trader == UINT32_MAX` is rejected because the producer loop runs
  `for (sequence = 1; sequence <= orders_per_trader; ++sequence)` — `UINT32_MAX` would make
  `sequence <= max` always true after wraparound → infinite loop. This is a genuine off-by-one
  landmine, defused by validation.
- `traders > INT32_MAX` and `max_quantity > INT64_MAX` are rejected because those values are
  stored in **signed** SQLite integer columns; the check keeps the DB representable.
- Probabilities must be in `[0,1]`; `limit_price_band` and `max_deviation` must be in the
  *open* interval `(0,1)` (a band of `0` generates degenerate limits, `1` lets the price hit
  zero).
- Tickers must be non-empty and **unique** — enforced with a `std::set<std::string>` and
  `.insert(...).second`. Duplicate tickers would collide in the engine's
  `unordered_map<string, Market>`.

Validation runs on both the JSON path and the CLI path because `load_configuration` always
calls it last (unless `--help`). The test constructs a `SimulationConfig` with
`max_deviation = 1.0` and asserts `validate` throws.

**`effective_config_json`:**
Serializes the *fully resolved* config (after all three layers) to a compact JSON string. The
ledger stores this verbatim in `runs.config_json`, so a `.db` file is self-describing: you can
always recover exactly what parameters produced it, including defaults the user never typed.
Prices are written back through `display_price`, so the stored JSON is human-readable decimals,
symmetric with the input format.

**`CliResult { SimulationConfig config; bool show_help; }`:**
`--help` must short-circuit *before* validation (you should be able to run `amm-sim --help`
with no valid config). Returning a struct with a `show_help` flag lets `main` decide, rather
than having the parser call `exit()` or print from deep in a library function.

**Complexity:** linear in `argc` + JSON size; negligible.

**Benefits:**
- Predictable precedence (defaults < file < CLI) and a single validation choke point.
- Typos fail loudly instead of silently running the wrong scenario.
- The `.db` embeds its own provenance via `effective_config_json`.
- Numeric edge cases (`NaN`, overflow, the `UINT32_MAX` loop bug) are caught before threads start.

**Drawbacks / tradeoffs accepted:**
- Two `argv` passes and a hand-rolled flag loop — more code than a parser library, but
  zero-dependency and fully controlled.
- `read_if` + `reject_unknown` must be kept in sync with the struct by hand; adding a field
  means editing three places (`struct`, `apply_json`, `effective_config_json`, allow-lists).
- CLI numeric parsing mixes `std::stoull` (with a used-length check) and bare `std::stod`
  (no such check) — a `--market-probability 0.5abc` would parse as `0.5` silently.

**Alternatives considered and rejected:**
1. A CLI library (CLI11, cxxopts) — rejected: keeps the dependency set at three, and the
   layered semantics are easy to get exactly right by hand.
2. Validating inside each setter as values arrive — rejected: cross-field checks
   (`max_quantity >= min_quantity`, unique tickers) need the whole struct; one final gate is
   simpler to reason about.
3. Silently ignoring unknown keys (jsona's default) — rejected: silent misconfiguration is the
   worst failure mode for a reproducibility-focused tool.

**How this connects to what came before:**
Consumes the `types.hpp` structs and their default initializers (§3). Produces the
`SimulationConfig` that `main` (§9) hands to the queue, the engine, and — via
`effective_config_json` — the ledger.

**Check your understanding:**
1. Why does `load_configuration` scan `argv` twice?
2. What concrete bug does rejecting `orders_per_trader == UINT32_MAX` prevent?
3. Why is `double` acceptable in `parse_price` but not in the engine's price arithmetic?
4. What does `runs.config_json` let you do that re-reading the original CLI/JSON would not?
5. Why must ticker uniqueness be checked in `validate` rather than left to the engine?

---

## 5. The bounded MPSC queue — `include/amm/bounded_queue.hpp`

**What it does:**
A header-only, templated, **m**ulti-**p**roducer / **s**ingle-**c**onsumer queue with a fixed
capacity. Producers `push` (blocking when full — *backpressure*); the consumer `pop`s
(blocking when empty). It assigns each element a global `enqueue_sequence`, supports permanent
`close()`, and exposes three metrics (`size`, `high_water_mark`, `blocked_pushes`).

**Exact locations:**
- [`include/amm/bounded_queue.hpp`](../include/amm/bounded_queue.hpp) lines 1–78

**The data structure:**
```cpp
const std::size_t capacity_;
mutable std::mutex mutex_;
std::condition_variable not_empty_;     // consumer waits here
std::condition_variable not_full_;      // producers wait here
std::deque<T> queue_;
bool closed_{};
std::uint64_t next_sequence_{1};
std::size_t high_water_{};
std::uint64_t blocked_pushes_{};
```
One mutex, two condition variables, a `std::deque` as the ring. Two CVs (not one) so that
waking a producer never spuriously wakes another producer and vice versa — `not_full_` is only
notified by `pop`, `not_empty_` only by `push`/`close`.

**`push` — the interesting method:**
```cpp
bool push(T value, const std::atomic_bool& stop_requested) {
    std::unique_lock lock(mutex_);
    if (queue_.size() >= capacity_ && !closed_ && !stop_requested.load(relaxed))
        ++blocked_pushes_;                                   // (1) metric
    not_full_.wait(lock, [&] {                               // (2) wait predicate
        return queue_.size() < capacity_ || closed_
            || stop_requested.load(relaxed);
    });
    if (closed_ || stop_requested.load(relaxed)) return false;   // (3) woke to shut down
    if constexpr (requires(T t) { t.enqueue_sequence; })         // (4) compile-time check
        value.enqueue_sequence = next_sequence_++;
    queue_.push_back(std::move(value));
    if (queue_.size() > high_water_) high_water_ = queue_.size();
    not_empty_.notify_one();
    return true;
}
```
1. **Backpressure metric.** If the caller is about to block (queue full, not shutting down),
   count it. `blocked_pushes` is a direct measure of how often the queue was the bottleneck —
   if it is high, the engine can't keep up and you should raise `queue_capacity` or accept
   that producers are throttled.
2. **`condition_variable::wait(lock, predicate)`** is the correct idiom: it re-checks the
   predicate on every wakeup, so spurious wakeups and lost notifications are handled. The
   predicate is "there is room **OR** we are closed **OR** stop was requested" — the last two
   are escape hatches so a blocked producer cannot deadlock during shutdown.
3. After waking, if the reason was closure/stop rather than room, return `false`. The producer
   loop in `main.cpp` treats `false` as "stop generating."
4. **`if constexpr (requires ...)`** — a C++20 *requires-expression* used as a compile-time
   boolean. "If `T` has a member named `enqueue_sequence`, assign the next sequence number;
   otherwise don't." This lets the same template serve `BoundedMpscQueue<Order>` (gets
   sequencing) and `BoundedMpscQueue<int>` (used in a test, no such member) with zero runtime
   cost and no separate specialization. The sequence is assigned **under the mutex**, so it is
   a true global order even though many producers race to push.

**`pop`:**
```cpp
std::optional<T> pop() {
    std::unique_lock lock(mutex_);
    not_empty_.wait(lock, [&] { return !queue_.empty() || closed_; });
    if (queue_.empty()) return std::nullopt;      // closed and drained -> stop signal
    T value = std::move(queue_.front());
    queue_.pop_front();
    not_full_.notify_one();                        // one producer may proceed
    return value;
}
```
Returning `std::optional<T>` overloads the return value with an end-of-stream signal:
`std::nullopt` means "closed and empty — there will never be more." The engine loop is exactly
`while (auto order = queue_.pop()) process(std::move(*order));`. Crucially, a *closed* queue
still returns buffered elements until drained — closure stops *new* pushes, it does not discard
what is already queued. That is what makes "Ctrl+C drains accepted entries" true.

**`close` and `wake_all`:**
```cpp
void close()    { lock; closed_ = true;  not_empty_.notify_all(); not_full_.notify_all(); }
void wake_all() { lock;                   not_empty_.notify_all(); not_full_.notify_all(); }
```
`close()` is **permanent and idempotent** — there is no `open()`. It notifies *all* waiters on
both CVs so every blocked producer and the consumer re-evaluate their predicates and make
progress (producers return `false`, consumer drains then returns `nullopt`). `wake_all()` is
the same broadcast *without* setting `closed_` — the engine calls it on a persistence failure
to shake loose producers that are blocked on a full queue so they can observe `stop_requested`
and exit, even though the queue itself is being torn down rather than cleanly closed.

**The metrics accessors** are `const` and take the lock (`mutable std::mutex`) so `main` can
read `high_water_mark()` / `blocked_pushes()` after all threads join. `high_water_` records the
peak `queue_.size()` ever observed — a capacity-planning number.

**Why a `std::deque` and a mutex, not a lock-free ring buffer:**
A lock-free MPSC queue (CAS on head/tail indices, a preallocated array) is faster and is what
LMAX Disruptor / folly `MPMCQueue` do. This project chooses the mutex version because:
- It is ~60 lines and obviously correct; the concurrency lesson is *backpressure + shutdown*,
  not lock-free ring math.
- The real throughput ceiling is SQLite (`fsync`-bounded), not queue contention, so the mutex
  never shows up in a profile.
- Blocking `wait` with a predicate cleanly expresses "sleep until room / until closed", which
  is fiddly to do correctly with a spin-based lock-free design.

**Complexity:** `push`/`pop` are O(1) amortized (deque push/pop at the ends). Under contention
they are O(1) plus lock wait time.

**Benefits:**
- Bounded memory: producers cannot outrun the engine and blow up RAM — they block instead.
- `blocked_pushes` + `high_water` quantify the bottleneck.
- Clean shutdown: closure drains, `nullopt` ends the consumer loop, `wake_all` unsticks
  producers on failure.
- One template covers `Order` (sequenced) and `int` (test) via `if constexpr (requires ...)`.

**Drawbacks / tradeoffs accepted:**
- Mutex + CV is slower than lock-free under heavy contention (irrelevant here, would matter at
  10M+ orders/s).
- `std::deque` node allocations vs. a preallocated array.
- MPSC only — `pop` is **not** safe to call from two threads. The type name says so; nothing
  enforces it.
- Global enqueue order is deterministic *given* an interleaving, but the interleaving itself is
  scheduler-dependent — so the run is reproducible per-producer but not globally (see below).

**Alternatives considered and rejected:**
1. Lock-free MPSC ring (Disruptor-style) — rejected: complexity not justified when SQLite is
   the bottleneck.
2. Unbounded queue — rejected: no backpressure; a fast producer burst = unbounded memory.
3. One condition variable for both directions — rejected: causes spurious cross-wakeups
   (producer wakes producer), wasting a lock acquisition each time.
4. Returning `bool` from `pop` with an out-param — rejected: `std::optional<T>` is the modern,
   move-friendly way to say "maybe a value."

**How this connects to what came before:**
Instantiated as `BoundedMpscQueue<Order>` in `main` (§9) with `config.queue_capacity` from
§4. Producers push `Order`s built from §3 types; the engine (§7) pops them.

**Check your understanding:**
1. Why two condition variables instead of one?
2. What does `pop()` returning `std::nullopt` mean, and how does the engine loop use it?
3. Explain `if constexpr (requires(T t) { t.enqueue_sequence; })`. What breaks if you delete it
   and `T` is `int`?
4. Why is `enqueue_sequence` assigned *inside* the locked region rather than by the producer
   before it calls `push`?
5. What is the difference between `close()` and `wake_all()`, and when is each used?
6. `blocked_pushes` is 40% of `generated` on a run. What two knobs would you turn, and what
   does each trade off?

---

## 6. The pricing model — the `Market` class

**What it does:**
Owns all mutable state for **one** symbol: the current reference price, the running executed
buy/sell volume, and the two priority queues of resting limit orders. Provides the closed-form
AMM price function and the price-time-priority logic for pending limits.

**Exact locations:**
- [`include/amm/engine.hpp`](../include/amm/engine.hpp) lines 16–45 (class + comparators)
- [`src/engine.cpp`](../src/engine.cpp) lines 12–77

**The price function:**
```
net_flow = executed_buy_quantity - executed_sell_quantity
pressure = tanh(net_flow / liquidity)
price    = intrinsic_price * (1 + max_deviation * pressure)
```
In code (`projected_price`):
```cpp
const long double net = (long double)buys - (long double)sells;
const long double pressure = std::tanh(net / (long double)config_.liquidity);
const long double calculated = (long double)config_.intrinsic_price *
    (1.0L + (long double)config_.max_deviation * pressure);
// isfinite / >0 / <= Price max checks, then:
return (Price)std::llround(calculated);
```

**Why `tanh`, specifically:**
`tanh` maps `(-inf, +inf) -> (-1, +1)`, smoothly, with slope ~1 near the origin and
saturating tails. That gives exactly the market behavior you want:
- **Bounded.** `pressure` is in `(-1, 1)`, so `price` is strictly inside
  `intrinsic * (1 ± max_deviation)` — with `max_deviation = 0.5`, the price can never leave
  `[intrinsic*0.5, intrinsic*1.5]` no matter how lopsided the flow. No circuit breakers
  needed; the function *cannot* produce a runaway price.
- **Monotonic.** More net buying ⇒ strictly higher price; more net selling ⇒ strictly lower.
  Tested directly (`AMM price is bounded and directionally monotonic`).
- **Diminishing impact.** The first 1,000 shares of net buy pressure move the price a lot;
  the millionth moves it almost nothing (the tail is flat). This mimics real order-book depth
  — you eat through the easy liquidity first.
- **Symmetric.** `tanh(-x) = -tanh(x)`, so buys and sells are mirror images around
  `intrinsic_price`.

`liquidity` is the horizontal scale: `net_flow / liquidity` is the `tanh` argument, so a
larger `liquidity` means it takes proportionally more net volume to reach the same price
level. `ALT` in the example config has half the liquidity of `SIM`, so it is twice as
"jumpy".

**Why `long double` for the intermediate math:**
The inputs are integers, but `tanh` and the multiply are floating-point. `long double`
(80-bit extended on x86) buys extra mantissa bits so the single final `std::llround` back to
`int64_t` is as accurate as possible. Only the *inputs* and the *output* are integers; the
transient is float, and that is fine because it is computed once per fill and immediately
rounded — no accumulation of float error over time (the running volumes `buy_quantity_` /
`sell_quantity_` are exact `uint64_t`).

**`projected_price` vs `apply`:**
```cpp
Price projected_price(OrderSide, Quantity) const;   // "what WOULD the price be" - pure
Price apply(OrderSide side, Quantity quantity) {    // commit: mutate volumes + price
    const Price next = projected_price(side, quantity);
    if (side == Buy) buy_quantity_ += quantity; else sell_quantity_ += quantity;
    current_price_ = next;
    return next;
}
```
`projected_price` is `const` and side-effect-free — it computes the hypothetical price for a
volume delta *without* committing it. The engine uses it to get a **market order's fill
price** (the price *after* its own impact) before calling `apply`. Overflow is checked here:
if adding `quantity` to the running volume would wrap `uint64_t`, or the computed price is
non-finite / negative / beyond `Price` max, it throws `std::overflow_error` — which the engine
treats as fatal.

**The pending-limit priority queues:**
```cpp
std::priority_queue<Order, std::vector<Order>, BuyPriority>  buys_;
std::priority_queue<Order, std::vector<Order>, SellPriority> sells_;
```
```cpp
bool BuyPriority::operator()(const Order& a, const Order& b) const {   // a < b ?  (max-heap: "less" = lower priority)
    if (a.limit_price != b.limit_price) return a.limit_price < b.limit_price;   // higher price = higher priority
    return a.enqueue_sequence > b.enqueue_sequence;                             // earlier arrival = higher priority
}
```
`std::priority_queue` is a **max-heap**: `top()` is the element for which the comparator
returns `false` against all others, i.e. the "greatest". So the comparator is written so that
"greatest" = "should fill first":
- **Buy limits:** highest limit price first (a buyer willing to pay more has priority). This is
  correct because a buy limit is eligible when `current_price <= limit_price` — the highest
  limit is the first to become eligible as the price falls.
- **Sell limits:** *lowest* limit price first (`a.limit_price > b.limit_price` inverts it) — a
  seller asking less has priority, and the lowest ask is first eligible as the price rises.
- **Tie-break:** lower `enqueue_sequence` (arrived earlier) wins. Note the `>` — because the
  heap surfaces the "greatest", you invert the sequence comparison so the *smallest* sequence
  is treated as greatest. This is the "time" in price-time priority. Tested by
  `pending orders use price-time priority`.

`eligible()` is the trigger check:
```cpp
bool Market::eligible(const Order& o) const {
    if (!o.limit_price) return true;                 // market order: always
    return o.side == Buy ? current_price_ <= *o.limit_price
                         : current_price_ >= *o.limit_price;
}
```

**`take_eligible(side)`** peeks `top()`; if the pool is non-empty *and* the top is eligible at
the current price, it pops and returns it, else `std::nullopt`. Because the top is by
construction the "most likely to be eligible" order, one `eligible(top())` check decides
whether *any* order on that side can fire. **`take_all_pending()`** drains both heaps into a
vector — used at shutdown to expire everything left.

**Why a heap and not a sorted `std::map<Price, queue<Order>>` (a real book):**
A price-level map is what a matching engine uses because it needs range scans, level
aggregation, and O(1) access to best-bid/best-ask. This model never needs any of that: it only
ever asks "is the single best resting order on this side eligible?" A binary heap answers that
in O(1) peek / O(log n) pop, with one contiguous `std::vector` backing it (cache-friendly, no
per-node allocation). The tradeoff: you *cannot* efficiently cancel an arbitrary order or
iterate levels — neither of which this project does.

**Complexity (per symbol):**
- `projected_price` / `apply` / `eligible`: O(1) (one `tanh`, a few multiplies).
- `pend`: O(log n) heap push.
- `take_eligible`: O(1) peek, O(log n) pop when it fires.
- `take_all_pending`: O(n log n) to drain.

**Benefits:**
- The price is *provably* bounded and monotonic — no runaway, no circuit breaker logic.
- Diminishing marginal impact without modeling a book.
- O(1) pricing; heap-backed pending pool is allocation-light and cache-friendly.
- Exact running volumes (`uint64_t`); float only in a transient that is immediately rounded.

**Drawbacks / tradeoffs accepted:**
- Not a real book: no depth, no spread, no partial fills, no arbitrary cancel.
- `std::priority_queue` gives no iteration and no efficient removal — fine because the model
  never needs them.
- The price depends only on cumulative net flow, so it is **path-independent**: 10 buys of 100
  then 10 sells of 100 returns to exactly `intrinsic_price`, which a real market would not.
- `tanh` on `long double` is not bit-identical across compilers/libms, so final prices can
  differ in the last integer digit between platforms.

**Alternatives considered and rejected:**
1. `price = intrinsic * (1 + k * net_flow / liquidity)` (linear) — rejected: unbounded, needs
   explicit clamping, and clamping introduces a non-smooth kink.
2. Constant-product AMM (`x*y=k`, Uniswap-style) — rejected: models a two-asset pool with real
   reserves; heavier and less aligned with "reference price nudged by flow."
3. `std::map<Price, std::deque<Order>>` pending book — rejected: pays for range/iteration
   capability the model never uses.
4. `std::multiset` keyed by a priority tuple — rejected: `priority_queue` is a tighter fit for
   "only ever touch the best one".

**How this connects to what came before:**
`Market` is constructed from a `SymbolConfig` (§3/§4). It is a member of the engine's
`unordered_map<string, Market>` (§7), which is the sole owner — no locking inside `Market`
because only the engine thread ever calls it.

**Check your understanding:**
1. Why does `tanh` give you a bounded price for free, and what is the exact interval the price
   is confined to?
2. What does `liquidity` control geometrically in the `tanh` curve?
3. Why does the engine call `projected_price` before `apply` for a market order, but not for a
   resting limit?
4. `std::priority_queue` is a max-heap. Explain how `BuyPriority` is written so that `top()` is
   the order that should fill first.
5. Why is the tie-break comparison `a.enqueue_sequence > b.enqueue_sequence` (`>`, not `<`)?
6. The price is path-independent. Give a sequence of orders that a real market would price
   differently from this model, and say why.

---

## 7. The engine — `include/amm/engine.hpp` + `src/engine.cpp`

**What it does:**
The single consumer thread. Drains the queue, routes each order (market → immediate execute;
limit → execute-if-eligible or rest), runs the post-fill cascade, and at shutdown expires all
resting orders, checks invariants, writes per-symbol summaries, and finalizes the ledger.
Turns any exception into a clean failure: set flags, roll back the DB, close the queue.

**Exact locations:**
- [`include/amm/engine.hpp`](../include/amm/engine.hpp) lines 47–69
- [`src/engine.cpp`](../src/engine.cpp) lines 79–200

**`run()` — the lifecycle and the error boundary:**
```cpp
RunMetrics Engine::run() {
    try {
        ledger_ = std::make_unique<Ledger>(config_.database_path, config_.commit_batch_size,
                                           effective_config_json(config_));
        while (auto order = queue_.pop()) process(std::move(*order));   // main loop
        expire_pending();                                               // resting -> EXPIRED
        verify_invariants();                                            // self-check
        for (const auto& [ticker, market] : markets_) {                 // final summaries
            SymbolMetrics item{ticker, market.current_price(), market.config().price_scale,
                               market.buy_quantity(), market.sell_quantity()};
            metrics_.symbols.push_back(item);
            ledger_->record_symbol_summary(item);
        }
        metrics_.transactions = ledger_->transaction_count() + 1;
        ledger_->finish(stop_requested_ ? "INTERRUPTED" : "COMPLETED");
        metrics_.interrupted = stop_requested_.load(relaxed);
    } catch (const std::exception& error) {
        metrics_.failed = true;
        metrics_.error  = error.what();
        fatal_error_.store(true, relaxed);
        stop_requested_.store(true, relaxed);      // <- makes producers stop
        if (ledger_) ledger_->rollback();          // <- drop the uncommitted batch
        queue_.close();                            // <- unblock producers
    }
    return metrics_;
}
```
The entire body is one `try`. Any `std::exception` from anywhere — a bad symbol, a price
overflow, a SQLite error — lands in one handler that:
1. records the message into `RunMetrics`,
2. sets `fatal_error_` (checked by `main`'s exit code) and `stop_requested_` (checked by
   producers' generation loop),
3. rolls back the open transaction (so a half-written batch never persists), and
4. closes the queue so any producer blocked on a full queue wakes and exits.

This is the "a persistence failure invalidates the success guarantees" line from
`concurrency-invariants.md`, in code.

**`process(order)` — routing:**
```cpp
void Engine::process(Order order) {
    ++metrics_.processed; ++metrics_.accepted;
    auto it = markets_.find(order.symbol);
    if (it == markets_.end()) throw std::invalid_argument("unknown order symbol: " + order.symbol);
    if (order.quantity == 0)  throw std::invalid_argument("zero-quantity order");

    if (order.type == OrderType::Market) {
        if (order.limit_price) throw std::invalid_argument("market order has a limit price");
        ++metrics_.market_orders;
        ledger_->record_order(order, OrderStatus::Accepted);
        execute(std::move(order), true);
    } else {
        if (!order.limit_price || *order.limit_price <= 0) throw std::invalid_argument("limit order has invalid price");
        ++metrics_.limit_orders;
        if (it->second.eligible(order)) {
            ledger_->record_order(order, OrderStatus::Accepted);
            execute(std::move(order), false);
        } else {
            ledger_->record_order(order, OrderStatus::Pending);
            it->second.pend(std::move(order));
        }
    }
}
```
Note the ordering: the order is **written to the ledger first** (`ACCEPTED` or `PENDING`),
*then* executed or pended. So the DB always has a row for an order before it can have a fill
that references it — the `executions.order_id` foreign key can never dangle. `accepted` and
`processed` are incremented together and later asserted equal (a redundancy check that catches
a missed increment).

**`execute(order, market_order)`:**
```cpp
void Engine::execute(Order order, bool market_order) {
    Market& market = markets_.at(order.symbol);
    const Price old_price  = market.current_price();
    const Price fill_price = market_order ? market.projected_price(order.side, order.quantity)
                                          : old_price;
    market.apply(order.side, order.quantity);
    const std::uint64_t sequence = ++engine_sequence_;
    Execution execution{next_execution_id_++, order.id, order.symbol, order.side,
                        order.quantity, fill_price, sequence};
    ledger_->record_execution(execution);
    ledger_->update_status(order.id, OrderStatus::Filled, sequence);
    ++metrics_.filled; ++metrics_.executions;
    cascade(market, old_price);
}
```
The fill-price rule is the subtle bit:
- A **market order** fills at `projected_price(side, qty)` — the price *including its own
  impact*. It is the aggressor; it pays the price it moves to.
- A **resting limit** that becomes eligible fills at `old_price` — the current reference price
  *before* its own volume is applied. The limit order "was already there" at a price the
  market came to; it does not pay its own impact. (Its volume still moves the price via
  `apply`, which can then trigger the cascade.)

`engine_sequence_` and `next_execution_id_` are plain non-atomic members — only this thread
touches them.

**`cascade(market, old_price)` — the chain reaction:**
```cpp
void Engine::cascade(Market& market, Price old_price) {
    if (market.current_price() == old_price) return;
    OrderSide triggered = market.current_price() > old_price ? OrderSide::Sell : OrderSide::Buy;
    while (true) {
        auto next = market.take_eligible(triggered);
        if (!next) {                                    // check the other side too
            const OrderSide opposite = triggered == Buy ? Sell : Buy;
            next = market.take_eligible(opposite);
        }
        if (!next) return;                              // stable: nothing eligible either side
        old_price = market.current_price();
        const Price fill_price = old_price;             // resting fill = pre-impact price
        market.apply(next->side, next->quantity);
        if      (market.current_price() > old_price) triggered = OrderSide::Sell;
        else if (market.current_price() < old_price) triggered = OrderSide::Buy;
        const std::uint64_t sequence = ++engine_sequence_;
        ledger_->record_execution({next_execution_id_++, next->id, next->symbol, next->side,
                                   next->quantity, fill_price, sequence});
        ledger_->update_status(next->id, OrderStatus::Filled, sequence);
        ++metrics_.filled; ++metrics_.executions;
    }
}
```
Mechanics and the reasoning:
- A fill moved the price. If it went **up**, sell limits may now be eligible (`current >=
  ask`); if **down**, buy limits. `triggered` names that side.
- Each iteration fills the single best eligible order on the triggered side, applies its
  volume, and re-evaluates. This can flip the direction: a large resting sell can push the
  price back down far enough to make *buy* limits eligible. The comment in the source calls
  this out — after each fill the code recomputes `triggered` from the new vs. old price.
- **The opposite-side re-check** (lines 157–160) is a real correctness fix, not defensive
  padding: a fill can reverse the price without exhausting the previous side's eligible
  orders. Only when *neither* side has an eligible top is the market stable and the loop
  returns.
- **It is a loop, not recursion.** A recursive `execute → cascade → execute` could blow the
  stack on a long chain (thousands of stacked limits). The iterative form is O(fills) time,
  O(1) stack.
- **Termination:** every iteration removes one order from a finite pending pool and never adds
  one, so the loop runs at most "number of resting orders" times.

**`expire_pending()`:** after the queue is drained, every order still resting never triggered.
`take_all_pending()` drains both heaps per symbol and each order gets
`update_status(id, Expired, ++engine_sequence_)`. This is why the queue's "closed but not
empty still drains" behavior matters — expiry only runs once the last real order is processed.

**`verify_invariants()` — the built-in audit:**
```cpp
if (accepted != processed)                 throw;   // every accepted order was processed
if (accepted != filled + expired)          throw;   // every order reached a terminal state
if (filled != executions)                  throw;   // one execution per fill (no partials here)
for each market:
    if (pending_count() != 0)              throw;   // nothing left resting
    if (price outside [floor(intrinsic*(1-dev)), ceil(intrinsic*(1+dev))]) throw;   // bound held
```
This runs on the happy path *before* `finish()`. It converts "we believe the model is
consistent" into "the process exits non-zero if it ever wasn't." The price-bound check is the
runtime counterpart to the mathematical argument that `tanh` keeps the price bounded — belt
and suspenders.

**Construction:**
```cpp
Engine::Engine(const SimulationConfig& config, BoundedMpscQueue<Order>& queue,
               std::atomic_bool& stop_requested, std::atomic_bool& fatal_error)
    : config_(config), queue_(queue), stop_requested_(stop_requested), fatal_error_(fatal_error) {
    for (const auto& symbol : config.symbols) markets_.emplace(symbol.ticker, Market(symbol));
}
```
The engine holds **references** to the queue and both atomics — they are owned by `main` and
outlive the engine. `markets_` is built once up front so `process` can assume every configured
symbol has a `Market` (an order for an unconfigured symbol is a hard error, not a lazy
insert). The `Ledger` is created inside `run()` (not the constructor) so that the potentially
failing "open the database" step happens on the engine thread inside the `try`, where its
exception becomes a clean `metrics_.failed` rather than escaping a constructor on the main
thread.

**Complexity:** O(1) amortized per market order; O(log n) to rest a limit; the cascade is
O(k log n) for `k` triggered fills. Overall O(total_orders · log n) for the run.

**Benefits:**
- One `try` around the whole lifecycle = one place shutdown/rollback logic lives.
- Ledger-before-execute ordering keeps foreign keys valid at all times.
- Iterative cascade: bounded stack, provable termination.
- `verify_invariants` makes silent accounting bugs into loud non-zero exits.
- Single-threaded engine ⇒ `engine_sequence_`, `next_execution_id_`, all counters need no locks.

**Drawbacks / tradeoffs accepted:**
- Throughput is capped by one core and by SQLite; no per-symbol parallelism.
- No partial fills: a fill always consumes the whole order, so `filled == executions` exactly.
- Cascade fill price is the pre-impact reference price for *every* order in the chain — a
  modeling choice, not a market microstructure result.
- `markets_.at(order.symbol)` in `execute` does a second string hash after `process` already
  did `markets_.find` — minor redundant work.

**Alternatives considered and rejected:**
1. Recursive cascade — rejected: unbounded stack on long chains.
2. Lazy `markets_[symbol]` insertion — rejected: an order for an unknown symbol should fail,
   not silently create a market at default parameters.
3. Creating the `Ledger` in the constructor — rejected: moves a failing I/O step off the
   engine thread and outside the `try`.
4. Per-symbol engine threads — rejected: multiplies the concurrency surface (N DB writers or a
   shared locked one) for a simulator that does not need the throughput.
5. Skipping `verify_invariants` in release builds — rejected: it is cheap and it is the whole
   point of a *simulator* (trust the numbers).

**How this connects to what came before:**
Pops `Order`s from the queue (§5), prices them through `Market` (§6), writes through `Ledger`
(§8). Reads `stop_requested_` (set by the signal handler in §9) and writes `fatal_error_`
(read by §9 for the exit code). Uses `effective_config_json` from §4.

**Check your understanding:**
1. Why is an order written to the ledger *before* it is executed or pended?
2. A market buy and a resting buy limit both fill. Why do they fill at different prices?
3. Walk through why the cascade must re-check the *opposite* side before deciding the market
   is stable. Construct an order sequence where skipping that check loses a fill.
4. Why is the cascade a `while` loop and not recursion? What is the worst case it protects
   against?
5. Why is the `Ledger` constructed in `run()` rather than in the `Engine` constructor?
6. `verify_invariants` asserts `filled == executions`. In what kind of engine would that
   *not* hold, and why does it hold here?
7. On an exception, `run()` calls `stop_requested_.store(true)` and `queue_.close()`. What
   does each one unblock, and what goes wrong if you omit either?

---

## 8. The SQLite ledger — `include/amm/ledger.hpp` + `src/ledger.cpp`

**What it does:**
The engine thread's exclusive interface to SQLite. Opens/creates the database, defines the
schema, inserts a `runs` row for this run, and provides prepared-statement methods to record
orders, status transitions, executions, and symbol summaries — committing in fixed-size
batches and once more at the end. Provides `rollback()` for the failure path. Cleans up all
SQLite resources in the destructor.

**Exact locations:**
- [`include/amm/ledger.hpp`](../include/amm/ledger.hpp) lines 1–48
- [`src/ledger.cpp`](../src/ledger.cpp) lines 1–185

**The schema (created with `CREATE TABLE IF NOT EXISTS`):**
```
runs(id PK, started_at, finished_at, status, config_json)
orders(run_id, order_id, producer_id, producer_sequence, enqueue_sequence,
       symbol, side, type, quantity, limit_price NULL, status, terminal_engine_sequence NULL,
       PRIMARY KEY(run_id, order_id), FOREIGN KEY(run_id) -> runs(id))
executions(run_id, execution_id, order_id, symbol, side, quantity, price, engine_sequence,
       PRIMARY KEY(run_id, execution_id),
       FOREIGN KEY(run_id, order_id) -> orders(run_id, order_id))
symbol_summaries(run_id, symbol, final_price, price_scale, buy_quantity, sell_quantity,
       PRIMARY KEY(run_id, symbol), FOREIGN KEY(run_id) -> runs(id))
```
- **Every table is scoped by `run_id`.** The same `.db` file accumulates many runs; a query
  always filters by run. `orders` is keyed `(run_id, order_id)` — `order_id` is only unique
  *within* a run (it is the bit-packed producer/sequence value, which repeats run to run).
- **`executions` has a compound foreign key `(run_id, order_id)` into `orders`.** With
  `PRAGMA foreign_keys=ON`, SQLite refuses an execution row whose order was never inserted —
  which is exactly why the engine writes the order row first (§7).
- **`limit_price` and `terminal_engine_sequence` are nullable.** Market orders have no limit
  price; an order that has not reached a terminal state yet has no terminal sequence. (If the
  process crashes mid-run, that is how you spot the orders that never finished.)
- `config_json` on `runs` is the `effective_config_json` string — self-describing runs.

**The PRAGMAs — the performance/durability dial:**
```cpp
execute("PRAGMA foreign_keys=ON");     // enforce the FKs above (off by default in SQLite!)
execute("PRAGMA journal_mode=WAL");    // write-ahead log
execute("PRAGMA synchronous=NORMAL");  // fsync at checkpoints, not every commit
```
- **WAL** (write-ahead logging): writers append to a `-wal` sidecar file instead of rewriting
  pages in place. Commits are cheap (append + fsync the wal), and the main db file is updated
  in bulk at checkpoints. This is the standard "I do lots of small transactions" SQLite mode.
- **`synchronous=NORMAL`** with WAL means SQLite fsyncs the wal at each checkpoint but not on
  every single `COMMIT`. The documented consequence, restated in the README: *"A process crash
  can lose the current uncommitted batch"* — and, with `NORMAL`, potentially the last committed
  transaction on an OS/power crash (not just an app crash). The project accepts that: it is a
  simulator, and the speed is worth it. `synchronous=FULL` would be the choice if every
  committed order had to survive a power cut.

**Prepared statements — compile the SQL once:**
```cpp
insert_order_     = prepare("INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL)");
update_order_     = prepare("UPDATE orders SET status=?, terminal_engine_sequence=? WHERE run_id=? AND order_id=?");
insert_execution_ = prepare("INSERT INTO executions VALUES(?,?,?,?,?,?,?,?)");
insert_summary_   = prepare("INSERT INTO symbol_summaries VALUES(?,?,?,?,?,?)");
```
Each is `sqlite3_prepare_v2`'d **once** in the constructor. Per event the code only does
`sqlite3_bind_*` + `sqlite3_step` + `sqlite3_reset` + `sqlite3_clear_bindings` (the `reset`
helper). This skips the SQL parser/planner on every one of potentially millions of inserts —
the single biggest SQLite throughput lever after transaction batching.

`sqlite3_bind_text(..., SQLITE_TRANSIENT)` vs `SQLITE_STATIC`: `TRANSIENT` tells SQLite to
copy the string immediately (used for `symbol`, whose `std::string` may not outlive the step);
`STATIC` promises the pointer stays valid (used for the `to_string(enum)` results, which are
string literals with static storage). Getting this wrong is a classic use-after-free.

**Transaction batching:**
```cpp
void begin()  { execute("BEGIN IMMEDIATE"); transaction_open_ = true; events_in_batch_ = 0; }
void commit(bool reopen) {
    if (!transaction_open_) return;
    execute("COMMIT"); transaction_open_ = false; ++transaction_count_; events_in_batch_ = 0;
    if (reopen) begin();
}
void count_event() { if (++events_in_batch_ >= batch_size_) commit(true); }
```
Every `record_*` / `update_status` call ends with `count_event()`. After `commit_batch_size`
events (default 1000), the current transaction commits and a fresh one opens. Committing every
row individually would fsync ~1000× more often; wrapping the *entire* run in one transaction
would hold a write lock forever and lose everything on a crash. Batching is the middle ground,
and `commit_batch_size` is the exposed knob.

**`BEGIN IMMEDIATE`** (not plain `BEGIN`): acquires the write lock *now* rather than lazily on
the first write. Since this connection is the only writer and will definitely write, taking
the lock eagerly avoids a `SQLITE_BUSY` surprise later.

**RAII and the failure path:**
```cpp
Ledger::Ledger(...) {
    if (sqlite3_open_v2(...) != SQLITE_OK) { ...close...; throw std::runtime_error(...); }
    try {
        ...pragmas, schema, insert runs row, prepare statements...
        begin();
    } catch (...) {                 // constructor partially succeeded
        finalize_all();             // free any statements already prepared
        sqlite3_close(db_); db_ = nullptr;
        throw;                      // -> becomes metrics_.failed in Engine::run
    }
}
Ledger::~Ledger() {
    if (!db_) return;
    if (transaction_open_) rollback();   // never leave a dangling transaction
    finalize_all();
    sqlite3_close(db_);
}
void Ledger::rollback() noexcept {       // safe to call from the catch handler
    if (db_ && transaction_open_) { sqlite3_exec(db_, "ROLLBACK", ...); transaction_open_ = false; }
}
```
- The constructor's inner `try/catch` handles *partial construction* — if `prepare()` #3
  throws, statements #1 and #2 are already allocated and must be finalized before rethrow, or
  they leak.
- `rollback()` is `noexcept` because it runs from `Engine::run`'s catch block and during
  destruction — throwing from there would call `std::terminate`.
- The destructor rolls back any still-open transaction: if the engine throws *between*
  `begin()` and the next `commit()`, `~Ledger` (via the `unique_ptr` reset) discards that
  partial batch. Combined with `Engine::run` explicitly calling `ledger_->rollback()`, an
  uncommitted batch after a failure never reaches the database.

**`finish(status)`:**
```cpp
void Ledger::finish(const std::string& status) {
    commit(false);                                  // final flush, do NOT reopen
    // UPDATE runs SET status=?, finished_at=CURRENT_TIMESTAMP WHERE id=?
}
```
Called by the engine on the happy path after summaries are written. It commits the last
partial batch and stamps the `runs` row `COMPLETED` or `INTERRUPTED` with a finish timestamp.
A run left `RUNNING` in the table is therefore a crash marker.

**`Ledger(const Ledger&) = delete;`** — it owns a raw `sqlite3*` and four
`sqlite3_stmt*`; copying would double-free. Non-copyable, held by `std::unique_ptr` in the
engine.

**Complexity:** O(1) per record (bind + step on a prepared statement); a `COMMIT`/`fsync`
every `batch_size` events amortizes to O(1/batch_size) syncs per event.

**Benefits:**
- Prepared statements + batched transactions + WAL = the three standard SQLite throughput
  levers, all applied.
- Foreign keys on ⇒ the DB structurally cannot hold an execution for a non-existent order.
- RAII (constructor try/catch, `noexcept` rollback, destructor rollback) ⇒ no leaked handles,
  no dangling transaction, no half-batch survives a failure.
- Self-describing: `config_json` + `started_at`/`finished_at`/`status` per run.

**Drawbacks / tradeoffs accepted:**
- `synchronous=NORMAL`: the last committed transaction can be lost on an OS/power crash; the
  in-flight batch is always at risk on any crash. Explicitly documented, not a bug.
- Single writer only — no concurrent readers expected during a run (though WAL would allow
  them).
- Hand-written bind/step/reset for every column: verbose and easy to get an index wrong
  (`VALUES(?,?,...)` positions are 1-based and must match the struct field order exactly).
- `SQLITE_OPEN_FULLMUTEX` is requested even though only one thread uses the handle — harmless
  but unnecessary serialization if it were ever shared.

**Alternatives considered and rejected:**
1. Commit per row — rejected: ~1000× more fsyncs, throughput collapse.
2. One transaction for the whole run — rejected: total data loss on any crash, long-held
   write lock.
3. `synchronous=FULL` — rejected for a simulator; correct for real durability needs.
4. An ORM / query builder — rejected: a dependency and an abstraction layer over four
   `INSERT`s.
5. Writing from multiple threads with a shared mutex — rejected: violates the single-writer
   principle the whole design rests on.

**How this connects to what came before:**
Constructed inside `Engine::run` (§7) with `config.database_path`, `config.commit_batch_size`
(§4) and the `effective_config_json` string (§4). Every `record_*` call is fed `types.hpp`
structs (§3). Its exceptions are caught by the engine's one `try`.

**Check your understanding:**
1. Why does the engine insert the `orders` row before the `executions` row — what enforces
   that it must?
2. What do `journal_mode=WAL` and `synchronous=NORMAL` each buy you, and what is the exact
   durability cost stated in the README?
3. Why is each `sqlite3_stmt` prepared once in the constructor instead of per call?
4. When is `SQLITE_TRANSIENT` required for `sqlite3_bind_text` and when is `SQLITE_STATIC`
   safe? What bug does the wrong choice cause?
5. Why must `rollback()` be `noexcept`?
6. The constructor has an inner `try/catch` that calls `finalize_all()` and rethrows. What
   leaks if it is removed?
7. A `.db` file has a `runs` row still marked `RUNNING`. What happened?

---

## 9. Orchestration — `src/main.cpp`

**What it does:**
The composition root. Parses config, installs the SIGINT handler, constructs the queue / engine
/ atomics, spawns one consumer thread and *N* producer threads, joins them in the right order,
fills in the run-level metrics, prints the report, and returns the process exit code.

**Exact locations:**
- [`src/main.cpp`](../src/main.cpp) lines 1–128

**The signal handler — what you may and may not do in one:**
```cpp
namespace { std::atomic_bool global_stop_requested{false}; }
extern "C" void handle_interrupt(int) {
    global_stop_requested.store(true, std::memory_order_relaxed);
}
...
std::signal(SIGINT, handle_interrupt);
```
A signal handler runs asynchronously on some thread at an arbitrary instruction boundary. The
only safe things to do are set a flag of type `volatile sig_atomic_t` or `std::atomic` (that
is lock-free) and return. This handler does exactly one store and nothing else — **no
`malloc`, no `std::cout`, no mutex, no SQLite.** Everything reacts to the flag cooperatively:
producers check `stop.load()` each iteration and in the queue's wait predicate; the engine
reads it to label the run `INTERRUPTED`. `concurrency-invariants.md` states this as a hard
rule. `extern "C"` because `std::signal` expects a C-linkage function pointer.

**`splitmix64` — per-producer seeding:**
```cpp
std::uint64_t splitmix64(std::uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31U);
}
...
std::mt19937_64 rng(splitmix64(config.seed ^ producer_id));
```
Each producer needs its *own* RNG (a shared one would need a lock and would serialize
generation). Naively seeding producer *k* with `seed ^ k` is dangerous: `mt19937` seeded with
nearby integers produces correlated early output. `splitmix64` is a fast bijective bit-mixer
(the same one libstdc++ uses inside `std::seed_seq`-free seeding) that scrambles
`seed ^ producer_id` into a well-distributed 64-bit value first. Result: **the same
`--seed` reproduces each producer's exact payload stream**, independent of how many producers
there are. (Global *interleaving* still depends on the scheduler — see §5.)

**The producer function:**
```cpp
void produce(producer_id, config, queue, stop, generated) {
    std::mt19937_64 rng(splitmix64(config.seed ^ producer_id));
    std::bernoulli_distribution   market(config.market_order_probability);
    std::bernoulli_distribution   buy(config.buy_probability);
    std::uniform_int_distribution quantity(config.min_quantity, config.max_quantity);
    std::uniform_int_distribution symbol_index(0, config.symbols.size() - 1);
    std::uniform_real_distribution offset(-config.limit_price_band, config.limit_price_band);

    for (sequence = 1; sequence <= config.orders_per_trader; ++sequence) {
        if (stop.load(relaxed)) break;
        const auto& symbol = config.symbols[symbol_index(rng)];
        Order order;
        order.id = ((uint64_t)(producer_id + 1) << 32) | sequence;   // provenance-encoded id
        order.producer_id = producer_id;
        order.producer_sequence = sequence;
        order.symbol = symbol.ticker;
        order.side = buy(rng)    ? Buy    : Sell;
        order.type = market(rng) ? Market : Limit;
        order.quantity = quantity(rng);
        if (order.type == Limit) {
            const double base = display_price(symbol.intrinsic_price, symbol.price_scale);
            order.limit_price = parse_price(base * (1.0 + offset(rng)), symbol.price_scale);
        }
        generated.fetch_add(1, relaxed);
        if (!queue.push(std::move(order), stop)) break;   // false = queue closed / stopping
    }
}
```
- All distributions are constructed once, outside the loop.
- Limit prices are generated as `intrinsic ± up to limit_price_band` (a fraction), centered on
  the **immutable configured** `intrinsic_price` — never on the live market price. This is a
  core invariant: *no producer reads mutable market state*, so producers never need to
  synchronize with the engine.
- `generated` is a `std::atomic<uint64_t>` shared by all producers (incremented `relaxed` —
  we only need the final total, not ordering). It is the one counter `main` reads to fill
  `metrics.generated` after the join.
- `push` returning `false` (queue closed or stop requested) breaks the loop — the producer
  exits promptly on shutdown instead of finishing its quota.

**Thread orchestration and join order:**
```cpp
std::thread consumer([&] { metrics = engine.run(); });
try {
    for (id = 0; id < config.traders; ++id)
        producers.emplace_back(produce, id, cref(config), ref(queue), ref(global_stop_requested), ref(generated));
} catch (...) {                       // thread creation itself failed (resource exhaustion)
    global_stop_requested.store(true);
    queue.close();
    for (auto& p : producers) p.join();
    consumer.join();
    throw;
}
for (auto& p : producers) p.join();   // 1. wait for every producer to finish generating
queue.close();                        // 2. now no more pushes will ever happen
consumer.join();                      // 3. engine drains the remainder, expires, commits
```
The order is load-bearing:
1. **Join producers first.** Only once every producer has stopped can you know the queue will
   receive no further pushes.
2. **Then `close()`.** This flips the consumer's `pop()` from "block for more" to "drain what's
   left, then return `nullopt`".
3. **Then join the consumer.** `Engine::run`'s `while (pop())` loop ends when the queue is
   closed and empty; it then runs `expire_pending`, `verify_invariants`, writes summaries,
   `finish()`s the ledger, and returns `metrics`.

If you `close()` before joining producers, a producer mid-`push` gets `false` and drops an
order it already counted in `generated` — `generated != accepted` and the invariant check
would fire. The `catch (...)` around thread creation handles the rare case where spawning the
Nth `std::thread` throws (OS thread limit): it tears everything down cleanly and rethrows.

**Post-run metrics and exit code:**
```cpp
metrics.generated        = generated.load(relaxed);
metrics.queue_high_water = queue.high_water_mark();
metrics.blocked_pushes   = queue.blocked_pushes();
metrics.elapsed_seconds  = duration<double>(steady_clock::now() - started).count();
print_report(metrics, config);
return metrics.failed || fatal_error.load(relaxed) ? 1 : 0;
```
`main` fills in the numbers that only it can see (queue metrics after join, wall-clock via
`steady_clock` — the monotonic clock, correct for measuring elapsed time). Exit codes:
`0` success (including a clean Ctrl+C — an interrupted run still committed everything it
accepted), `1` a run/persistence failure, `2` a config or startup exception (the outer
`catch (const std::exception&)` that also prints `--help`).

**`print_report`** writes the human summary: the funnel (`generated → accepted → processed →
filled/expired`), the market/limit split, queue high-water and blocked pushes, transaction
count, elapsed seconds and derived `orders/second`, and one line per symbol with the final
price (via `display_price`) and cumulative buy/sell quantity.

**Complexity:** spawns `traders + 1` threads; the process is O(total_orders) work overall,
wall-time gated by the engine + SQLite.

**Benefits:**
- Async-signal-safe shutdown: one atomic store, everything else cooperative.
- Deterministic per-producer streams via `splitmix64`-mixed seeds.
- Provenance-encoded order ids: no shared id counter, and the id tells you who made it.
- Join-then-close-then-join guarantees `generated == accepted` on a clean run.
- `steady_clock` for timing; correct exit codes for the three outcome classes.

**Drawbacks / tradeoffs accepted:**
- Global mutable `global_stop_requested` at namespace scope — required because a signal
  handler cannot take a capture, but it is still global state.
- One `std::atomic` increment (`generated`) per generated order across all producers — mild
  true sharing on one cache line; negligible next to the queue push.
- Producer quota (`orders_per_trader`) is per-thread, so total work scales with `traders` —
  changing thread count changes the workload size, not just its parallelism.

**Alternatives considered and rejected:**
1. Doing real work in the signal handler (flushing, logging) — rejected: not async-signal-safe,
   can deadlock or corrupt.
2. `seed ^ producer_id` straight into `mt19937_64` — rejected: correlated streams across
   producers.
3. A shared locked RNG — rejected: serializes generation, the thing producers exist to
   parallelize.
4. `close()` before joining producers — rejected: drops counted-but-unpushed orders, breaks
   the accounting invariant.
5. `system_clock` for elapsed time — rejected: not monotonic (NTP steps, DST) — `steady_clock`
   is the right tool.

**How this connects to what came before:**
Ties every prior section together: config (§4) → queue (§5) + engine (§7) construction →
producers building `Order`s (§3) with `parse_price` (§4) → engine pricing (§6) and persistence
(§8). Owns the atomics the engine and signal handler communicate through, and the invariants
doc (§10) is the specification this file implements.

**Check your understanding:**
1. List everything the SIGINT handler is allowed to do, and why it may not print or lock.
2. Why is `seed ^ producer_id` run through `splitmix64` before seeding `mt19937_64`?
3. Why must producers be joined *before* `queue.close()`? What invariant breaks otherwise?
4. Why are generated limit prices centered on `intrinsic_price` and never on the live price?
5. What are exit codes 0, 1, and 2, and which code path produces each?
6. Why `steady_clock` and not `system_clock` for `elapsed_seconds`?

---

## 10. The concurrency invariants document

**What it does:**
[`docs/concurrency-invariants.md`](concurrency-invariants.md) is the *specification* of the
synchronization model — 12 bullet points that the code in §5–§9 implements. It is short on
purpose: if the concurrency rules do not fit on one screen, the design is too complex.

**Walking the invariants against the code:**

| # | Invariant | Where it lives |
|---|-----------|----------------|
| 1 | Producers own their RNG and unpublished `Order`s | `produce()` local `rng`; `Order` is local until `push` |
| 2 | The queue mutex protects only queue contents, sequencing, closure, metrics | `bounded_queue.hpp` — every member locks `mutex_`, nothing else shares it |
| 3 | Producers release the queue mutex before the consumer handles an order | `push` returns (unlocks) before `pop` can take the element |
| 4 | The engine thread exclusively owns prices, volumes, pending pools, execution sequencing, SQLite | `Market` members, `engine_sequence_`, `next_execution_id_`, `Ledger` — all touched only in `Engine::run`'s thread |
| 5 | No producer reads mutable market state; limits are centered on immutable intrinsic prices | `produce()` uses `symbol.intrinsic_price`, never a `Market` |
| 6 | `stop_requested` and `fatal_error` are the only lock-free shared state; both atomic | `std::atomic_bool` in `main`, passed by reference |
| 7 | The signal handler only stores to `stop_requested` — no lock/alloc/log/SQLite | `handle_interrupt` is one `.store()` |
| 8 | Queue closure is permanent; a closed queue rejects pushes but allows draining | `closed_` has no reset; `pop` still returns buffered elements |
| 9 | A successful run ends only after every dequeued order is terminal and the final commit lands | `Engine::run`: loop, then `expire_pending`, then `finish()` |
| 10 | A persistence failure invalidates success, wakes/stops producers, rolls back, returns non-zero | `Engine::run` catch block; `main` exit code |

**Why write this file at all:**
Concurrency bugs do not show up in normal testing — they show up once in production under a
scheduling you never hit locally. The defense is to state the ownership rules explicitly, keep
them minimal, and then *review every shared access against the list*. This doc is also the
spec you would hand to ThreadSanitizer expectations: TSan finding a race means either the code
or invariant #2/#4/#6 is wrong.

**Check your understanding:**
1. Invariant #4 says the engine "exclusively owns … SQLite." Which earlier design decision
   (§1) makes that possible without a single DB lock?
2. Invariant #8 distinguishes "rejects pushes" from "allows draining." Which method enforces
   each half?
3. If you added a feature where producers adjust their order rate based on the current market
   price, which invariants would you break, and what synchronization would you now need?

---

## 11. The test suite — `tests/test_core.cpp`

**What it does:**
Nine Catch2 `TEST_CASE`s covering each subsystem in isolation plus one true end-to-end test
that shells out to the built binary and audits the resulting database.

**Exact locations:**
- [`tests/test_core.cpp`](../tests/test_core.cpp) lines 1–187

**The cases and what each one pins down:**

1. **`bounded queue is FIFO and assigns enqueue sequence`** — push two, close, pop two; assert
   order preserved, `enqueue_sequence` is 1 then 2, a third `pop` returns falsy, high-water is
   2. Locks in the core queue contract.
2. **`bounded queue applies backpressure`** — capacity 1, push one, spawn a thread that pushes
   a second, sleep 20 ms, assert it has *not* returned (it is blocked), `pop` one, join, assert
   the second push now succeeded and `blocked_pushes == 1`. This is the one test that actually
   exercises a blocking wait across threads.
3. **`AMM price is bounded and directionally monotonic`** — buy pressure raises the price but
   keeps it `<= intrinsic*(1+dev)`; sell pressure lowers it but keeps it `>= intrinsic*(1-dev)`.
   The runtime check of the `tanh` bound argument.
4. **`pending orders use price-time priority`** — pend three buy limits (two at 9500 arriving
   in a known order, one at 9000), drop the price so all are eligible, assert they come out
   earlier-of-the-two-9500s, then the other 9500, then 9000. Verifies both comparator axes.
5. **`configuration file is overridden by CLI`** — write a JSON with `traders:2,
   orders_per_trader:3`, pass `--config <file> --traders 4`, assert `traders == 4` (CLI wins)
   and `orders_per_trader == 3` (file value survives). The layered-precedence test.
6. **`configuration rejects unknown keys and invalid markets`** — `validate` throws on
   `max_deviation == 1.0`; loading a `{"mystery":1}` JSON throws. The fail-loud test.
7. **`engine persists every order to a terminal state`** — feed one market buy + one limit,
   run the engine against a temp DB with `commit_batch_size = 2`, assert
   `accepted==2, filled==1, expired==1, executions==1`, then **reopen the DB read-only** and
   assert 2 order rows, 2 of them terminal, 1 execution row, 1 `COMPLETED` run. Verifies the
   engine *and* the ledger together.
8. **`engine reports persistence failure`** — point the DB at a non-existent directory, run,
   assert `metrics.failed` and `fatal_error` is set. The failure-path test.
9. **`CLI completes and creates an auditable ledger` `[e2e]`** — `std::system()` runs the real
   `amm-sim` (path injected via `AMM_SIM_PATH`) with `--traders 2 --orders-per-trader 50`,
   asserts exit 0, then opens the DB and checks 100 order rows, zero non-terminal, one
   `COMPLETED` run, and `executions count == FILLED count`. The full-stack smoke test.

**Testing techniques worth copying:**
- **`scalar(db, sql)` helper** — a two-line "run this query, return the single integer" so DB
  assertions read like `REQUIRE(scalar(handle, "SELECT COUNT(*) FROM orders") == 100)`.
- **Temp files via `std::filesystem::temp_directory_path()`** with `remove()` before and after
  — hermetic, no fixture directory.
- **Reopening the DB `SQLITE_OPEN_READONLY`** in the same test — proves the data is actually on
  disk and committed, not just in the engine's counters.
- **Platform `#ifdef _WIN32`** for the bad path (`Z:/...` vs `/...`) and the shell invocation
  (`cmd /c "..."` quoting vs POSIX).
- **`[e2e]` tag** so `ctest`/Catch can select or exclude the slow shell-out test.

**Complexity:** the unit cases are microsecond-scale; case 2 sleeps 20 ms deliberately; case 9
runs a real 100-order simulation (still well under a second).

**Benefits:**
- Every subsystem has an isolated test *and* there is one test that runs the shipped binary.
- The failure path is tested, not just the happy path.
- DB assertions go through a fresh read-only connection — real durability verification.

**Drawbacks / tradeoffs accepted:**
- The backpressure test uses a fixed `sleep_for(20ms)` — a timing assumption that could be
  flaky on a very loaded CI box (though 20 ms is generous).
- No test forces a specific *multi-producer interleaving* (it is scheduler-dependent and
  documented as such), so cascade behavior under heavy concurrency is only covered
  statistically by the e2e run.
- No ThreadSanitizer invocation in the test file itself — that is a build-option concern
  (`AMM_ENABLE_TSAN`) run separately.

**Alternatives considered and rejected:**
1. Mocking SQLite — rejected: the ledger's whole job is real SQLite semantics (FKs, WAL,
   batching); a mock would test nothing useful.
2. A deterministic fake scheduler for interleavings — rejected: large infrastructure; the
   invariants doc + TSan is the chosen strategy.
3. Only end-to-end tests — rejected: a failing e2e test would not localize the bug; the unit
   cases do.

**How this connects to what came before:**
Directly exercises §5 (cases 1–2), §6 (cases 3–4), §4 (cases 5–6), §7+§8 (cases 7–8), and the
whole stack including §9 (case 9). Linked against `amm_core` (§2) so it tests shipping code.

**Check your understanding:**
1. Case 7 sets `commit_batch_size = 2` and then reopens the DB read-only. What two distinct
   things is that combination proving?
2. Why does the backpressure test need a second thread and a `sleep_for`? What would a
   single-threaded version fail to test?
3. Case 9 injects the binary path via `AMM_SIM_PATH` (a compile definition) rather than
   assuming `./amm-sim`. Why?
4. Which test would fail first if someone changed `BuyPriority` to sort by lowest price? Which
   if they removed `reject_unknown`?

---

## 12. Cross-cutting themes and what to study next

**The five ideas this project is really about:**

1. **Single-writer ownership beats fine-grained locking.** One thread owns all mutable market
   state and the database; everyone else talks to it through one bounded queue. No price lock,
   no pending-pool lock, no DB lock. This is the LMAX Disruptor / Redis / matching-core
   pattern. (§1, §7, §10)

2. **Backpressure is a feature.** A bounded queue that *blocks* a fast producer is how you get
   bounded memory and a measurable bottleneck (`blocked_pushes`, `high_water`). An unbounded
   queue just relocates the failure to the allocator. (§5)

3. **Determinism is engineered, not assumed.** Integer-scaled prices (associative arithmetic),
   per-producer `splitmix64`-mixed seeds (uncorrelated reproducible streams), and an explicit
   acknowledgement that *global* interleaving is still scheduler-dependent. (§3, §6, §9)

4. **Graceful shutdown is a state machine.** SIGINT → atomic flag → producers stop → join →
   close queue → engine drains → expire → verify → final commit → stamp `runs.status`. Every
   arrow is in the code and in the invariants doc. The failure path is the same machine with
   `rollback()` spliced in. (§7, §9, §10)

5. **Persistence has three knobs and this project turns all of them.** Prepared statements
   (skip the parser), batched transactions (`commit_batch_size`, amortize fsync), WAL +
   `synchronous=NORMAL` (cheap commits, documented durability cost). Plus foreign keys ON so
   the schema itself rejects inconsistent data. (§8)

**Where the model is deliberately unrealistic** (know these for any discussion of it):
- No partial fills, no cancel/replace, no bid/ask spread, no book depth.
- Price is path-independent (depends only on cumulative net flow).
- One engine thread — no per-symbol parallelism.
- `synchronous=NORMAL` — the last committed transaction can be lost on a power cut.

**Natural next steps if you wanted to extend it:**
- Add ThreadSanitizer to CI and run the e2e workload under it — the real test of §10.
- Shard the engine by symbol: one consumer thread + one `Ledger` (or one `ATTACH`ed DB) per
  symbol, producers route by `symbol_index`. Measure the throughput change.
- Replace the AMM price with a real `std::map<Price, std::deque<Order>>` book and an
  aggressor/resting matcher — turning this into the MiniExchange design.
- Make the workload size independent of thread count (a shared `orders_remaining` atomic
  instead of a per-producer quota).
- Add a replay tool that reads `orders`/`executions` for a `run_id` and re-derives the final
  `symbol_summaries`, asserting they match.

**Check your understanding (capstone):**
1. Someone proposes making `Market` thread-safe with a mutex so two engine threads can share
   it. Explain why that is the wrong direction and what to do instead.
2. A run reports `blocked_pushes` equal to 90% of `generated` and `queue_high_water` pinned at
   `queue_capacity`. What is happening, and what are your three options?
3. You need every committed order to survive a data-center power loss. What one PRAGMA do you
   change, what does it cost, and does anything else in the design need to change?
4. The final price for symbol `SIM` differs by 1 (integer) between a Linux and a macOS run at
   the same seed. Where does that come from, and is it a bug?
5. Trace exactly what happens, step by step, from the instant a user presses Ctrl+C during a
   run to the process returning 0.
