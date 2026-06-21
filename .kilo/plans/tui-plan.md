# TUI UI Plan for AMM Trading Simulator

## Goal
Build a minimalistic black-and-white terminal user interface (TUI) for the AMM Trading Simulator that allows users to configure, run, and monitor simulations.

## Options

**Option A: Configuration Wizard + Live Monitor TUI**
- Terminal-driven config editor that generates JSON configs
- Live dashboard during simulation showing throughput, queue stats, price movements
- Post-run summary viewer with database results

**Option B: Full Interactive Workflow TUI**
- All-in-one terminal app with prompts for parameters
- Runs simulation in background thread while updating UI
- Colorless (black/white) ncurses-style interface

## Recommended: Option A
More practical - preserves CLI workflow, adds configuration assistance and live monitoring.

## Implementation Plan

### 1. New Component: `src/tui_config.cpp`
Terminal-based configuration wizard that:
- Prompts for traders, orders-per-trader, queue-capacity, seed
- Prompts for workload parameters (probabilities, quantity range, price band)
- Prompts for symbol configurations (ticker, intrinsic_price, liquidity, max_deviation, price_scale)
- Outputs valid JSON to stdout or file
- Uses standard C++20 (no external TUI libs)

### 2. New Component: `src/monitor.cpp` 
Live monitoring overlay:
- Reads SQLite ledger in read-only mode
- Polls `runs` table for status updates
- Displays live counters: generated/accepted/processed/filled/expired
- Shows queue high-water mark and blocked pushes
- Updates every ~250ms during active run

### 3. Integration with main.cpp
- Add `--tui` flag to launch configuration wizard
- Add `--monitor <run_id>` to attach to running/finished simulation
- Run simulation in separate thread, update terminal UI

### 4. Data Flow
```
tui_config → JSON config → amm-sim (background) → SQLite ledger
                                      ↓
                                monitor (polls DB)
```

### 5. UI Mockup (ASCII)
```
┌─ AMM Trading Simulator ─────────────────────────────┐
│ Traders: [8     ] Orders/trader: [10000 ] Queue cap: [65536] │
│ ┌─ Workload ──────────────────────────────────────┐ │
│ │ Market %: [0.70] Buy %: [0.50] Qty: [1-100] Band: [0.10] │
│ └───────────────────────────────────────────────────┘ │
│ ┌─ Symbols ───────────────────────────────────────┐ │
│ │ [1] SIM: price=100, liquidity=100000, dev=0.5   │
│ │ [2] ALT: price=25,  liquidity=50000,  dev=0.35  │
│ └───────────────────────────────────────────────────┘ │
│ [ Run ] [ Cancel ]                                    │
└───────────────────────────────────────────────────────┘
```

## Questions

**Q: Should the TUI run the simulation in-process (single binary) or spawn the existing amm-sim executable?**

A: In-process for better integration (recommended). This allows live monitoring and more responsive UI.

**Q: Do you want to save this configuration wizard approach?**

Options:
1. Finalize and save the plan
2. Continue refining