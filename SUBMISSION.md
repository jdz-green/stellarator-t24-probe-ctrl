# t=24 submission — probe_ctrl (Rust, tokio + std only)

Repo: https://github.com/jdz-green/stellarator-t24-probe-ctrl
Crate: `probe_ctrl` (tokio + std only)
Solana payout: `6aZyRojc4rnRSLQY7AEhvAP3Do6Atix5ve71Sq4cyjXx`

## What it does
Adaptive capture-tier selection over `{Ultra, High, Medium, Low}` driven by
background goodput probes, with the three required invariants encoded as
structure rather than convention:

1. **Strict non-reentrancy** — `set_active_capture(true)` aborts any in-flight
   probe and `trigger()` returns `Suppressed`; probes literally cannot run while
   the capture holds the link.
2. **Single-flight coalescing** — a `compare_exchange` on the in-flight flag is
   the only spawn gate; overlapping triggers return `Coalesced` and never create
   a second task.
3. **Bounded deadline + cancellation safety** — each probe is wrapped in
   `tokio::time::timeout(chunk_deadline, ...)`; an RAII `FlightGuard` releases
   the slot on completion, panic, timeout or `abort()`, so a cancelled probe
   cannot wedge the controller (proved by a test that aborts mid-probe and
   immediately re-triggers).
4. **Anti-flap** — EWMA kbps + asymmetric up/down hysteresis (`down[i] < up[i]`)
   + minimum dwell timer; one-step transitions only.
5. **Offline vs Degraded** — `Dns`/`Connect` → `Offline`; slow sample or
   deadline expiry → `Degraded(tier)` with one-step demotion floored at `Low`.

## Verification
`cargo test` → **12 passed, 0 failed**, entirely under `tokio::time::pause()` +
`advance()` (no wall-clock sleeps, fully deterministic):
first-sample settling; capture suppression; 8 overlapping triggers → 1 task;
timeout → `Degraded(Low)` + slot released; DNS/connect → `Offline`; dwell blocks
rapid tier changes; EWMA smoothing; abort-on-capture cancellation safety;
stepwise promotion/demotion; config validation; tier ordering.

## Files
- `src/lib.rs` — controller, state machine, EWMA/hysteresis/dwell, FlightGuard
- `tests/controller.rs` — 12 deterministic tests
- `README.md` — design + invariants + usage
