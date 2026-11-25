# probe_ctrl — adaptive capture-tier controller

Standalone Rust library (`tokio` + `std` only) that selects one of four capture
tiers (`Ultra`, `High`, `Medium`, `Low`) from background link-goodput probes.

## Why it exists

A streaming capture pipeline must not probe the link while it is capturing: the
probe traffic contaminates the measurement and the capture. It also must not
run two probes at once, and it must not oscillate between tiers when the link
is noisy. `probe_ctrl` encodes those three rules as hard invariants.

## Invariants

1. **Strict non-reentrancy** — no probe executes while `active_capture` is
   held. `set_active_capture(true)` aborts an in-flight probe and `trigger()`
   returns `Suppressed`.
2. **Single-flight** — overlapping `trigger()` calls coalesce into the one
   in-flight probe (`TriggerOutcome::Coalesced`); a `compare_exchange` on the
   in-flight flag is the only spawn gate.
3. **Bounded deadline + cancellation safety** — every probe runs under
   `Config::chunk_deadline`. A `FlightGuard` (RAII) releases the slot on
   normal completion, panic, timeout or `abort()`, so a cancelled probe can
   never wedge the controller.
4. **Anti-flap** — EWMA (`Config::alpha`) + asymmetric up/down hysteresis
   (`up[i]` / `down[i]`, with `down[i] < up[i]`) + a minimum dwell timer
   (`Config::dwell`) between tier changes.
5. **Failure separation** — `ProbeError::Dns` / `Connect` produce
   `State::Offline`; a slow sample or a deadline expiry produces
   `State::Degraded(tier)` and demotes one tier (floored at `Low`).

## State machine

```
Idle --trigger--> Probing --ok(kbps)--> Healthy(tier) | Degraded(tier)
                    |                        ^              ^
                    |--dns/connect---------> Offline -------|
                    |--timeout--------------> Degraded(tier-1)
active_capture=true --> abort in-flight, back to Healthy(last) / Idle
```

## Usage

```rust
use probe_ctrl::{Config, ProbeController, State, Tier};
use std::time::Duration;

let ctl = ProbeController::new(Config::default(), || {
    Box::pin(async {
        // measure the link, e.g. one bounded chunk of a download
        Ok(4_200u64) // kbps
    })
})?;

ctl.trigger().await;            // Started | Coalesced | Suppressed
ctl.wait_idle().await;
assert_eq!(ctl.state().await, State::Healthy(Tier::High));

ctl.set_active_capture(true).await;   // probes are now forbidden
ctl.set_active_capture(false).await;
```

## Tests

Deterministic, driven by `tokio::time::pause()` / `advance()` — no wall-clock
sleeps. 12 tests cover: first-sample settling, capture suppression, trigger
coalescing, timeout demotion, DNS/connect -> Offline, dwell anti-flap, EWMA
smoothing, abort-on-capture cancellation safety, stepwise promotion, config
validation, tier ordering and the paused clock itself.

```
$ cargo test
running 12 tests
............
test result: ok. 12 passed; 0 failed
```

## Layout

| file | contents |
|---|---|
| `src/lib.rs` | controller, state machine, EWMA/hysteresis/dwell, RAII flight guard |
| `tests/controller.rs` | 12 deterministic tests |
