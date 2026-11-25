//! `probe_ctrl` — adaptive capture-tier selection driven by background
//! link-goodput probes.
//!
//! The controller answers one question: *which capture tier should the
//! streaming pipeline use right now?*  It answers it from background
//! throughput probes that must never interfere with an active capture.
//!
//! # Invariants
//!
//! 1. **Strict non-reentrancy.** No probe executes while `active_capture` is
//!    held. Setting the flag aborts an in-flight probe, and `trigger` is
//!    rejected while the flag is set.
//! 2. **Single-flight.** Overlapping triggers coalesce into the one in-flight
//!    probe; they never spawn a second task.
//! 3. **Bounded deadlines + cancellation safety.** Every probe runs under a
//!    per-chunk deadline. If the controller, the deadline or an abort drops
//!    the probe future, its guard releases the in-flight slot — a cancelled
//!    probe can never wedge the controller.
//! 4. **Anti-flap.** An EWMA over kbps samples plus asymmetric up/down
//!    hysteresis thresholds and a minimum dwell timer keep the tier stable
//!    under noisy links.
//! 5. **Failure separation.** A DNS/TCP failure means [`State::Offline`]; a
//!    reachable-but-slow or timed-out link means [`State::Degraded`], so the
//!    pipeline can tell "no network" from "bad network".

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::AbortHandle;
use tokio::time::Instant;

/// Capture tiers, ordered from narrowest to widest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Tier {
    Low,
    Medium,
    High,
    Ultra,
}

impl Tier {
    pub const ALL: [Tier; 4] = [Tier::Low, Tier::Medium, Tier::High, Tier::Ultra];

    pub fn index(self) -> usize {
        self as usize
    }

    pub fn from_index(i: usize) -> Tier {
        match i {
            0 => Tier::Low,
            1 => Tier::Medium,
            2 => Tier::High,
            _ => Tier::Ultra,
        }
    }
}

/// Externally observable controller state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// No probe has completed yet.
    Idle,
    /// A probe is running; the last settled tier (if any) is retained
    /// internally and exposed by [`ProbeController::tier`].
    Probing,
    /// Link is reachable and the tier is settled.
    Healthy(Tier),
    /// Link is reachable but slow; `tier` is the current (reduced) tier.
    Degraded(Tier),
    /// Last probe failed at DNS/TCP level.
    Offline,
}

/// Why a probe failed. Connectivity errors are separated from slowness on
/// purpose: slowness is a `Degraded` sample, connectivity is `Offline`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeError {
    Dns,
    Connect,
    Timeout,
    Other,
}

impl ProbeError {
    /// True when the error means "the network is gone", as opposed to
    /// "the network is reachable but the sample timed out".
    pub fn is_connectivity(self) -> bool {
        matches!(self, ProbeError::Dns | ProbeError::Connect)
    }
}

/// A probe measurement in kilobits per second.
pub type ProbeFuture = Pin<Box<dyn Future<Output = Result<u64, ProbeError>> + Send>>;

/// Tunables. Thresholds are kbps; `down[i] < up[i]` is required.
#[derive(Clone, Debug)]
pub struct Config {
    /// EWMA weight of the newest sample, in `(0, 1]`.
    pub alpha: f64,
    /// Minimum time between two tier changes.
    pub dwell: Duration,
    /// Per-chunk deadline; a probe that exceeds it is a timeout sample.
    pub chunk_deadline: Duration,
    /// `up[i]` promotes tier `i` to `i + 1`.
    pub up: [u64; 3],
    /// `down[i]` demotes tier `i + 1` back to `i`.
    pub down: [u64; 3],
}

impl Default for Config {
    fn default() -> Self {
        Config {
            alpha: 0.3,
            dwell: Duration::from_secs(10),
            chunk_deadline: Duration::from_millis(250),
            up: [750, 2_400, 6_000],
            down: [500, 1_800, 4_800],
        }
    }
}

impl Config {
    fn validate(&self) -> Result<(), ConfigError> {
        if !(self.alpha > 0.0 && self.alpha <= 1.0) {
            return Err(ConfigError::Alpha);
        }
        for i in 0..3 {
            if self.down[i] >= self.up[i] {
                return Err(ConfigError::Thresholds(i));
            }
            if i > 0 && self.up[i - 1] >= self.up[i] {
                return Err(ConfigError::Thresholds(i));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    Alpha,
    Thresholds(usize),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Alpha => write!(f, "alpha must be in (0, 1]"),
            ConfigError::Thresholds(i) => write!(f, "invalid threshold ordering at {i}"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// What a `trigger` call did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TriggerOutcome {
    /// A probe task was started.
    Started,
    /// A probe was already in flight; this trigger was folded into it.
    Coalesced,
    /// An active capture is running; probes are forbidden right now.
    Suppressed,
}

struct Core {
    state: State,
    tier: Option<Tier>,
    ewma_kbps: f64,
    samples: u64,
    failures: u64,
    last_change: Instant,
}

struct Inner {
    cfg: Config,
    core: Mutex<Core>,
    in_flight: AtomicBool,
    active_capture: AtomicBool,
    probe: Box<dyn Fn() -> ProbeFuture + Send + Sync>,
    cancel: Mutex<Option<AbortHandle>>,
}

/// Resets the single-flight slot however the probe task ends: normal return,
/// panic, deadline expiry or `abort()`.
struct FlightGuard(Arc<Inner>);

impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.0.in_flight.store(false, Ordering::Release);
    }
}

/// The controller handle. Cheap to clone (it is an `Arc` inside).
#[derive(Clone)]
pub struct ProbeController {
    inner: Arc<Inner>,
}

impl ProbeController {
    /// Build a controller around a probe closure. The closure is called once
    /// per started probe and must not block the runtime.
    pub fn new<F>(cfg: Config, probe: F) -> Result<Self, ConfigError>
    where
        F: Fn() -> ProbeFuture + Send + Sync + 'static,
    {
        cfg.validate()?;
        Ok(ProbeController {
            inner: Arc::new(Inner {
                cfg,
                core: Mutex::new(Core {
                    state: State::Idle,
                    tier: None,
                    ewma_kbps: 0.0,
                    samples: 0,
                    failures: 0,
                    last_change: Instant::now(),
                }),
                in_flight: AtomicBool::new(false),
                active_capture: AtomicBool::new(false),
                probe: Box::new(probe),
                cancel: Mutex::new(None),
            }),
        })
    }

    /// Current state.
    pub async fn state(&self) -> State {
        self.inner.core.lock().await.state
    }

    /// Last settled tier, retained while `Probing`/`Offline`.
    pub async fn tier(&self) -> Option<Tier> {
        self.inner.core.lock().await.tier
    }

    /// Current EWMA estimate in kbps.
    pub async fn ewma_kbps(&self) -> f64 {
        self.inner.core.lock().await.ewma_kbps
    }

    /// Whether a probe is currently in flight.
    pub fn in_flight(&self) -> bool {
        self.inner.in_flight.load(Ordering::Acquire)
    }

    /// Whether an active capture currently forbids probes.
    pub fn active_capture(&self) -> bool {
        self.inner.active_capture.load(Ordering::Acquire)
    }

    /// Start one background probe, unless suppressed or already running.
    ///
    /// This is the only entry point that spawns work, and the spawn is guarded
    /// by a `compare_exchange` on the single-flight flag, so concurrent callers
    /// can never create two probes.
    pub async fn trigger(&self) -> TriggerOutcome {
        if self.inner.active_capture.load(Ordering::Acquire) {
            return TriggerOutcome::Suppressed;
        }
        if self
            .inner
            .in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return TriggerOutcome::Coalesced;
        }

        {
            let mut core = self.inner.core.lock().await;
            core.state = State::Probing;
        }

        let inner = Arc::clone(&self.inner);
        let guard = FlightGuard(Arc::clone(&inner));
        let handle = tokio::spawn(async move {
            let _guard = guard;
            let outcome = tokio::time::timeout(inner.cfg.chunk_deadline, (inner.probe)()).await;
            let mut core = inner.core.lock().await;
            core.apply(&inner.cfg, outcome);
        });
        *self.inner.cancel.lock().await = Some(handle.abort_handle());
        TriggerOutcome::Started
    }

    /// Enter (`true`) or leave (`false`) an active capture.
    ///
    /// Entering aborts any in-flight probe immediately: invariant 1 says no
    /// probe may execute while the capture holds the link. The abort is
    /// cancellation-safe — the `FlightGuard` releases the slot.
    pub async fn set_active_capture(&self, active: bool) {
        if active {
            self.inner.active_capture.store(true, Ordering::Release);
            if let Some(h) = self.inner.cancel.lock().await.take() {
                h.abort();
            }
            let mut core = self.inner.core.lock().await;
            if core.state == State::Probing {
                core.state = match core.tier {
                    Some(t) => State::Healthy(t),
                    None => State::Idle,
                };
            }
        } else {
            self.inner.active_capture.store(false, Ordering::Release);
        }
    }

    /// Wait until no probe is in flight. Intended for tests and graceful
    /// shutdown; yields rather than blocking a runtime thread.
    pub async fn wait_idle(&self) {
        for _ in 0..10_000 {
            if !self.in_flight() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

impl Core {
    /// Fold one probe outcome into the state machine.
    fn apply(
        &mut self,
        cfg: &Config,
        outcome: Result<Result<u64, ProbeError>, tokio::time::error::Elapsed>,
    ) {
        let now = Instant::now();
        let sample: Result<u64, ProbeError> = match outcome {
            Ok(inner) => inner,
            Err(_) => Err(ProbeError::Timeout),
        };

        match sample {
            Ok(kbps) => {
                self.samples += 1;
                self.ewma_kbps = if self.samples == 1 {
                    kbps as f64
                } else {
                    cfg.alpha * kbps as f64 + (1.0 - cfg.alpha) * self.ewma_kbps
                };

                let current = self.tier.unwrap_or_else(|| Tier::from_index(base_tier(self.ewma_kbps, cfg)));
                let target = next_tier(current, self.ewma_kbps, cfg);

                // Reachable but under the lowest promotion threshold: this is a
                // degraded link even when the tier cannot move further down.
                let slow = self.ewma_kbps < cfg.up[0] as f64;
                let next_state = if slow {
                    State::Degraded(target)
                } else {
                    State::Healthy(target)
                };

                let first = self.tier.is_none();
                if first || self.can_transition(now, cfg.dwell, next_state) {
                    self.tier = Some(target);
                    self.state = next_state;
                    self.last_change = now;
                }
            }
            Err(err) => {
                self.failures += 1;
                if err.is_connectivity() {
                    self.state = State::Offline;
                    self.last_change = now;
                } else {
                    // Timeout: reachable but slow. Demote one tier, floor at Low.
                    let cur = self.tier.unwrap_or(Tier::Low);
                    let demoted = Tier::from_index(cur.index().saturating_sub(1));
                    self.tier = Some(demoted);
                    self.state = State::Degraded(demoted);
                    self.last_change = now;
                }
            }
        }
    }

    /// A state change is allowed when it is a no-op or when the dwell timer
    /// has elapsed.
    fn can_transition(&self, now: Instant, dwell: Duration, next: State) -> bool {
        if same_kind(self.state, next) {
            return true;
        }
        now.saturating_duration_since(self.last_change) >= dwell
    }
}

fn same_kind(a: State, b: State) -> bool {
    match (a, b) {
        (State::Healthy(x), State::Healthy(y)) => x == y,
        (State::Degraded(x), State::Degraded(y)) => x == y,
        (State::Offline, State::Offline) => true,
        (State::Idle, State::Idle) => true,
        (State::Probing, State::Probing) => true,
        _ => false,
    }
}

fn base_tier(kbps: f64, cfg: &Config) -> usize {
    let mut tier = 0;
    for (i, threshold) in cfg.up.iter().enumerate() {
        if kbps >= *threshold as f64 {
            tier = i + 1;
        }
    }
    tier
}

/// One-step hysteresis: promote only above `up[current]`, demote only below
/// `down[current - 1]`. Everything in between keeps the current tier, which is
/// what makes the controller resist flapping on a noisy link.
fn next_tier(current: Tier, kbps: f64, cfg: &Config) -> Tier {
    let i = current.index();
    if i < 3 && kbps >= cfg.up[i] as f64 {
        return Tier::from_index(i + 1);
    }
    if i > 0 && kbps < cfg.down[i - 1] as f64 {
        return Tier::from_index(i - 1);
    }
    current
}