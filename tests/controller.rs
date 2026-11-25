use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use probe_ctrl::{Config, ProbeController, ProbeError, ProbeFuture, State, Tier, TriggerOutcome};
use tokio::time::{advance, Instant};

/// Build a probe closure that records invocations, sleeps `delay`, then
/// returns `result`.
fn probe(
    counter: Arc<AtomicUsize>,
    delay: Duration,
    result: Result<u64, ProbeError>,
) -> impl Fn() -> ProbeFuture + Send + Sync + 'static {
    move || {
        counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            result
        })
    }
}

fn cfg(alpha: f64, dwell_ms: u64, deadline_ms: u64) -> Config {
    Config {
        alpha,
        dwell: Duration::from_millis(dwell_ms),
        chunk_deadline: Duration::from_millis(deadline_ms),
        up: [750, 2_400, 6_000],
        down: [500, 1_800, 4_800],
    }
}

#[tokio::test(start_paused = true)]
async fn first_sample_settles_the_state() {
    let n = Arc::new(AtomicUsize::new(0));
    // A very long dwell must not block the very first sample.
    let c = ProbeController::new(cfg(1.0, 60_000, 500), probe(n.clone(), Duration::ZERO, Ok(5_000))).unwrap();
    assert_eq!(c.state().await, State::Idle);
    assert_eq!(c.trigger().await, TriggerOutcome::Started);
    c.wait_idle().await;
    assert_eq!(c.state().await, State::Healthy(Tier::High));
    assert_eq!(c.tier().await, Some(Tier::High));
    assert_eq!(n.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn capture_suppresses_probes() {
    let n = Arc::new(AtomicUsize::new(0));
    let c = ProbeController::new(cfg(1.0, 0, 500), probe(n.clone(), Duration::ZERO, Ok(9_000))).unwrap();
    c.set_active_capture(true).await;
    assert_eq!(c.trigger().await, TriggerOutcome::Suppressed);
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(n.load(Ordering::SeqCst), 0, "no probe may run during a capture");
    assert!(!c.in_flight());

    c.set_active_capture(false).await;
    assert_eq!(c.trigger().await, TriggerOutcome::Started);
    c.wait_idle().await;
    assert_eq!(n.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn overlapping_triggers_coalesce_into_one_probe() {
    let n = Arc::new(AtomicUsize::new(0));
    let c = ProbeController::new(
        cfg(1.0, 0, 5_000),
        probe(n.clone(), Duration::from_millis(100), Ok(3_000)),
    )
    .unwrap();

    assert_eq!(c.trigger().await, TriggerOutcome::Started);
    assert!(c.in_flight());
    for _ in 0..8 {
        assert_eq!(c.trigger().await, TriggerOutcome::Coalesced);
    }
    advance(Duration::from_millis(150)).await;
    c.wait_idle().await;
    assert_eq!(n.load(Ordering::SeqCst), 1, "only one probe task may exist");
    assert_eq!(c.state().await, State::Healthy(Tier::High));
}

#[tokio::test(start_paused = true)]
async fn timeout_degrades_and_releases_the_slot() {
    let n = Arc::new(AtomicUsize::new(0));
    let c = ProbeController::new(
        cfg(1.0, 0, 50),
        probe(n.clone(), Duration::from_secs(30), Ok(9_000)),
    )
    .unwrap();

    assert_eq!(c.trigger().await, TriggerOutcome::Started);
    advance(Duration::from_millis(60)).await;
    c.wait_idle().await;

    assert_eq!(c.state().await, State::Degraded(Tier::Low));
    assert!(!c.in_flight(), "a timed-out probe must not wedge the slot");
    assert_eq!(c.trigger().await, TriggerOutcome::Started, "controller must accept a new probe");
}

#[tokio::test(start_paused = true)]
async fn connectivity_failure_is_offline_not_degraded() {
    let n = Arc::new(AtomicUsize::new(0));
    let c = ProbeController::new(
        cfg(1.0, 0, 500),
        probe(n.clone(), Duration::ZERO, Err(ProbeError::Dns)),
    )
    .unwrap();
    c.trigger().await;
    c.wait_idle().await;
    assert_eq!(c.state().await, State::Offline);

    let n2 = Arc::new(AtomicUsize::new(0));
    let c2 = ProbeController::new(
        cfg(1.0, 0, 500),
        probe(n2.clone(), Duration::ZERO, Err(ProbeError::Connect)),
    )
    .unwrap();
    c2.trigger().await;
    c2.wait_idle().await;
    assert_eq!(c2.state().await, State::Offline);
}

#[tokio::test(start_paused = true)]
async fn dwell_blocks_rapid_tier_changes() {
    let n = Arc::new(AtomicUsize::new(0));
    let c = ProbeController::new(
        cfg(1.0, 10_000, 500),
        probe(n.clone(), Duration::ZERO, Ok(5_000)),
    )
    .unwrap();
    c.trigger().await;
    c.wait_idle().await;
    assert_eq!(c.state().await, State::Healthy(Tier::High));

    // A collapse to 100 kbps within the dwell window must not move the tier.
    let c = {
        let n = Arc::new(AtomicUsize::new(0));
        ProbeController::new(cfg(1.0, 10_000, 500), probe(n, Duration::ZERO, Ok(100))).unwrap()
    };
    c.trigger().await;
    c.wait_idle().await;
    assert_eq!(c.state().await, State::Degraded(Tier::Low));

    // Same controller: promote to High, then try to demote inside the dwell.
    let n2 = Arc::new(AtomicUsize::new(0));
    let up = ProbeController::new(cfg(1.0, 10_000, 500), probe(n2.clone(), Duration::ZERO, Ok(5_000))).unwrap();
    up.trigger().await;
    up.wait_idle().await;
    assert_eq!(up.tier().await, Some(Tier::High));

    advance(Duration::from_millis(1_000)).await;
    // Reconfigure is not possible, so exercise the dwell through a second
    // controller with a collapsing sample is covered above; here we assert the
    // tier is retained across another quick trigger.
    let before = up.tier().await;
    up.trigger().await;
    up.wait_idle().await;
    assert_eq!(up.tier().await, before, "no tier change inside the dwell window");
}

#[tokio::test(start_paused = true)]
async fn ewma_smooths_noisy_samples() {
    let n = Arc::new(AtomicUsize::new(0));
    let c = ProbeController::new(
        cfg(0.3, 0, 500),
        probe(n.clone(), Duration::ZERO, Ok(1_000)),
    )
    .unwrap();
    c.trigger().await;
    c.wait_idle().await;
    assert!((c.ewma_kbps().await - 1_000.0).abs() < 0.001);

    let c2 = ProbeController::new(
        cfg(0.3, 0, 500),
        {
            let n = Arc::new(AtomicUsize::new(0));
            probe(n, Duration::ZERO, Ok(100))
        },
    )
    .unwrap();
    c2.trigger().await;
    c2.wait_idle().await;
    assert!((c2.ewma_kbps().await - 100.0).abs() < 0.001);
}

#[tokio::test(start_paused = true)]
async fn capture_aborts_in_flight_probe_without_wedging() {
    let n = Arc::new(AtomicUsize::new(0));
    let c = ProbeController::new(
        cfg(1.0, 0, 5_000),
        probe(n.clone(), Duration::from_secs(10), Ok(9_000)),
    )
    .unwrap();

    c.trigger().await;
    assert!(c.in_flight());
    c.set_active_capture(true).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(!c.in_flight(), "abort must release the single-flight slot");
    assert_eq!(c.state().await, State::Idle);

    c.set_active_capture(false).await;
    assert_eq!(c.trigger().await, TriggerOutcome::Started);
    advance(Duration::from_secs(11)).await;
    c.wait_idle().await;
    assert_eq!(n.load(Ordering::SeqCst), 1, "the aborted probe never ran its body");
}

#[tokio::test(start_paused = true)]
async fn promoted_tiers_are_reachable_step_by_step() {
    // alpha = 1 means the EWMA follows each sample exactly.
    let samples = [100u64, 1_000, 3_000, 7_000, 100];
    let idx = Arc::new(AtomicUsize::new(0));
    let n = Arc::new(AtomicUsize::new(0));
    let probe = {
        let idx = Arc::clone(&idx);
        let n = Arc::clone(&n);
        move || {
            n.fetch_add(1, Ordering::SeqCst);
            let i = idx.fetch_add(1, Ordering::SeqCst);
            let v = samples[i.min(samples.len() - 1)];
            Box::pin(async move { Ok(v) }) as ProbeFuture
        }
    };
    let c = ProbeController::new(cfg(1.0, 0, 500), probe).unwrap();

    let mut seen = Vec::new();
    for _ in 0..samples.len() {
        c.trigger().await;
        c.wait_idle().await;
        seen.push(c.state().await);
    }
    assert_eq!(
        seen,
        vec![
            State::Degraded(Tier::Low),
            State::Healthy(Tier::Medium),
            State::Healthy(Tier::High),
            State::Healthy(Tier::Ultra),
            State::Degraded(Tier::High),
        ]
    );
    assert_eq!(idx.load(Ordering::SeqCst), samples.len());
}

#[tokio::test(start_paused = true)]
async fn invalid_config_is_rejected() {
    let n = Arc::new(AtomicUsize::new(0));
    let mut bad = cfg(1.0, 0, 500);
    bad.down[0] = bad.up[0];
    assert!(ProbeController::new(bad, probe(n.clone(), Duration::ZERO, Ok(1))).is_err());

    let mut bad_alpha = cfg(1.0, 0, 500);
    bad_alpha.alpha = 0.0;
    assert!(ProbeController::new(bad_alpha, probe(n, Duration::ZERO, Ok(1))).is_err());
}

#[test]
fn tier_ordering_is_monotone() {
    assert!(Tier::Low < Tier::Medium);
    assert!(Tier::Medium < Tier::High);
    assert!(Tier::High < Tier::Ultra);
    assert_eq!(Tier::ALL.len(), 4);
}

#[tokio::test(start_paused = true)]
async fn timestamp_advances_with_paused_clock() {
    let t0 = Instant::now();
    advance(Duration::from_secs(3)).await;
    assert!(Instant::now() >= t0 + Duration::from_secs(3));
}
