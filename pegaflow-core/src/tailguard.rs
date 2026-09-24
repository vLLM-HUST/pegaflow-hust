//! Opt-in admission for demand remote reads.
//!
//! This is the supported subset of the historical standalone TailGuard
//! prototype. It does not implement placement, refill, migration, or request
//! priority. A denied read becomes a cache miss; the caller keeps any local
//! prefix and lets the inference engine compute the missing blocks.

#[cfg(feature = "rdma")]
use std::sync::Arc;
#[cfg(feature = "rdma")]
use std::time::Instant;

#[cfg(feature = "rdma")]
use parking_lot::Mutex;

#[derive(Clone, Debug)]
pub struct TailGuardRemoteReadConfig {
    pub bytes_per_second: u64,
    pub burst_bytes: u64,
    pub max_inflight_bytes: u64,
}

impl TailGuardRemoteReadConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.bytes_per_second == 0 || self.burst_bytes == 0 || self.max_inflight_bytes == 0 {
            return Err(
                "TailGuard remote-read rate, burst, and inflight limits must be positive".into(),
            );
        }
        Ok(())
    }
}

#[cfg(feature = "rdma")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdmissionDenial {
    BurstLimit,
    InflightLimit,
    RateLimit,
}

#[cfg(feature = "rdma")]
impl AdmissionDenial {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BurstLimit => "burst_limit",
            Self::InflightLimit => "inflight_limit",
            Self::RateLimit => "rate_limit",
        }
    }
}

#[cfg(feature = "rdma")]
struct State {
    tokens: u64,
    fractional_token_nanos: u128,
    updated_at: Instant,
    inflight_bytes: u64,
}

#[cfg(feature = "rdma")]
pub(crate) struct TailGuardRemoteReadController {
    config: TailGuardRemoteReadConfig,
    state: Mutex<State>,
}

#[cfg(feature = "rdma")]
impl TailGuardRemoteReadController {
    pub(crate) fn new(config: TailGuardRemoteReadConfig) -> Result<Arc<Self>, String> {
        config.validate()?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                tokens: config.burst_bytes,
                fractional_token_nanos: 0,
                updated_at: Instant::now(),
                inflight_bytes: 0,
            }),
            config,
        }))
    }

    pub(crate) fn try_reserve(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<TailGuardRemoteReadPermit, AdmissionDenial> {
        self.try_reserve_at(bytes, Instant::now())
    }

    fn try_reserve_at(
        self: &Arc<Self>,
        bytes: u64,
        now: Instant,
    ) -> Result<TailGuardRemoteReadPermit, AdmissionDenial> {
        let mut state = self.state.lock();
        let elapsed_nanos = now.saturating_duration_since(state.updated_at).as_nanos();
        let replenished = u128::from(self.config.bytes_per_second)
            .saturating_mul(elapsed_nanos)
            .saturating_add(state.fractional_token_nanos);
        let tokens = u128::from(state.tokens)
            .saturating_add(replenished / 1_000_000_000)
            .min(u128::from(self.config.burst_bytes));
        state.tokens = tokens as u64;
        state.fractional_token_nanos = if state.tokens == self.config.burst_bytes {
            0
        } else {
            replenished % 1_000_000_000
        };
        state.updated_at = now.max(state.updated_at);

        if bytes > self.config.burst_bytes {
            return Err(AdmissionDenial::BurstLimit);
        }
        if state
            .inflight_bytes
            .checked_add(bytes)
            .is_none_or(|total| total > self.config.max_inflight_bytes)
        {
            return Err(AdmissionDenial::InflightLimit);
        }
        if bytes > state.tokens {
            return Err(AdmissionDenial::RateLimit);
        }

        state.tokens -= bytes;
        state.inflight_bytes += bytes;
        Ok(TailGuardRemoteReadPermit {
            controller: Arc::clone(self),
            bytes,
        })
    }
}

#[cfg(feature = "rdma")]
pub(crate) struct TailGuardRemoteReadPermit {
    controller: Arc<TailGuardRemoteReadController>,
    bytes: u64,
}

#[cfg(feature = "rdma")]
impl Drop for TailGuardRemoteReadPermit {
    fn drop(&mut self) {
        let mut state = self.controller.state.lock();
        debug_assert!(state.inflight_bytes >= self.bytes);
        state.inflight_bytes = state.inflight_bytes.saturating_sub(self.bytes);
    }
}

#[cfg(all(test, feature = "rdma"))]
mod tests {
    use super::*;
    use std::time::Duration;

    fn controller() -> Arc<TailGuardRemoteReadController> {
        TailGuardRemoteReadController::new(TailGuardRemoteReadConfig {
            bytes_per_second: 100,
            burst_bytes: 100,
            max_inflight_bytes: 100,
        })
        .unwrap()
    }

    #[test]
    fn permit_releases_inflight_bytes_but_not_spent_tokens() {
        let controller = controller();
        let now = controller.state.lock().updated_at;
        let first = controller.try_reserve_at(80, now).unwrap();
        assert_eq!(
            controller.try_reserve_at(30, now).err(),
            Some(AdmissionDenial::InflightLimit)
        );
        drop(first);
        assert_eq!(
            controller.try_reserve_at(30, now).err(),
            Some(AdmissionDenial::RateLimit)
        );
        let next = controller
            .try_reserve_at(30, now + Duration::from_secs(1))
            .unwrap();
        drop(next);
        assert_eq!(controller.state.lock().inflight_bytes, 0);
    }

    #[test]
    fn oversized_read_is_denied_without_reservation() {
        let controller = controller();
        assert_eq!(
            controller.try_reserve(101).err(),
            Some(AdmissionDenial::BurstLimit)
        );
        assert_eq!(controller.state.lock().inflight_bytes, 0);
    }

    #[test]
    fn invalid_config_is_rejected() {
        assert!(
            TailGuardRemoteReadController::new(TailGuardRemoteReadConfig {
                bytes_per_second: 0,
                burst_bytes: 100,
                max_inflight_bytes: 100,
            })
            .is_err()
        );
    }
}
