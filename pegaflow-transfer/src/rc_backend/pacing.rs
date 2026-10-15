use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::error::{Result, TransferError};

pub(crate) const PACING_CHUNK_BYTES: usize = 1024 * 1024;

pub(crate) struct SharedPacer {
    schedule: Mutex<(u64, Instant)>,
    pub(crate) active: AtomicUsize,
    pub(crate) posted_bytes: AtomicU64,
    pub(crate) completed_bytes: AtomicU64,
}

impl SharedPacer {
    pub(crate) fn new() -> Self {
        Self {
            schedule: Mutex::new((0, Instant::now())),
            active: AtomicUsize::new(0),
            posted_bytes: AtomicU64::new(0),
            completed_bytes: AtomicU64::new(0),
        }
    }

    pub(crate) fn set_rate(&self, rate: u64) -> Result<()> {
        let mut schedule = self.schedule.lock();
        if schedule.0 == rate {
            return Ok(());
        }
        if self.active.load(Ordering::Acquire) != 0 {
            return Err(TransferError::Backend(
                "pacing changes require drained transfers".into(),
            ));
        }
        *schedule = (rate, Instant::now());
        Ok(())
    }

    pub(crate) fn limited(&self) -> bool {
        self.schedule.lock().0 != 0
    }

    pub(crate) fn begin_batch(&self) {
        let _schedule = self.schedule.lock();
        self.active.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Instant {
        let mut schedule = self.schedule.lock();
        let now = Instant::now();
        if schedule.0 == 0 {
            return now;
        }
        let start = schedule.1.max(now);
        let ns = (bytes as u128 * 8 * 1_000_000_000).div_ceil(schedule.0 as u128);
        schedule.1 = start + Duration::from_nanos(ns.min(u64::MAX as u128) as u64);
        start
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_share_one_byte_budget_and_require_drain() {
        let pacer = SharedPacer::new();
        pacer.set_rate(1_000_000_000).unwrap();
        let a = pacer.reserve(PACING_CHUNK_BYTES);
        let b = pacer.reserve(PACING_CHUNK_BYTES);
        assert_eq!(b.duration_since(a), Duration::from_nanos(8_388_608));
        pacer.begin_batch();
        assert!(pacer.set_rate(500_000_000).is_err());
        assert!(pacer.set_rate(1_000_000_000).is_ok());
        pacer.active.store(0, Ordering::Release);
        pacer.set_rate(0).unwrap();
        assert!(!pacer.limited());
    }
}
