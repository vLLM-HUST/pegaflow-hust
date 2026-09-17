//! Frozen transport-timing treatment used by the Issue #23 causal experiment.
//!
//! This module is deliberately opt-in.  With no experiment configuration the
//! normal PegaFlow path is unchanged.  An experiment starts in preparation
//! mode, records the exact logical transfer bundles, and may then atomically
//! hide the restored D-side cache before enabling a frozen smooth/burst gate.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
#[cfg(feature = "rdma")]
use tokio::sync::oneshot;

#[cfg(feature = "rdma")]
use crate::backing::RdmaTransport;
use crate::block::{BlockKey, SealedBlock};

const EXPECTED_COHORT_SIZE: usize = 4;
const BURST_OFFSET_MS: u64 = 15;
const SMOOTH_OFFSETS_MS: [u64; EXPECTED_COHORT_SIZE] = [0, 10, 20, 30];
const MAX_BURST_POSTING_SPREAD: Duration = Duration::from_millis(1);
#[cfg(feature = "rdma")]
const PRECISE_WAIT_GUARD: Duration = Duration::from_millis(2);

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Issue23Backend {
    Rdma,
    LocalCopy,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Issue23Schedule {
    Smooth,
    Burst,
}

#[derive(Clone, Debug)]
pub struct Issue23ExperimentConfig {
    pub plan: Option<Issue23TransferPlan>,
    pub backend: Issue23Backend,
    pub schedule: Issue23Schedule,
    pub trace_path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Issue23TransferPlan {
    pub schema_version: u32,
    pub protocol_id: String,
    pub block_id: String,
    pub qps_per_peer: usize,
    pub requests: Vec<Issue23RequestPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freeze_provenance: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Issue23RequestPlan {
    pub request_id: String,
    pub cohort_id: String,
    pub smooth_slot: usize,
    pub operations: Vec<Issue23OperationPlan>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Issue23OperationPlan {
    pub operation_id: String,
    pub block_hash_hex: String,
    pub slot_index: usize,
    pub segment: String,
    pub source: String,
    pub destination: String,
    pub transfer_type: String,
    pub logical_payload_bytes: usize,
    pub qp_index: usize,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ObservedOperation {
    pub(crate) operation_id: String,
    pub(crate) block_hash_hex: String,
    pub(crate) slot_index: usize,
    pub(crate) segment: String,
    pub(crate) source: String,
    pub(crate) destination: String,
    pub(crate) transfer_type: String,
    pub(crate) logical_payload_bytes: usize,
    pub(crate) qp_index: usize,
    #[serde(skip)]
    pub(crate) local_addr: u64,
    #[serde(skip)]
    pub(crate) remote_addr: u64,
}

impl ObservedOperation {
    fn as_plan(&self) -> Issue23OperationPlan {
        Issue23OperationPlan {
            operation_id: self.operation_id.clone(),
            block_hash_hex: self.block_hash_hex.clone(),
            slot_index: self.slot_index,
            segment: self.segment.clone(),
            source: self.source.clone(),
            destination: self.destination.clone(),
            transfer_type: self.transfer_type.clone(),
            logical_payload_bytes: self.logical_payload_bytes,
            qp_index: self.qp_index,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub(crate) struct Issue23ActivationStats {
    pub(crate) hidden_blocks: usize,
    pub(crate) hidden_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Issue23Completion {
    pub(crate) eligible_wait: Duration,
    pub(crate) submit: Duration,
    pub(crate) transport_wait: Duration,
}

fn bundle_timing_event(
    event: &str,
    stable_request_id: &str,
    raw_request_id: &str,
    plan: &Issue23RequestPlan,
    scheduled_offset_ms: Option<u64>,
) -> serde_json::Value {
    let logical_payload_bytes = plan
        .operations
        .iter()
        .map(|operation| operation.logical_payload_bytes as u64)
        .sum::<u64>();
    let mut value = serde_json::json!({
        "event": event,
        "request_id": stable_request_id,
        "raw_request_id": raw_request_id,
        "cohort_id": plan.cohort_id,
        "smooth_slot": plan.smooth_slot,
        "operation_count": plan.operations.len(),
        "logical_payload_bytes": logical_payload_bytes,
    });
    if let Some(offset) = scheduled_offset_ms {
        value
            .as_object_mut()
            .expect("bundle timing event is an object")
            .insert("scheduled_offset_ms".into(), serde_json::json!(offset));
    }
    value
}

#[cfg(feature = "rdma")]
pub(crate) struct PreparedIssue23Bundle {
    stable_request_id: String,
    raw_request_id: String,
    plan: Issue23RequestPlan,
    eligible_at: Instant,
    operations: Vec<ObservedOperation>,
    rdma_batch: Option<pegaflow_transfer::PreparedTransferBatch>,
    result_tx: oneshot::Sender<Result<Issue23Completion, String>>,
}

#[cfg(feature = "rdma")]
#[derive(Default)]
struct CohortGateState {
    cohorts: HashMap<String, Vec<PreparedIssue23Bundle>>,
}

pub(crate) struct Issue23CausalExperiment {
    config: Issue23ExperimentConfig,
    requests: HashMap<String, Issue23RequestPlan>,
    expected_hidden: HashMap<Vec<u8>, u64>,
    trace: Mutex<File>,
    trace_origin: Instant,
    active: AtomicBool,
    hidden: RwLock<HashMap<Vec<u8>, Arc<SealedBlock>>>,
    #[cfg(feature = "rdma")]
    gate: Mutex<CohortGateState>,
}

impl Issue23TransferPlan {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported Issue23 transfer plan schema {}",
                self.schema_version
            ));
        }
        if self.protocol_id.is_empty() || self.block_id.is_empty() {
            return Err("protocol_id and block_id must not be empty".into());
        }
        if self.qps_per_peer != 2 {
            return Err(format!(
                "Issue23 protocol freezes qps_per_peer=2, got {}",
                self.qps_per_peer
            ));
        }
        if self.requests.is_empty() {
            return Err("Issue23 transfer plan must contain requests".into());
        }

        let mut request_ids = HashSet::new();
        let mut operation_ids = HashSet::new();
        let mut cohorts: HashMap<&str, Vec<usize>> = HashMap::new();
        for request in &self.requests {
            if request.request_id.is_empty() || request.cohort_id.is_empty() {
                return Err("request_id and cohort_id must not be empty".into());
            }
            if !request_ids.insert(request.request_id.as_str()) {
                return Err(format!("duplicate request_id {}", request.request_id));
            }
            if request.smooth_slot >= EXPECTED_COHORT_SIZE {
                return Err(format!(
                    "request {} has invalid smooth_slot {}",
                    request.request_id, request.smooth_slot
                ));
            }
            if request.operations.is_empty() {
                return Err(format!(
                    "request {} has an empty transfer bundle",
                    request.request_id
                ));
            }
            cohorts
                .entry(&request.cohort_id)
                .or_default()
                .push(request.smooth_slot);
            for (index, operation) in request.operations.iter().enumerate() {
                if operation.logical_payload_bytes == 0 {
                    return Err(format!(
                        "operation {} has zero payload bytes",
                        operation.operation_id
                    ));
                }
                if operation.segment != "k" && operation.segment != "v" {
                    return Err(format!(
                        "operation {} has invalid segment {}",
                        operation.operation_id, operation.segment
                    ));
                }
                if operation.source.is_empty() || operation.destination.is_empty() {
                    return Err(format!(
                        "operation {} has an empty source or destination",
                        operation.operation_id
                    ));
                }
                if operation.transfer_type != "load" {
                    return Err(format!(
                        "operation {} has invalid transfer_type {}",
                        operation.operation_id, operation.transfer_type
                    ));
                }
                if operation.qp_index != index % self.qps_per_peer {
                    return Err(format!(
                        "operation {} has qp_index {}, expected {}",
                        operation.operation_id,
                        operation.qp_index,
                        index % self.qps_per_peer
                    ));
                }
                if decode_hex(&operation.block_hash_hex).is_err() {
                    return Err(format!(
                        "operation {} has invalid block_hash_hex",
                        operation.operation_id
                    ));
                }
                if !operation_ids.insert(operation.operation_id.as_str()) {
                    return Err(format!("duplicate operation_id {}", operation.operation_id));
                }
            }
        }

        for (cohort, slots) in cohorts {
            let mut sorted = slots;
            sorted.sort_unstable();
            if sorted != [0, 1, 2, 3] {
                return Err(format!(
                    "cohort {cohort} must contain smooth slots [0,1,2,3], got {sorted:?}"
                ));
            }
        }
        Ok(())
    }
}

impl Issue23CausalExperiment {
    pub(crate) fn new(config: Issue23ExperimentConfig) -> Result<Arc<Self>, String> {
        if let Some(plan) = &config.plan {
            plan.validate()?;
        }
        if let Some(parent) = config.trace_path.parent()
            && !parent.is_dir()
        {
            return Err(format!(
                "Issue23 trace parent does not exist: {}",
                parent.display()
            ));
        }
        let trace = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&config.trace_path)
            .map_err(|error| {
                format!(
                    "create Issue23 trace {}: {error}",
                    config.trace_path.display()
                )
            })?;

        let requests = config
            .plan
            .as_ref()
            .map(|plan| {
                plan.requests
                    .iter()
                    .cloned()
                    .map(|request| (request.request_id.clone(), request))
                    .collect()
            })
            .unwrap_or_default();
        let expected_hidden = expected_hidden_layout(config.plan.as_ref())?;
        let experiment = Arc::new(Self {
            config,
            requests,
            expected_hidden,
            trace: Mutex::new(trace),
            trace_origin: Instant::now(),
            active: AtomicBool::new(false),
            hidden: RwLock::new(HashMap::new()),
            #[cfg(feature = "rdma")]
            gate: Mutex::new(CohortGateState::default()),
        });
        experiment.write_event(&serde_json::json!({
            "event": "experiment_opened",
            "protocol_id": experiment.config.plan.as_ref().map(|plan| plan.protocol_id.as_str()),
            "block_id": experiment.config.plan.as_ref().map(|plan| plan.block_id.as_str()),
            "backend": experiment.config.backend,
            "schedule": experiment.config.schedule,
        }))?;
        Ok(experiment)
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    pub(crate) fn backend(&self) -> Issue23Backend {
        self.config.backend
    }

    pub(crate) fn should_cache_remote_fetch(&self) -> bool {
        false
    }

    pub(crate) fn has_frozen_plan(&self) -> bool {
        self.config.plan.is_some()
    }

    pub(crate) fn frozen_remote_prefix_len(
        &self,
        raw_request_id: &str,
        requested_hashes: &[Vec<u8>],
        available_prefix_len: usize,
    ) -> Result<usize, String> {
        if !self.has_frozen_plan() {
            return Ok(available_prefix_len);
        }
        if available_prefix_len > requested_hashes.len() {
            return Err(format!(
                "remote prefix length {available_prefix_len} exceeds requested block count {}",
                requested_hashes.len()
            ));
        }

        let stable_request_id = stable_request_id(raw_request_id)?;
        let plan = self.requests.get(&stable_request_id).ok_or_else(|| {
            format!("request {stable_request_id} is absent from the frozen transfer plan")
        })?;
        let mut planned_hashes = Vec::new();
        let mut seen = HashSet::new();
        for operation in &plan.operations {
            let hash = decode_hex(&operation.block_hash_hex)?;
            if seen.insert(hash.clone()) {
                planned_hashes.push(hash);
            }
        }
        if planned_hashes.len() > available_prefix_len {
            return Err(format!(
                "request {stable_request_id} remote prefix is shorter than the frozen plan: available={available_prefix_len} expected={}",
                planned_hashes.len()
            ));
        }
        if requested_hashes.get(..planned_hashes.len()) != Some(planned_hashes.as_slice()) {
            return Err(format!(
                "request {stable_request_id} remote prefix hashes differ from the frozen plan"
            ));
        }
        Ok(planned_hashes.len())
    }

    /// Retain one hidden copy of every unique restored object while leaving it
    /// invisible to the normal read cache. Duplicate transfers are validated
    /// and immediately released after their serving lease completes.
    pub(crate) fn stage_hidden_blocks(
        &self,
        blocks: &[(BlockKey, Arc<SealedBlock>)],
    ) -> Result<(), String> {
        if !self.has_frozen_plan() {
            return Ok(());
        }
        if self.is_active() {
            return Err("cannot add hidden objects after Issue23 activation".into());
        }
        let mut hidden = self.hidden.write();
        for (key, block) in blocks {
            let expected_bytes = self.expected_hidden.get(&key.hash).ok_or_else(|| {
                format!(
                    "restored object {} is absent from the frozen hidden snapshot",
                    encode_hex(&key.hash)
                )
            })?;
            if block.memory_footprint() != *expected_bytes {
                return Err(format!(
                    "restored object {} size mismatch: observed={} expected={}",
                    encode_hex(&key.hash),
                    block.memory_footprint(),
                    expected_bytes
                ));
            }
            hidden
                .entry(key.hash.clone())
                .or_insert_with(|| Arc::clone(block));
        }
        Ok(())
    }

    pub(crate) fn activate_staged_hidden(&self) -> Result<Issue23ActivationStats, String> {
        if self.config.plan.is_none() {
            return Err("cannot activate Issue23 experiment without a frozen plan".into());
        }
        if self.is_active() {
            return Err("Issue23 experiment is already active".into());
        }
        let hidden = self.hidden.read();
        if hidden.len() != self.expected_hidden.len() {
            return Err(format!(
                "hidden snapshot block-count mismatch: observed={} expected={}",
                hidden.len(),
                self.expected_hidden.len()
            ));
        }
        let mut hidden_bytes = 0u64;
        for (hash, expected_bytes) in &self.expected_hidden {
            let block = hidden
                .get(hash)
                .ok_or_else(|| format!("hidden snapshot is missing block {}", encode_hex(hash)))?;
            if block.memory_footprint() != *expected_bytes {
                return Err(format!(
                    "hidden block {} size mismatch: observed={} expected={}",
                    encode_hex(hash),
                    block.memory_footprint(),
                    expected_bytes
                ));
            }
            hidden_bytes = hidden_bytes
                .checked_add(*expected_bytes)
                .ok_or_else(|| "hidden snapshot byte count overflow".to_string())?;
        }
        let stats = Issue23ActivationStats {
            hidden_blocks: hidden.len(),
            hidden_bytes,
        };
        drop(hidden);
        self.write_event(&serde_json::json!({
            "event": "experiment_activated",
            "hidden_blocks": stats.hidden_blocks,
            "hidden_bytes": stats.hidden_bytes,
        }))?;
        self.active.store(true, Ordering::Release);
        Ok(stats)
    }

    pub(crate) fn capture_bundle(
        &self,
        raw_request_id: &str,
        operations: &[ObservedOperation],
    ) -> Result<String, String> {
        let stable_request_id = stable_request_id(raw_request_id)?;
        if self.has_frozen_plan() {
            // Snapshot restoration is part of the frozen protocol too. Do
            // not allow an apparently complete hidden object set built from
            // request bundles whose operation manifest has drifted.
            self.request_plan(&stable_request_id, operations)?;
        }
        self.write_event(&serde_json::json!({
            "event": "bundle_observed",
            "phase": if self.is_active() { "measured" } else { "prepare" },
            "request_id": stable_request_id,
            "raw_request_id": raw_request_id,
            "operations": operations,
        }))?;
        Ok(stable_request_id)
    }

    pub(crate) fn local_source(
        &self,
        block_hash_hex: &str,
        slot_index: usize,
        segment: &str,
        expected_bytes: usize,
    ) -> Result<u64, String> {
        let hash = decode_hex(block_hash_hex)?;
        let hidden = self.hidden.read();
        let block = hidden
            .get(&hash)
            .ok_or_else(|| format!("hidden source block {block_hash_hex} is missing"))?;
        let slot = block.get_slot(slot_index).ok_or_else(|| {
            format!("hidden source block {block_hash_hex} has no slot {slot_index}")
        })?;
        let segment_index = match segment {
            "k" => 0,
            "v" => 1,
            other => return Err(format!("invalid segment {other}")),
        };
        let actual_bytes = slot.segment_size(segment_index).ok_or_else(|| {
            format!(
                "hidden source block {block_hash_hex} slot {slot_index} has no {segment} segment"
            )
        })?;
        if actual_bytes != expected_bytes {
            return Err(format!(
                "hidden source size mismatch for {block_hash_hex}/{slot_index}/{segment}: observed={actual_bytes} expected={expected_bytes}"
            ));
        }
        Ok(slot
            .segment_ptr(segment_index)
            .expect("segment size implies a pointer")
            .as_ptr() as u64)
    }

    pub(crate) fn request_plan(
        &self,
        stable_request_id: &str,
        observed: &[ObservedOperation],
    ) -> Result<Issue23RequestPlan, String> {
        let plan = self.requests.get(stable_request_id).ok_or_else(|| {
            format!("request {stable_request_id} is absent from the frozen transfer plan")
        })?;
        let observed_plan: Vec<_> = observed.iter().map(ObservedOperation::as_plan).collect();
        if observed_plan != plan.operations {
            return Err(format!(
                "request {stable_request_id} transfer manifest drift: observed {} operations, expected {}",
                observed_plan.len(),
                plan.operations.len()
            ));
        }
        Ok(plan.clone())
    }

    #[cfg(feature = "rdma")]
    pub(crate) async fn gate_bundle(
        self: &Arc<Self>,
        rdma: Arc<RdmaTransport>,
        remote_addr: String,
        raw_request_id: String,
        stable_request_id: String,
        operations: Vec<ObservedOperation>,
        rdma_batch: Option<pegaflow_transfer::PreparedTransferBatch>,
    ) -> Result<Issue23Completion, String> {
        let plan = self.request_plan(&stable_request_id, &operations)?;
        let eligible_at = Instant::now();
        self.write_event_at(
            &bundle_timing_event(
                "bundle_eligible",
                &stable_request_id,
                &raw_request_id,
                &plan,
                None,
            ),
            eligible_at,
        )?;

        let (result_tx, result_rx) = oneshot::channel();
        let bundle = PreparedIssue23Bundle {
            stable_request_id,
            raw_request_id,
            plan: plan.clone(),
            eligible_at,
            operations,
            rdma_batch,
            result_tx,
        };

        let ready = {
            let mut gate = self.gate.lock();
            let cohort = gate.cohorts.entry(plan.cohort_id.clone()).or_default();
            if cohort
                .iter()
                .any(|existing| existing.plan.smooth_slot == plan.smooth_slot)
            {
                return Err(format!(
                    "cohort {} received duplicate smooth slot {}",
                    plan.cohort_id, plan.smooth_slot
                ));
            }
            cohort.push(bundle);
            if cohort.len() == EXPECTED_COHORT_SIZE {
                gate.cohorts.remove(&plan.cohort_id)
            } else {
                None
            }
        };

        if let Some(cohort) = ready {
            // The fourth eligible request is already the cohort coordinator.
            // Running the short scheduling loop here avoids adding an
            // unrelated executor-spawn delay to the frozen T_g deadline.
            Arc::clone(self).run_cohort(rdma, remote_addr, cohort).await;
        }

        result_rx
            .await
            .map_err(|_| "Issue23 cohort coordinator dropped result channel".to_string())?
    }

    #[cfg(feature = "rdma")]
    async fn run_cohort(
        self: Arc<Self>,
        rdma: Arc<RdmaTransport>,
        remote_addr: String,
        mut cohort: Vec<PreparedIssue23Bundle>,
    ) {
        cohort.sort_by_key(|bundle| bundle.plan.smooth_slot);
        let tg = cohort
            .iter()
            .map(|bundle| bundle.eligible_at)
            .max()
            .expect("complete cohort is non-empty");

        match self.config.schedule {
            Issue23Schedule::Burst => {
                wait_until_precise(tg + Duration::from_millis(BURST_OFFSET_MS)).await;
                let posted = self.post_burst_cohort(&rdma, &remote_addr, cohort).await;
                let successful_posts: Vec<_> = posted
                    .iter()
                    .filter_map(|post| post.as_ref().ok().map(|pending| pending.posted_at))
                    .collect();
                if let (Some(first), Some(last)) =
                    (successful_posts.iter().min(), successful_posts.iter().max())
                {
                    let spread = last.duration_since(*first);
                    let _ = self.write_event(&serde_json::json!({
                        "event": "cohort_posting_spread",
                        "cohort_id": posted.iter().find_map(|post| post.as_ref().ok().map(|pending| pending.cohort_id.as_str())),
                        "posting_spread_ns": spread.as_nanos(),
                        "limit_ns": MAX_BURST_POSTING_SPREAD.as_nanos(),
                        "pass": spread <= MAX_BURST_POSTING_SPREAD,
                    }));
                }
                for post in posted {
                    self.finish_post(post).await;
                }
            }
            Issue23Schedule::Smooth => {
                // Give every smooth slot its own blocking scheduler. Keeping the
                // deadline wait and synchronous RDMA enqueue off the async
                // request worker prevents one descheduled slot from collapsing
                // the rest of the cohort onto the same late timestamp.
                let mut tasks = Vec::with_capacity(cohort.len());
                for bundle in cohort {
                    let release_at =
                        tg + Duration::from_millis(SMOOTH_OFFSETS_MS[bundle.plan.smooth_slot]);
                    let experiment = Arc::clone(&self);
                    let rdma = Arc::clone(&rdma);
                    let remote_addr = remote_addr.clone();
                    tasks.push(tokio::task::spawn_blocking(move || {
                        wait_until_precise_blocking(release_at);
                        experiment.post_bundle(&rdma, &remote_addr, bundle)
                    }));
                }
                for task in tasks {
                    match task.await {
                        Ok(post) => self.finish_post(post).await,
                        Err(error) => {
                            let _ = self.write_event(&serde_json::json!({
                                "event": "cohort_post_task_failed",
                                "schedule": "smooth",
                                "error": error.to_string(),
                            }));
                        }
                    }
                }
            }
        }
    }

    #[cfg(feature = "rdma")]
    async fn post_burst_cohort(
        self: &Arc<Self>,
        rdma: &Arc<RdmaTransport>,
        remote_addr: &str,
        cohort: Vec<PreparedIssue23Bundle>,
    ) -> Vec<Result<PendingBundle, FailedBundle>> {
        // Prepare all blocking workers before releasing the barrier. This
        // makes the frozen burst deadline the common release point instead of
        // serializing four submit_prepared_batch calls on one runtime worker.
        let barrier = Arc::new(std::sync::Barrier::new(cohort.len() + 1));
        let mut tasks = Vec::with_capacity(cohort.len());
        for bundle in cohort {
            let experiment = Arc::clone(self);
            let rdma = Arc::clone(rdma);
            let remote_addr = remote_addr.to_owned();
            let barrier = Arc::clone(&barrier);
            tasks.push(tokio::task::spawn_blocking(move || {
                barrier.wait();
                experiment.post_bundle(&rdma, &remote_addr, bundle)
            }));
        }
        barrier.wait();

        let mut posted = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(post) => posted.push(post),
                Err(error) => {
                    let _ = self.write_event(&serde_json::json!({
                        "event": "cohort_post_task_failed",
                        "error": error.to_string(),
                    }));
                }
            }
        }
        posted
    }

    #[cfg(feature = "rdma")]
    fn post_bundle(
        &self,
        rdma: &RdmaTransport,
        _remote_addr: &str,
        mut bundle: PreparedIssue23Bundle,
    ) -> Result<PendingBundle, FailedBundle> {
        let post_start = Instant::now();
        let completion = match self.config.backend {
            Issue23Backend::Rdma => {
                let Some(prepared) = bundle.rdma_batch.take() else {
                    return Err(FailedBundle::new(bundle, "RDMA bundle was not prepared"));
                };
                match rdma.engine().submit_prepared_batch(prepared) {
                    Ok(receivers) => PendingCompletion::Rdma(receivers),
                    Err(error) => {
                        return Err(FailedBundle::new(
                            bundle,
                            format!("RDMA submit failed: {error}"),
                        ));
                    }
                }
            }
            Issue23Backend::LocalCopy => {
                let copies: Vec<_> = bundle
                    .operations
                    .iter()
                    .map(|operation| {
                        (
                            operation.remote_addr,
                            operation.local_addr,
                            operation.logical_payload_bytes,
                        )
                    })
                    .collect();
                PendingCompletion::Local(tokio::task::spawn_blocking(move || {
                    let mut bytes = 0usize;
                    for (source, destination, len) in copies {
                        // SAFETY: source belongs to the activated hidden snapshot and
                        // destination belongs to the staged allocation held by the
                        // awaiting fetch task. The frozen manifest guarantees equal,
                        // non-overlapping segment lengths.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                source as *const u8,
                                destination as *mut u8,
                                len,
                            );
                        }
                        bytes = bytes.saturating_add(len);
                    }
                    bytes
                }))
            }
        };
        let posted_at = Instant::now();
        let submit = posted_at.duration_since(post_start);
        let trace_error = self
            .write_event_at(
                &bundle_timing_event(
                    "bundle_posted",
                    &bundle.stable_request_id,
                    &bundle.raw_request_id,
                    &bundle.plan,
                    Some(match self.config.schedule {
                        Issue23Schedule::Burst => BURST_OFFSET_MS,
                        Issue23Schedule::Smooth => SMOOTH_OFFSETS_MS[bundle.plan.smooth_slot],
                    }),
                ),
                posted_at,
            )
            .err();
        Ok(PendingBundle {
            stable_request_id: bundle.stable_request_id,
            raw_request_id: bundle.raw_request_id,
            cohort_id: bundle.plan.cohort_id,
            eligible_at: bundle.eligible_at,
            posted_at,
            submit,
            completion,
            trace_error,
            result_tx: bundle.result_tx,
        })
    }

    #[cfg(feature = "rdma")]
    async fn finish_post(&self, post: Result<PendingBundle, FailedBundle>) {
        let pending = match post {
            Ok(pending) => pending,
            Err(failed) => {
                let _ = failed.result_tx.send(Err(failed.error));
                return;
            }
        };
        let result = match pending.completion {
            PendingCompletion::Rdma(receivers) => {
                let mut bytes = 0usize;
                let mut error = None;
                for receiver in receivers {
                    match receiver.await {
                        Ok(Ok(completed)) => bytes = bytes.saturating_add(completed),
                        Ok(Err(cause)) => {
                            error.get_or_insert_with(|| format!("RDMA transfer failed: {cause}"));
                        }
                        Err(_) => {
                            error.get_or_insert_with(|| "RDMA transfer channel closed".to_string());
                        }
                    }
                }
                error.map_or(Ok(bytes), Err)
            }
            PendingCompletion::Local(handle) => handle
                .await
                .map_err(|error| format!("local copy worker failed: {error}")),
        };
        let completed_at = Instant::now();
        let status = if result.is_ok() { "ok" } else { "error" };
        let completed_bytes = result.as_ref().copied().unwrap_or_default();
        let trace_result = self.write_event_at(
            &serde_json::json!({
                "event": "bundle_completed",
                "request_id": pending.stable_request_id,
                "raw_request_id": pending.raw_request_id,
                "cohort_id": pending.cohort_id,
                "status": status,
                "completed_payload_bytes": completed_bytes,
            }),
            completed_at,
        );
        let result = result
            .and_then(|bytes| pending.trace_error.map_or(Ok(bytes), Err))
            .map(|_| Issue23Completion {
                eligible_wait: pending.posted_at.duration_since(pending.eligible_at),
                submit: pending.submit,
                transport_wait: completed_at.duration_since(pending.posted_at),
            })
            .and_then(|completion| trace_result.map(|_| completion));
        let _ = pending.result_tx.send(result);
    }

    fn write_event<T: Serialize>(&self, event: &T) -> Result<(), String> {
        self.write_event_at(event, Instant::now())
    }

    fn write_event_at<T: Serialize>(&self, event: &T, event_at: Instant) -> Result<(), String> {
        let mut value = serde_json::to_value(event)
            .map_err(|error| format!("serialize Issue23 trace event: {error}"))?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| "Issue23 trace event must serialize as an object".to_string())?;
        object.insert(
            "wall_time_unix_ns".into(),
            serde_json::json!(system_time_ns()?),
        );
        object.insert(
            "monotonic_ns".into(),
            serde_json::json!(event_at.duration_since(self.trace_origin).as_nanos()),
        );
        let mut trace = self.trace.lock();
        serde_json::to_writer(&mut *trace, &value)
            .map_err(|error| format!("write Issue23 trace event: {error}"))?;
        trace
            .write_all(b"\n")
            .and_then(|_| trace.flush())
            .map_err(|error| format!("flush Issue23 trace: {error}"))
    }
}

#[cfg(feature = "rdma")]
async fn wait_until_precise(deadline: Instant) {
    if let Some(coarse_deadline) = deadline.checked_sub(PRECISE_WAIT_GUARD)
        && Instant::now() < coarse_deadline
    {
        tokio::time::sleep_until(coarse_deadline.into()).await;
    }
    while Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

#[cfg(feature = "rdma")]
fn wait_until_precise_blocking(deadline: Instant) {
    if let Some(coarse_deadline) = deadline.checked_sub(PRECISE_WAIT_GUARD) {
        let now = Instant::now();
        if now < coarse_deadline {
            std::thread::sleep(coarse_deadline.duration_since(now));
        }
    }
    while Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

#[cfg(feature = "rdma")]
struct FailedBundle {
    error: String,
    result_tx: oneshot::Sender<Result<Issue23Completion, String>>,
}

#[cfg(feature = "rdma")]
impl FailedBundle {
    fn new(bundle: PreparedIssue23Bundle, error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            result_tx: bundle.result_tx,
        }
    }
}

#[cfg(feature = "rdma")]
struct PendingBundle {
    stable_request_id: String,
    raw_request_id: String,
    cohort_id: String,
    eligible_at: Instant,
    posted_at: Instant,
    submit: Duration,
    completion: PendingCompletion,
    trace_error: Option<String>,
    result_tx: oneshot::Sender<Result<Issue23Completion, String>>,
}

#[cfg(feature = "rdma")]
enum PendingCompletion {
    Rdma(Vec<mea::oneshot::Receiver<pegaflow_transfer::Result<usize>>>),
    Local(tokio::task::JoinHandle<usize>),
}

fn expected_hidden_layout(
    plan: Option<&Issue23TransferPlan>,
) -> Result<HashMap<Vec<u8>, u64>, String> {
    let mut segments: HashMap<(Vec<u8>, usize, String), usize> = HashMap::new();
    for request in plan.into_iter().flat_map(|plan| &plan.requests) {
        for operation in &request.operations {
            let hash = decode_hex(&operation.block_hash_hex)?;
            let key = (hash, operation.slot_index, operation.segment.clone());
            if let Some(previous) = segments.insert(key, operation.logical_payload_bytes)
                && previous != operation.logical_payload_bytes
            {
                return Err(format!(
                    "inconsistent payload size for operation {}",
                    operation.operation_id
                ));
            }
        }
    }
    let mut blocks = HashMap::new();
    for ((hash, _, _), bytes) in segments {
        let total = blocks.entry(hash).or_insert(0u64);
        *total = total
            .checked_add(bytes as u64)
            .ok_or_else(|| "hidden block byte count overflow".to_string())?;
    }
    Ok(blocks)
}

pub(crate) fn stable_request_id(raw: &str) -> Result<String, String> {
    let body = raw.strip_prefix("cmpl-").unwrap_or(raw);
    let (prefix, suffix) = body
        .rsplit_once('-')
        .ok_or_else(|| format!("request id {raw} lacks the frozen vLLM suffix"))?;
    if suffix.len() != 8 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(format!("request id {raw} has an invalid vLLM suffix"));
    }
    let stable = prefix
        .strip_suffix("-0")
        .ok_or_else(|| format!("request id {raw} lacks the frozen completion index"))?;
    if stable.is_empty() {
        return Err(format!("request id {raw} has an empty stable identity"));
    }
    Ok(stable.to_string())
}

pub(crate) fn operation_id(
    stable_request_id: &str,
    block_hash: &[u8],
    slot_index: usize,
    segment: &str,
) -> String {
    format!(
        "{stable_request_id}|{}|{slot_index}|{segment}",
        encode_hex(block_hash)
    )
}

pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hex(value: &str) -> Result<Vec<u8>, String> {
    if !value.len().is_multiple_of(2) {
        return Err("hex string has odd length".into());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = decode_nibble(pair[0])?;
            let low = decode_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn decode_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex byte {byte}")),
    }
}

fn system_time_ns() -> Result<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .map_err(|error| format!("system clock is before UNIX epoch: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(request: &str, hash: &str, index: usize) -> Issue23OperationPlan {
        Issue23OperationPlan {
            operation_id: format!("{request}|{hash}|0|k"),
            block_hash_hex: hash.into(),
            slot_index: 0,
            segment: "k".into(),
            source: "10.17.119.1:50055".into(),
            destination: "10.17.119.71:50055".into(),
            transfer_type: "load".into(),
            logical_payload_bytes: 64,
            qp_index: index % 2,
        }
    }

    fn valid_plan() -> Issue23TransferPlan {
        Issue23TransferPlan {
            schema_version: 1,
            protocol_id: "p".into(),
            block_id: "b".into(),
            qps_per_peer: 2,
            requests: (0..4)
                .map(|slot| {
                    let request = format!("req-{slot}");
                    Issue23RequestPlan {
                        request_id: request.clone(),
                        cohort_id: "c0".into(),
                        smooth_slot: slot,
                        operations: vec![operation(&request, &format!("{slot:02x}"), 0)],
                    }
                })
                .collect(),
            freeze_provenance: None,
        }
    }

    #[test]
    fn stable_request_identity_strips_only_frozen_vllm_affixes() {
        assert_eq!(
            stable_request_id("cmpl-issue23-measure-0007-0-a26e1651").unwrap(),
            "issue23-measure-0007"
        );
        assert!(stable_request_id("cmpl-issue23-measure-0007").is_err());
    }

    #[test]
    fn plan_requires_one_of_each_smooth_slot() {
        let mut plan = valid_plan();
        assert!(plan.validate().is_ok());
        plan.requests[3].smooth_slot = 2;
        assert!(plan.validate().unwrap_err().contains("smooth slots"));
    }

    #[test]
    fn hidden_layout_deduplicates_objects_across_requests() {
        let mut plan = valid_plan();
        plan.requests[1].operations[0].block_hash_hex = "00".into();
        plan.requests[1].operations[0].operation_id = "req-1|00|0|k".into();
        let layout = expected_hidden_layout(Some(&plan)).unwrap();
        assert_eq!(layout.len(), 3);
        assert_eq!(layout.get(&vec![0]), Some(&64));
    }

    #[test]
    fn frozen_remote_prefix_uses_the_request_manifest_before_transfer() {
        let mut plan = valid_plan();
        plan.requests[0]
            .operations
            .push(operation("req-0", "01", 1));
        let temp = tempfile::tempdir().unwrap();
        let experiment = Issue23CausalExperiment::new(Issue23ExperimentConfig {
            plan: Some(plan),
            backend: Issue23Backend::Rdma,
            schedule: Issue23Schedule::Smooth,
            trace_path: temp.path().join("trace.jsonl"),
        })
        .unwrap();

        let requested = vec![vec![0], vec![1], vec![2]];
        assert_eq!(
            experiment
                .frozen_remote_prefix_len("cmpl-req-0-0-deadbeef", &requested, 3)
                .unwrap(),
            2
        );
        assert!(
            experiment
                .frozen_remote_prefix_len("cmpl-req-0-0-deadbeef", &requested, 1)
                .unwrap_err()
                .contains("shorter than the frozen plan")
        );
        assert!(
            experiment
                .frozen_remote_prefix_len("cmpl-req-0-0-deadbeef", &[vec![0], vec![2], vec![1]], 3,)
                .unwrap_err()
                .contains("hashes differ")
        );
    }

    #[test]
    fn plan_json_roundtrip_preserves_transport_identity() {
        let plan = valid_plan();
        let encoded = serde_json::to_vec(&plan).unwrap();
        let decoded: Issue23TransferPlan = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            serde_json::to_value(plan).unwrap()
        );
    }

    #[test]
    fn timing_event_size_is_independent_of_operation_identity_volume() {
        let mut plan = valid_plan().requests.remove(0);
        plan.operations = (0..2304)
            .map(|index| operation("req-0", &format!("{index:064x}"), index))
            .collect();
        let event = bundle_timing_event("bundle_posted", "req-0", "raw-0", &plan, Some(10));
        assert_eq!(event["operation_count"], 2304);
        assert_eq!(event["logical_payload_bytes"], 2304 * 64);
        assert_eq!(event["scheduled_offset_ms"], 10);
        assert!(event.get("operation_ids").is_none());
        assert!(serde_json::to_vec(&event).unwrap().len() < 512);
    }

    #[test]
    fn trace_event_uses_the_captured_event_instant() {
        let temp = tempfile::tempdir().unwrap();
        let trace_path = temp.path().join("trace.jsonl");
        let experiment = Issue23CausalExperiment::new(Issue23ExperimentConfig {
            plan: None,
            backend: Issue23Backend::Rdma,
            schedule: Issue23Schedule::Smooth,
            trace_path: trace_path.clone(),
        })
        .unwrap();
        let captured_at = experiment.trace_origin + Duration::from_millis(7);
        experiment
            .write_event_at(&serde_json::json!({"event": "captured"}), captured_at)
            .unwrap();
        let trace = std::fs::read_to_string(trace_path).unwrap();
        let event: serde_json::Value = serde_json::from_str(trace.lines().last().unwrap()).unwrap();
        assert_eq!(event["monotonic_ns"], 7_000_000);
    }

    #[test]
    fn frozen_restore_rejects_operation_manifest_drift() {
        let plan = valid_plan();
        let request_plan = plan.requests[0].clone();
        let temp = tempfile::tempdir().unwrap();
        let experiment = Issue23CausalExperiment::new(Issue23ExperimentConfig {
            plan: Some(plan),
            backend: Issue23Backend::Rdma,
            schedule: Issue23Schedule::Smooth,
            trace_path: temp.path().join("trace.jsonl"),
        })
        .unwrap();
        let mut observed: Vec<_> = request_plan
            .operations
            .iter()
            .map(|operation| ObservedOperation {
                operation_id: operation.operation_id.clone(),
                block_hash_hex: operation.block_hash_hex.clone(),
                slot_index: operation.slot_index,
                segment: operation.segment.clone(),
                source: operation.source.clone(),
                destination: operation.destination.clone(),
                transfer_type: operation.transfer_type.clone(),
                logical_payload_bytes: operation.logical_payload_bytes,
                qp_index: operation.qp_index,
                local_addr: 1,
                remote_addr: 2,
            })
            .collect();
        assert!(
            experiment
                .capture_bundle("cmpl-req-0-0-deadbeef", &observed)
                .is_ok()
        );
        observed[0].logical_payload_bytes += 1;
        assert!(
            experiment
                .capture_bundle("cmpl-req-0-0-deadbeef", &observed)
                .unwrap_err()
                .contains("manifest drift")
        );
    }
}
