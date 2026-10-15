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
#[cfg(feature = "rdma")]
use std::sync::mpsc;
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
// The experiment engine is CPU-affined with other service workers.  A 2 ms
// guard was not sufficient to cover an observed scheduler wake-up delay, even
// though the subsequent synchronous transport submit took less than 1 ms.
// Spinning for the final 10 ms is cheap at the frozen 0.5 QPS offered load and
// keeps the recorded successful-post timestamp tied to the frozen deadline.
const PRECISE_WAIT_GUARD: Duration = Duration::from_millis(10);
#[cfg(feature = "rdma")]
const SMOOTH_SCHEDULER_PRIORITY: libc::c_int = 10;

#[cfg(feature = "rdma")]
type SmoothSchedulerJob = Box<dyn FnOnce() + Send + 'static>;

/// A pre-started scheduler for all four smooth slots.
///
/// The worker stays blocked on a channel between cohorts. Set
/// PEGAFLOW_ISSUE23_DEFAULT_SCHEDULER=1 to retain SCHED_OTHER at priority zero.
#[cfg(feature = "rdma")]
struct SmoothScheduler {
    sender: mpsc::Sender<SmoothSchedulerJob>,
    realtime_error: Option<String>,
    default_policy: bool,
}

#[cfg(feature = "rdma")]
impl SmoothScheduler {
    fn new() -> Result<Self, String> {
        Self::with_default_policy(
            std::env::var("PEGAFLOW_ISSUE23_DEFAULT_SCHEDULER").as_deref() == Ok("1"),
        )
    }

    fn with_default_policy(default_policy: bool) -> Result<Self, String> {
        let (sender, receiver) = mpsc::channel::<SmoothSchedulerJob>();
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("issue23-smooth-scheduler".into())
            .spawn(move || {
                let realtime = if default_policy {
                    verify_default_scheduler()
                } else {
                    configure_realtime_scheduler()
                };
                let _ = startup_sender.send(realtime.clone());
                for job in receiver {
                    job();
                }
            })
            .map_err(|error| format!("start Issue23 smooth scheduler: {error}"))?;
        let realtime_error = startup_receiver
            .recv()
            .map_err(|_| "Issue23 smooth scheduler exited during startup".to_string())?
            .err();
        Ok(Self {
            sender,
            realtime_error,
            default_policy,
        })
    }

    fn submit(&self, job: SmoothSchedulerJob) -> Result<(), String> {
        self.sender
            .send(job)
            .map_err(|_| "Issue23 smooth scheduler is unavailable".to_string())
    }

    fn require_scheduler(&self) -> Result<(), String> {
        self.realtime_error.as_ref().map_or(Ok(()), |error| {
            Err(format!(
                "Issue23 smooth scheduler configuration failed: {error}"
            ))
        })
    }
}

#[cfg(feature = "rdma")]
fn verify_default_scheduler() -> Result<(), String> {
    let mut policy = 0;
    let mut parameter = libc::sched_param { sched_priority: 0 };
    // SAFETY: both output pointers are valid and pthread_self identifies this worker.
    let result =
        unsafe { libc::pthread_getschedparam(libc::pthread_self(), &mut policy, &mut parameter) };
    if result != 0 {
        return Err(std::io::Error::from_raw_os_error(result).to_string());
    }
    if policy != libc::SCHED_OTHER || parameter.sched_priority != 0 {
        return Err(format!(
            "expected SCHED_OTHER/0, got policy={policy} priority={}",
            parameter.sched_priority
        ));
    }
    Ok(())
}

#[cfg(feature = "rdma")]
fn configure_realtime_scheduler() -> Result<(), String> {
    let parameter = libc::sched_param {
        sched_priority: SMOOTH_SCHEDULER_PRIORITY,
    };
    // SAFETY: pthread_self returns the calling thread, parameter points to a
    // valid sched_param for the duration of the call, and SCHED_FIFO accepts
    // the configured positive priority on Linux.
    let result =
        unsafe { libc::pthread_setschedparam(libc::pthread_self(), libc::SCHED_FIFO, &parameter) };
    if result != 0 {
        return Err(std::io::Error::from_raw_os_error(result).to_string());
    }
    Ok(())
}

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

#[derive(Clone, Debug, Deserialize)]
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
    #[serde(default = "default_cohort_size")]
    pub cohort_size: usize,
    #[serde(default = "default_smooth_interval_ms")]
    pub smooth_interval_ms: u64,
    #[serde(default)]
    pub rdma_rate_bps: u64,
    pub requests: Vec<Issue23RequestPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freeze_provenance: Option<serde_json::Value>,
}

fn default_cohort_size() -> usize {
    4
}
fn default_smooth_interval_ms() -> u64 {
    10
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
    pub namespace: String,
    pub storage_group_id: u32,
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
    pub(crate) namespace: String,
    pub(crate) storage_group_id: u32,
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
            namespace: self.namespace.clone(),
            storage_group_id: self.storage_group_id,
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
    posted_event: serde_json::Value,
    result_txs: Vec<(Instant, oneshot::Sender<Result<Issue23Completion, String>>)>,
}

#[cfg(feature = "rdma")]
struct PreparedIssue23Part {
    raw_request_id: String,
    eligible_at: Instant,
    operations: Vec<ObservedOperation>,
    result_tx: oneshot::Sender<Result<Issue23Completion, String>>,
}

#[cfg(feature = "rdma")]
#[derive(Default)]
struct CohortGateState {
    cohorts: HashMap<String, Vec<PreparedIssue23Bundle>>,
    parts: HashMap<String, HashMap<(String, u32), PreparedIssue23Part>>,
    assembled_requests: HashSet<String>,
}

pub(crate) struct Issue23CausalExperiment {
    config: Issue23ExperimentConfig,
    requests: HashMap<String, Issue23RequestPlan>,
    expected_hidden: HashMap<BlockKey, u64>,
    trace: Mutex<File>,
    trace_origin: Instant,
    active: AtomicBool,
    hidden: RwLock<HashMap<BlockKey, Arc<SealedBlock>>>,
    #[cfg(feature = "rdma")]
    gate: Mutex<CohortGateState>,
    #[cfg(feature = "rdma")]
    smooth_scheduler: Option<SmoothScheduler>,
}

impl Issue23TransferPlan {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 2 {
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
        if !(2..=16).contains(&self.cohort_size) || self.smooth_interval_ms == 0 {
            return Err("cohort_size must be 2..16 and smooth_interval_ms positive".into());
        }
        self.smooth_interval_ms
            .checked_mul((self.cohort_size - 1) as u64)
            .ok_or("release schedule overflows")?;

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
            if request.smooth_slot >= self.cohort_size {
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
                if operation.namespace.is_empty() {
                    return Err(format!(
                        "operation {} has an empty namespace",
                        operation.operation_id
                    ));
                }
                let expected_id = operation_id(
                    &request.request_id,
                    &operation.namespace,
                    operation.storage_group_id,
                    &decode_hex(&operation.block_hash_hex)?,
                    operation.slot_index,
                    &operation.segment,
                );
                if operation.operation_id != expected_id {
                    return Err(format!(
                        "operation {} has an invalid identity",
                        operation.operation_id
                    ));
                }
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
            if sorted != (0..self.cohort_size).collect::<Vec<_>>() {
                return Err(format!(
                    "cohort {cohort} must contain all {} smooth slots, got {sorted:?}",
                    self.cohort_size
                ));
            }
        }
        Ok(())
    }
}

impl Issue23CausalExperiment {
    pub(crate) fn is_idle(&self) -> bool {
        #[cfg(feature = "rdma")]
        {
            let gate = self.gate.lock();
            gate.cohorts.is_empty() && gate.parts.is_empty()
        }
        #[cfg(not(feature = "rdma"))]
        {
            true
        }
    }

    pub(crate) fn inherit_hidden(&self, previous: &Self) -> Result<(), String> {
        let blocks = previous
            .hidden
            .read()
            .iter()
            .map(|(key, block)| (key.clone(), Arc::clone(block)))
            .collect::<Vec<_>>();
        self.stage_hidden_blocks(&blocks)
    }

    fn cohort_size(&self) -> usize {
        self.config.plan.as_ref().map_or(4, |plan| plan.cohort_size)
    }

    fn offset_ms(&self, slot: usize) -> u64 {
        let interval = self
            .config
            .plan
            .as_ref()
            .map_or(10, |plan| plan.smooth_interval_ms);
        match self.config.schedule {
            Issue23Schedule::Smooth => interval * slot as u64,
            Issue23Schedule::Burst => interval * (self.cohort_size() - 1) as u64 / 2,
        }
    }
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
        #[cfg(feature = "rdma")]
        let smooth_scheduler = (config.schedule == Issue23Schedule::Smooth)
            .then(SmoothScheduler::new)
            .transpose()?;
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
            #[cfg(feature = "rdma")]
            smooth_scheduler,
        });
        #[cfg(feature = "rdma")]
        let (scheduler_policy, scheduler_priority, scheduler_error) = experiment
            .smooth_scheduler
            .as_ref()
            .map_or((None, None, None), |scheduler| {
                (
                    Some(if scheduler.realtime_error.is_none() {
                        if scheduler.default_policy {
                            "sched_other"
                        } else {
                            "sched_fifo"
                        }
                    } else {
                        "unavailable"
                    }),
                    Some(if scheduler.default_policy {
                        0
                    } else {
                        SMOOTH_SCHEDULER_PRIORITY
                    }),
                    scheduler.realtime_error.as_deref(),
                )
            });
        #[cfg(not(feature = "rdma"))]
        let (scheduler_policy, scheduler_priority, scheduler_error) =
            (None::<&str>, None::<libc::c_int>, None::<&str>);
        experiment.write_event(&serde_json::json!({
            "event": "experiment_opened",
            "protocol_id": experiment.config.plan.as_ref().map(|plan| plan.protocol_id.as_str()),
            "block_id": experiment.config.plan.as_ref().map(|plan| plan.block_id.as_str()),
            "backend": experiment.config.backend,
            "schedule": experiment.config.schedule,
            "smooth_scheduler_policy": scheduler_policy,
            "smooth_scheduler_priority": scheduler_priority,
            "smooth_scheduler_error": scheduler_error,
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

    pub(crate) fn record_fetch_stage(
        &self,
        event: &str,
        raw_request_id: &str,
        namespace: &str,
        mut fields: serde_json::Value,
    ) -> Result<(), String> {
        let object = fields
            .as_object_mut()
            .ok_or("fetch telemetry must be an object")?;
        object.insert("event".into(), serde_json::json!(event));
        object.insert(
            "request_id".into(),
            serde_json::json!(stable_request_id(raw_request_id)?),
        );
        object.insert("raw_request_id".into(), serde_json::json!(raw_request_id));
        object.insert("namespace".into(), serde_json::json!(namespace));
        object.insert(
            "storage_group_id".into(),
            serde_json::json!(storage_group_id(raw_request_id, namespace)?),
        );
        object.insert(
            "phase".into(),
            serde_json::json!(if self.is_active() {
                "measured"
            } else {
                "prepare"
            }),
        );
        self.write_event(&fields)
    }

    pub(crate) fn frozen_remote_prefix_len(
        &self,
        raw_request_id: &str,
        namespace: &str,
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
        let group_id = storage_group_id(raw_request_id, namespace)?;
        for operation in plan
            .operations
            .iter()
            .filter(|op| op.namespace == namespace && op.storage_group_id == group_id)
        {
            let hash = decode_hex(&operation.block_hash_hex)?;
            if seen.insert(hash.clone()) {
                planned_hashes.push(hash);
            }
        }
        if planned_hashes.is_empty() {
            return Err(format!(
                "request {stable_request_id} has no frozen group {group_id} in namespace {namespace}"
            ));
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
        if self.is_active() {
            return Err("cannot add hidden objects after Issue23 activation".into());
        }
        let mut hidden = self.hidden.write();
        for (key, block) in blocks {
            let expected_bytes = if self.has_frozen_plan() {
                *self.expected_hidden.get(key).ok_or_else(|| {
                    format!(
                        "restored object {} is absent from the frozen hidden snapshot",
                        encode_hex(&key.hash)
                    )
                })?
            } else {
                block.memory_footprint()
            };
            if block.memory_footprint() != expected_bytes {
                return Err(format!(
                    "restored object {} size mismatch: observed={} expected={}",
                    encode_hex(&key.hash),
                    block.memory_footprint(),
                    expected_bytes
                ));
            }
            hidden
                .entry(key.clone())
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
        #[cfg(feature = "rdma")]
        if let Some(scheduler) = &self.smooth_scheduler {
            scheduler.require_scheduler()?;
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
        for (key, expected_bytes) in &self.expected_hidden {
            let block = hidden.get(key).ok_or_else(|| {
                format!(
                    "hidden snapshot is missing block {}:{}",
                    key.namespace,
                    encode_hex(&key.hash)
                )
            })?;
            if block.memory_footprint() != *expected_bytes {
                return Err(format!(
                    "hidden block {} size mismatch: observed={} expected={}",
                    encode_hex(&key.hash),
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
            self.request_part_plan(&stable_request_id, operations)?;
        }
        self.write_event(&serde_json::json!({
            "event": "bundle_part_observed",
            "phase": if self.is_active() { "measured" } else { "prepare" },
            "request_id": stable_request_id,
            "raw_request_id": raw_request_id,
            "storage_group_id": operations.first().map(|op| op.storage_group_id),
            "namespace": operations.first().map(|op| op.namespace.as_str()),
            "operations": operations,
        }))?;
        Ok(stable_request_id)
    }

    pub(crate) fn local_source(
        &self,
        namespace: &str,
        block_hash_hex: &str,
        slot_index: usize,
        segment: &str,
        expected_bytes: usize,
    ) -> Result<u64, String> {
        let hash = decode_hex(block_hash_hex)?;
        let hidden = self.hidden.read();
        let block = hidden
            .get(&BlockKey::new(namespace.to_string(), hash))
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

    fn request_part_plan(
        &self,
        request_id: &str,
        observed: &[ObservedOperation],
    ) -> Result<Issue23RequestPlan, String> {
        let first = observed.first().ok_or("empty Issue23 group bundle")?;
        if observed.iter().any(|op| {
            op.namespace != first.namespace || op.storage_group_id != first.storage_group_id
        }) {
            return Err("one fetch must contain exactly one namespace and storage group".into());
        }
        let plan = self.requests.get(request_id).ok_or_else(|| {
            format!("request {request_id} is absent from the frozen transfer plan")
        })?;
        let expected: Vec<_> = plan
            .operations
            .iter()
            .filter(|op| {
                op.namespace == first.namespace && op.storage_group_id == first.storage_group_id
            })
            .enumerate()
            .map(|(index, op)| {
                let mut op = op.clone();
                op.qp_index = index % 2;
                op
            })
            .collect();
        if expected.is_empty()
            || expected
                != observed
                    .iter()
                    .map(ObservedOperation::as_plan)
                    .collect::<Vec<_>>()
        {
            return Err(format!(
                "request {request_id} group {} transfer manifest drift",
                first.storage_group_id
            ));
        }
        Ok(plan.clone())
    }

    #[cfg(feature = "rdma")]
    pub(crate) async fn gate_bundle(
        self: &Arc<Self>,
        rdma: Option<Arc<RdmaTransport>>,
        remote_addr: String,
        raw_request_id: String,
        stable_request_id: String,
        operations: Vec<ObservedOperation>,
    ) -> Result<Issue23Completion, String> {
        let plan = self.request_part_plan(&stable_request_id, &operations)?;
        let part_at = Instant::now();
        let first = operations.first().ok_or("empty group bundle")?;
        let part_key = (first.namespace.clone(), first.storage_group_id);
        self.write_event_at(&serde_json::json!({
            "event": "bundle_part_eligible", "request_id": stable_request_id,
            "raw_request_id": raw_request_id, "namespace": part_key.0,
            "storage_group_id": part_key.1, "operation_count": operations.len(),
            "logical_payload_bytes": operations.iter().map(|op| op.logical_payload_bytes as u64).sum::<u64>(),
        }), part_at)?;
        let (result_tx, result_rx) = oneshot::channel();
        let expected_parts: HashSet<_> = plan
            .operations
            .iter()
            .map(|op| (op.namespace.clone(), op.storage_group_id))
            .collect();
        let parts = {
            let mut gate = self.gate.lock();
            if gate.assembled_requests.contains(&stable_request_id) {
                return Err(format!("request {stable_request_id} was already assembled"));
            }
            let parts = gate.parts.entry(stable_request_id.clone()).or_default();
            if parts.contains_key(&part_key) {
                return Err(format!(
                    "request {stable_request_id} has a repeated group {}",
                    part_key.1
                ));
            }
            parts.insert(
                part_key,
                PreparedIssue23Part {
                    raw_request_id,
                    eligible_at: part_at,
                    operations,
                    result_tx,
                },
            );
            if parts.len() == expected_parts.len() {
                gate.assembled_requests.insert(stable_request_id.clone());
                gate.parts.remove(&stable_request_id)
            } else {
                None
            }
        };
        let Some(parts) = parts else {
            return result_rx
                .await
                .map_err(|_| "Issue23 group assembler dropped result channel".to_string())?;
        };
        let part_count = parts.len();
        let groups_ready_at = Instant::now();
        let assembly_start = parts
            .values()
            .map(|part| part.eligible_at)
            .min()
            .ok_or("empty group assembly")?;
        let raw_request_id = parts
            .values()
            .find(|part| part.operations[0].storage_group_id == 0)
            .ok_or("frozen bundle lacks attention group")?
            .raw_request_id
            .clone();
        let mut by_id: HashMap<_, _> = parts
            .values()
            .flat_map(|part| part.operations.iter().cloned())
            .map(|op| (op.operation_id.clone(), op))
            .collect();
        let mut operations = Vec::with_capacity(plan.operations.len());
        for (index, expected) in plan.operations.iter().enumerate() {
            let Some(mut observed) = by_id.remove(&expected.operation_id) else {
                for part in parts.into_values() {
                    let _ = part
                        .result_tx
                        .send(Err("complete bundle lacks a frozen operation".into()));
                }
                return Err("complete bundle lacks a frozen operation".into());
            };
            observed.qp_index = index % 2;
            operations.push(observed);
        }
        let preparation = (|| {
            self.request_plan(&stable_request_id, &operations)?;
            if operations.iter().any(|op| op.source != remote_addr) {
                return Err("Issue23 complete bundle must use one frozen remote peer".to_string());
            }
            if self.config.backend == Issue23Backend::Rdma {
                let transport = rdma.as_ref().ok_or("RDMA transport is unavailable")?;
                let descs: Vec<_> = operations
                    .iter()
                    .map(|op| {
                        Ok(pegaflow_transfer::TransferDesc {
                            local_ptr: std::ptr::NonNull::new(op.local_addr as *mut u8)
                                .ok_or("null destination")?,
                            remote_ptr: std::ptr::NonNull::new(op.remote_addr as *mut u8)
                                .ok_or("null source")?,
                            len: op.logical_payload_bytes,
                        })
                    })
                    .collect::<Result<_, String>>()?;
                transport
                    .engine()
                    .prepare_batch_transfer(
                        pegaflow_transfer::TransferOp::Read,
                        &remote_addr,
                        &descs,
                        Some(0),
                    )
                    .map(Some)
                    .map_err(|error| format!("prepare complete RDMA bundle: {error}"))
            } else {
                Ok(None)
            }
        })();
        let rdma_batch = match preparation {
            Ok(batch) => batch,
            Err(error) => {
                for part in parts.into_values() {
                    let _ = part.result_tx.send(Err(error.clone()));
                }
                return Err(error);
            }
        };
        self.write_event(&serde_json::json!({
            "event": "bundle_observed", "phase": "measured", "request_id": stable_request_id,
            "raw_request_id": raw_request_id, "operations": operations,
        }))?;
        let eligible_at = Instant::now();
        self.write_event_at(&serde_json::json!({
            "event": "bundle_assembled", "request_id": stable_request_id,
            "part_count": part_count, "group_assembly_ns": groups_ready_at.duration_since(assembly_start).as_nanos(),
            "manifest_and_prepare_ns": eligible_at.duration_since(groups_ready_at).as_nanos(),
            "storage_groups": expected_parts.iter().map(|(namespace, group)| serde_json::json!({"namespace": namespace, "storage_group_id": group})).collect::<Vec<_>>(),
        }), eligible_at)?;
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

        let posted_event = bundle_timing_event(
            "bundle_posted",
            &stable_request_id,
            &raw_request_id,
            &plan,
            Some(match self.config.schedule {
                Issue23Schedule::Burst => self.offset_ms(plan.smooth_slot),
                Issue23Schedule::Smooth => self.offset_ms(plan.smooth_slot),
            }),
        );
        let bundle = PreparedIssue23Bundle {
            stable_request_id,
            raw_request_id,
            plan: plan.clone(),
            eligible_at,
            operations,
            rdma_batch,
            posted_event,
            result_txs: parts
                .into_values()
                .map(|part| (part.eligible_at, part.result_tx))
                .collect(),
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
            if cohort.len() == self.cohort_size() {
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
        rdma: Option<Arc<RdmaTransport>>,
        remote_addr: String,
        mut cohort: Vec<PreparedIssue23Bundle>,
    ) {
        cohort.sort_by_key(|bundle| bundle.plan.smooth_slot);
        let tg = cohort
            .iter()
            .map(|bundle| bundle.eligible_at)
            .max()
            .expect("complete cohort is non-empty");

        if self
            .smooth_scheduler
            .as_ref()
            .is_some_and(|scheduler| !scheduler.default_policy)
        {
            self.run_realtime_smooth_cohort(rdma, remote_addr, cohort, tg)
                .await;
            return;
        }

        let cohort_id = cohort[0].plan.cohort_id.clone();
        let posted_times = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = tokio::task::JoinSet::new();
        for bundle in cohort {
            let deadline = tg + Duration::from_millis(self.offset_ms(bundle.plan.smooth_slot));
            let experiment = Arc::clone(&self);
            let rdma = rdma.clone();
            let remote_addr = remote_addr.clone();
            let posted_times = Arc::clone(&posted_times);
            tasks.spawn(async move {
                let submitter = Arc::clone(&experiment);
                let result = tokio::task::spawn_blocking(move || {
                    wait_until_precise_blocking(deadline);
                    submitter.post_bundle(&rdma, &remote_addr, bundle)
                })
                .await;
                match result {
                    Ok(post) => {
                        if let Ok(pending) = &post {
                            posted_times.lock().push(pending.posted_at);
                        }
                        experiment.finish_post(post).await;
                    }
                    Err(error) => {
                        let _ = experiment.write_event(&serde_json::json!({
                            "event": "cohort_post_task_failed", "error": error.to_string(),
                        }));
                    }
                }
            });
        }
        while tasks.join_next().await.is_some() {}
        let times = posted_times.lock();
        if self.config.schedule == Issue23Schedule::Burst
            && let (Some(first), Some(last)) = (times.iter().min(), times.iter().max())
        {
            let spread = last.duration_since(*first);
            let _ = self.write_event(&serde_json::json!({
                "event": "cohort_posting_spread", "cohort_id": cohort_id, "posting_spread_ns": spread.as_nanos(),
                "limit_ns": MAX_BURST_POSTING_SPREAD.as_nanos(),
                "pass": spread <= MAX_BURST_POSTING_SPREAD,
            }));
        }
    }

    #[cfg(feature = "rdma")]
    async fn run_realtime_smooth_cohort(
        self: &Arc<Self>,
        rdma: Option<Arc<RdmaTransport>>,
        remote_addr: String,
        cohort: Vec<PreparedIssue23Bundle>,
        tg: Instant,
    ) {
        let scheduler = self
            .smooth_scheduler
            .as_ref()
            .expect("smooth scheduler exists");
        let experiment = Arc::clone(self);
        let runtime = tokio::runtime::Handle::current();
        let (sender, receiver) = oneshot::channel();
        let submitted = scheduler.submit(Box::new(move || {
            let _runtime_guard = runtime.enter();
            let posts = cohort
                .into_iter()
                .map(|bundle| {
                    wait_until_precise_blocking(
                        tg + Duration::from_millis(experiment.offset_ms(bundle.plan.smooth_slot)),
                    );
                    experiment.post_bundle(&rdma, &remote_addr, bundle)
                })
                .collect::<Vec<_>>();
            let _ = sender.send(posts);
        }));
        if let Err(error) = submitted {
            let _ = self.write_event(
                &serde_json::json!({"event": "cohort_post_task_failed", "error": error}),
            );
            return;
        }
        match receiver.await {
            Ok(posts) => {
                for post in posts {
                    self.finish_post(post).await;
                }
            }
            Err(error) => {
                let _ = self.write_event(&serde_json::json!({"event": "cohort_post_task_failed", "error": error.to_string()}));
            }
        }
    }

    #[cfg(feature = "rdma")]
    fn post_bundle(
        &self,
        rdma: &Option<Arc<RdmaTransport>>,
        _remote_addr: &str,
        mut bundle: PreparedIssue23Bundle,
    ) -> Result<PendingBundle, FailedBundle> {
        let post_start = Instant::now();
        let completion = match self.config.backend {
            Issue23Backend::Rdma => {
                let Some(rdma) = rdma.as_ref() else {
                    return Err(FailedBundle::new(bundle, "RDMA transport is unavailable"));
                };
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
                    let started = Instant::now();
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
                    (bytes, started, Instant::now())
                }))
            }
        };
        let posted_at = Instant::now();
        let submit = posted_at.duration_since(post_start);
        bundle.posted_event["submit_ns"] = serde_json::json!(submit.as_nanos());
        Ok(PendingBundle {
            stable_request_id: bundle.stable_request_id,
            raw_request_id: bundle.raw_request_id,
            cohort_id: bundle.plan.cohort_id,
            eligible_at: bundle.eligible_at,
            posted_at,
            submit,
            expected_bytes: bundle
                .plan
                .operations
                .iter()
                .map(|op| op.logical_payload_bytes)
                .sum(),
            completion,
            posted_event: bundle.posted_event,
            result_txs: bundle.result_txs,
        })
    }

    #[cfg(feature = "rdma")]
    async fn finish_post(&self, post: Result<PendingBundle, FailedBundle>) {
        let pending = match post {
            Ok(pending) => pending,
            Err(failed) => {
                for (_, sender) in failed.result_txs {
                    let _ = sender.send(Err(failed.error.clone()));
                }
                return;
            }
        };
        let posted_trace_result = self.write_event_at(&pending.posted_event, pending.posted_at);
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
            PendingCompletion::Local(handle) => match handle.await {
                Ok((bytes, started, finished)) => {
                    let _ = self.write_event_at(&serde_json::json!({
                        "event": "local_copy_work", "request_id": pending.stable_request_id,
                        "copy_started_ns": started.duration_since(self.trace_origin).as_nanos(),
                        "copy_finished_ns": finished.duration_since(self.trace_origin).as_nanos(),
                        "copy_ns": finished.duration_since(started).as_nanos(),
                        "completed_payload_bytes": bytes,
                    }), finished);
                    Ok(bytes)
                }
                Err(error) => Err(format!("local copy worker failed: {error}")),
            },
        };
        let result = result.and_then(|bytes| {
            if bytes == pending.expected_bytes {
                Ok(bytes)
            } else {
                Err(format!(
                    "bundle completion byte mismatch: observed={bytes} expected={}",
                    pending.expected_bytes
                ))
            }
        });
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
            .and_then(|bytes| posted_trace_result.map(|_| bytes))
            .map(|_| Issue23Completion {
                eligible_wait: pending.posted_at.duration_since(pending.eligible_at),
                submit: pending.submit,
                transport_wait: completed_at.duration_since(pending.posted_at),
            })
            .and_then(|completion| trace_result.map(|_| completion));
        for (part_at, sender) in pending.result_txs {
            let part_result = result.clone().map(|mut completion| {
                completion.eligible_wait = pending.posted_at.duration_since(part_at);
                completion
            });
            let _ = sender.send(part_result);
        }
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
        object.insert("pid".into(), serde_json::json!(std::process::id()));
        object.insert(
            "covered_tp_ranks".into(),
            serde_json::json!(
                self.config
                    .plan
                    .as_ref()
                    .and_then(|plan| plan.freeze_provenance.as_ref())
                    .and_then(|p| p.get("covered_tp_ranks"))
            ),
        );
        object.insert(
            "block_id".into(),
            serde_json::json!(self.config.plan.as_ref().map(|plan| plan.block_id.as_str())),
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
fn release_smooth_slots<T>(
    tg: Instant,
    bundles: Vec<T>,
    mut smooth_slot: impl FnMut(&T) -> usize,
    mut release: impl FnMut(T),
) {
    for bundle in bundles {
        let release_at = tg + Duration::from_millis(SMOOTH_OFFSETS_MS[smooth_slot(&bundle)]);
        wait_until_precise_blocking(release_at);
        release(bundle);
    }
}

#[cfg(feature = "rdma")]
struct FailedBundle {
    error: String,
    result_txs: Vec<(Instant, oneshot::Sender<Result<Issue23Completion, String>>)>,
}

#[cfg(feature = "rdma")]
impl FailedBundle {
    fn new(bundle: PreparedIssue23Bundle, error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            result_txs: bundle.result_txs,
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
    expected_bytes: usize,
    completion: PendingCompletion,
    posted_event: serde_json::Value,
    result_txs: Vec<(Instant, oneshot::Sender<Result<Issue23Completion, String>>)>,
}

#[cfg(feature = "rdma")]
enum PendingCompletion {
    Rdma(Vec<mea::oneshot::Receiver<pegaflow_transfer::Result<usize>>>),
    Local(tokio::task::JoinHandle<(usize, Instant, Instant)>),
}

pub(crate) fn expected_hidden_layout(
    plan: Option<&Issue23TransferPlan>,
) -> Result<HashMap<BlockKey, u64>, String> {
    let mut segments: HashMap<(BlockKey, usize, String), usize> = HashMap::new();
    for request in plan.into_iter().flat_map(|plan| &plan.requests) {
        for operation in &request.operations {
            let hash = decode_hex(&operation.block_hash_hex)?;
            let key = (
                BlockKey::new(operation.namespace.clone(), hash),
                operation.slot_index,
                operation.segment.clone(),
            );
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
    for ((key, _, _), bytes) in segments {
        let total = blocks.entry(key).or_insert(0u64);
        *total = total
            .checked_add(bytes as u64)
            .ok_or_else(|| "hidden block byte count overflow".to_string())?;
    }
    Ok(blocks)
}

pub(crate) fn stable_request_id(raw: &str) -> Result<String, String> {
    let checkpoint = crate::checkpoint::parse_checkpoint_request_id(raw)?;
    let raw = if let Some(identity) = &checkpoint {
        let (request, suffix) = identity
            .request_id
            .rsplit_once(":g")
            .ok_or("checkpoint request lacks its group suffix")?;
        let (group, position) = suffix
            .split_once(":checkpoint-")
            .ok_or("checkpoint request lacks its boundary suffix")?;
        if group.parse::<u32>().ok() != Some(identity.group_id)
            || position.parse::<usize>().is_err()
        {
            return Err("checkpoint request group or position is invalid".into());
        }
        request
    } else {
        raw
    };
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
    namespace: &str,
    group_id: u32,
    block_hash: &[u8],
    slot_index: usize,
    segment: &str,
) -> String {
    format!(
        "{stable_request_id}|{}:{namespace}|g{group_id}|{}|{slot_index}|{segment}",
        namespace.len(),
        encode_hex(block_hash)
    )
}

pub(crate) fn storage_group_id(raw: &str, namespace: &str) -> Result<u32, String> {
    match crate::checkpoint::parse_checkpoint_request_id(raw)? {
        Some(identity) if identity.namespace != namespace => {
            Err("checkpoint namespace differs from fetch namespace".into())
        }
        Some(identity) => Ok(identity.group_id),
        None => Ok(0),
    }
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

pub(crate) fn decode_hex(value: &str) -> Result<Vec<u8>, String> {
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
            operation_id: operation_id(request, "ns", 0, &decode_hex(hash).unwrap(), 0, "k"),
            namespace: "ns".into(),
            storage_group_id: 0,
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
            schema_version: 2,
            protocol_id: "p".into(),
            block_id: "b".into(),
            qps_per_peer: 2,
            cohort_size: 4,
            smooth_interval_ms: 10,
            rdma_rate_bps: 0,
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
    fn eight_slot_plan_accepts_complete_cohort_and_rejects_missing_slot() {
        let mut plan = valid_plan();
        plan.cohort_size = 8;
        plan.smooth_interval_ms = 1200;
        plan.rdma_rate_bps = 1_000_000_000;
        for slot in 4..8 {
            let mut request = plan.requests[slot - 4].clone();
            request.request_id = format!("extra-{slot}");
            request.smooth_slot = slot;
            for op in &mut request.operations {
                op.operation_id = operation_id(
                    &request.request_id,
                    &op.namespace,
                    op.storage_group_id,
                    &decode_hex(&op.block_hash_hex).unwrap(),
                    op.slot_index,
                    &op.segment,
                );
            }
            plan.requests.push(request);
        }
        assert!(plan.validate().is_ok());
        plan.requests.pop();
        assert!(plan.validate().is_err());
    }

    #[test]
    fn stable_request_identity_strips_only_frozen_vllm_affixes() {
        assert_eq!(
            stable_request_id("cmpl-issue23-measure-0007-0-a26e1651").unwrap(),
            "issue23-measure-0007"
        );
        assert!(stable_request_id("cmpl-issue23-measure-0007").is_err());
        let scoped = crate::checkpoint::checkpoint_request_id(
            "instance",
            "ns:with:separators",
            "cmpl-issue23-measure-0007-0-a26e1651:g3:checkpoint-1",
            3,
            &[0, 3],
        );
        assert_eq!(stable_request_id(&scoped).unwrap(), "issue23-measure-0007");
        assert_eq!(storage_group_id(&scoped, "ns:with:separators").unwrap(), 3);
        assert!(storage_group_id(&scoped, "other").is_err());
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
        plan.requests[1].operations[0].operation_id = operation_id("req-1", "ns", 0, &[0], 0, "k");
        let layout = expected_hidden_layout(Some(&plan)).unwrap();
        assert_eq!(layout.len(), 3);
        assert_eq!(layout.get(&BlockKey::new("ns".into(), vec![0])), Some(&64));
    }

    #[test]
    fn hidden_layout_keeps_namespaces_with_equal_hashes_independent() {
        let mut plan = valid_plan();
        plan.requests[1].operations[0].block_hash_hex = "00".into();
        plan.requests[1].operations[0].namespace = "other".into();
        plan.requests[1].operations[0].logical_payload_bytes = 128;
        let layout = expected_hidden_layout(Some(&plan)).unwrap();
        assert_eq!(layout.get(&BlockKey::new("ns".into(), vec![0])), Some(&64));
        assert_eq!(
            layout.get(&BlockKey::new("other".into(), vec![0])),
            Some(&128)
        );
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn complete_hybrid_gate_waits_for_all_groups_then_copies_the_full_bundle() {
        for schedule in [Issue23Schedule::Burst, Issue23Schedule::Smooth] {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(4)
                .build()
                .unwrap();
            runtime.block_on(async {
                let mut plan = valid_plan();
                for request in &mut plan.requests {
                    let hash = decode_hex(&request.operations[0].block_hash_hex).unwrap();
                    request.operations = (0..4)
                        .map(|group| {
                            let mut op = operation(
                                &request.request_id,
                                &encode_hex(&pegaflow_common::group_hash(&hash, group)),
                                group as usize,
                            );
                            op.storage_group_id = group;
                            op.operation_id = operation_id(
                                &request.request_id,
                                "ns",
                                group,
                                &decode_hex(&op.block_hash_hex).unwrap(),
                                0,
                                "k",
                            );
                            op
                        })
                        .collect();
                }
                plan.validate().unwrap();
                let temp = tempfile::tempdir().unwrap();
                let trace = temp.path().join("trace.jsonl");
                let mut experiment = Issue23CausalExperiment::new(Issue23ExperimentConfig {
                    plan: Some(plan.clone()),
                    backend: Issue23Backend::LocalCopy,
                    schedule,
                    trace_path: trace.clone(),
                })
                .unwrap();
                if schedule == Issue23Schedule::Smooth {
                    Arc::get_mut(&mut experiment).unwrap().smooth_scheduler =
                        Some(SmoothScheduler::with_default_policy(true).unwrap());
                }
                let buffers: Vec<_> = (0..16)
                    .map(|i| (vec![i as u8 + 1; 64], vec![0u8; 64]))
                    .collect();
                let mut tasks = Vec::new();
                for group in 0..4 {
                    for slot in 0..4 {
                        let request = &plan.requests[slot];
                        let op = &request.operations[group];
                        let pair = &buffers[group * 4 + slot];
                        let observed = ObservedOperation {
                            operation_id: op.operation_id.clone(),
                            namespace: op.namespace.clone(),
                            storage_group_id: op.storage_group_id,
                            block_hash_hex: op.block_hash_hex.clone(),
                            slot_index: op.slot_index,
                            segment: op.segment.clone(),
                            source: op.source.clone(),
                            destination: op.destination.clone(),
                            transfer_type: op.transfer_type.clone(),
                            logical_payload_bytes: op.logical_payload_bytes,
                            qp_index: 0,
                            remote_addr: pair.0.as_ptr() as u64,
                            local_addr: pair.1.as_ptr() as u64,
                        };
                        let raw = if group == 0 {
                            format!("cmpl-{}-0-deadbeef", request.request_id)
                        } else {
                            crate::checkpoint::checkpoint_request_id(
                                "instance",
                                "ns",
                                &format!(
                                    "cmpl-{}-0-deadbeef:g{group}:checkpoint-0",
                                    request.request_id
                                ),
                                group as u32,
                                &[1],
                            )
                        };
                        let (owned, rid, source) = (
                            Arc::clone(&experiment),
                            request.request_id.clone(),
                            op.source.clone(),
                        );
                        tasks.push(tokio::spawn(async move {
                            owned
                                .gate_bundle(None, source, raw, rid, vec![observed])
                                .await
                        }));
                    }
                    if group < 3 {
                        tokio::time::timeout(Duration::from_secs(2), async {
                            loop {
                                let ready = {
                                    let gate = experiment.gate.lock();
                                    gate.parts.len() == 4
                                        && gate.parts.values().all(|parts| parts.len() == group + 1)
                                };
                                if ready {
                                    break;
                                }
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .unwrap();
                        assert!(tasks.iter().all(|task| !task.is_finished()));
                        assert!(
                            buffers
                                .iter()
                                .all(|(_, destination)| destination.iter().all(|b| *b == 0))
                        );
                    }
                }
                for task in tasks {
                    tokio::time::timeout(Duration::from_secs(3), task)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                }
                assert!(
                    buffers
                        .iter()
                        .all(|(source, destination)| source == destination)
                );
                let events: Vec<serde_json::Value> = std::fs::read_to_string(trace)
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                assert_eq!(
                    events
                        .iter()
                        .filter(|event| event["event"] == "bundle_part_eligible")
                        .count(),
                    16
                );
                assert_eq!(
                    events
                        .iter()
                        .filter(|event| event["event"] == "bundle_posted")
                        .count(),
                    4
                );
                assert!(
                    events
                        .iter()
                        .filter(|event| event["event"] == "bundle_completed")
                        .all(|event| event["completed_payload_bytes"] == 256)
                );
                assert!(experiment.gate.lock().parts.is_empty());
            });
        }
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
                .frozen_remote_prefix_len("cmpl-req-0-0-deadbeef", "ns", &requested, 3)
                .unwrap(),
            2
        );
        assert!(
            experiment
                .frozen_remote_prefix_len("cmpl-req-0-0-deadbeef", "ns", &requested, 1)
                .unwrap_err()
                .contains("shorter than the frozen plan")
        );
        assert!(
            experiment
                .frozen_remote_prefix_len(
                    "cmpl-req-0-0-deadbeef",
                    "ns",
                    &[vec![0], vec![2], vec![1]],
                    3,
                )
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

    #[cfg(feature = "rdma")]
    #[test]
    fn default_scheduler_runs_without_realtime_privileges() {
        let scheduler = SmoothScheduler::with_default_policy(true).unwrap();
        scheduler.require_scheduler().unwrap();
        let (sender, receiver) = mpsc::sync_channel(1);
        scheduler
            .submit(Box::new(move || {
                sender.send(verify_default_scheduler()).unwrap();
            }))
            .unwrap();
        receiver.recv().unwrap().unwrap();
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn smooth_scheduler_releases_all_four_slots_on_its_prestarted_worker() {
        let scheduler = SmoothScheduler::new().unwrap();
        let realtime_available = scheduler.realtime_error.is_none();
        let default_policy = scheduler.default_policy;
        let caller_thread = std::thread::current().id();
        let release_at = Instant::now() + Duration::from_millis(5);
        let (sender, receiver) = mpsc::sync_channel(EXPECTED_COHORT_SIZE);
        scheduler
            .submit(Box::new(move || {
                let mut policy = 0;
                let mut parameter = libc::sched_param { sched_priority: 0 };
                // SAFETY: both output pointers are valid for the duration of
                // the call and pthread_self identifies the current worker.
                let result = unsafe {
                    libc::pthread_getschedparam(libc::pthread_self(), &mut policy, &mut parameter)
                };
                let worker_thread = std::thread::current().id();
                release_smooth_slots(
                    release_at,
                    vec![0usize, 1, 2, 3],
                    |slot| *slot,
                    |slot| {
                        sender
                            .send((
                                slot,
                                worker_thread,
                                result,
                                policy,
                                parameter.sched_priority,
                            ))
                            .unwrap();
                    },
                );
            }))
            .unwrap();
        for expected_slot in 0..EXPECTED_COHORT_SIZE {
            let (slot, worker_thread, result, policy, priority) = receiver.recv().unwrap();
            assert_eq!(slot, expected_slot);
            assert_ne!(worker_thread, caller_thread);
            assert_eq!(result, 0);
            if realtime_available {
                assert_eq!(
                    policy,
                    if default_policy {
                        libc::SCHED_OTHER
                    } else {
                        libc::SCHED_FIFO
                    }
                );
                assert_eq!(
                    priority,
                    if default_policy {
                        0
                    } else {
                        SMOOTH_SCHEDULER_PRIORITY
                    }
                );
            }
        }
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
                namespace: operation.namespace.clone(),
                storage_group_id: operation.storage_group_id,
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
