// RDMA remote block fetch: MetaServer query -> gRPC QueryBlocksForTransfer -> RDMA READ.

use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use log::{debug, info, warn};
use mea::singleflight::Group;
use pegaflow_proto::proto::engine::engine_client::EngineClient;
use pegaflow_proto::proto::engine::{
    QueryBlocksForTransferRequest, QueryBlocksForTransferResponse, RdmaHandshakeRequest,
    ReleaseTransferLockRequest, TransferBlockInfo,
};
use pegaflow_transfer::{ConnectionStatus, HandshakeMetadata, TransferDesc, TransferOp};
use tonic::transport::{Channel, Endpoint};

use pegaflow_common::NumaNode;

use opentelemetry::KeyValue;

use super::{AllocateFn, PrefetchResult, RdmaTransport};
use crate::block::{BlockKey, RawBlock, SealedBlock, Segment, object_id};
use crate::internode::MetaServerClient;
use crate::issue23_causal::{
    Issue23Backend, Issue23CausalExperiment, ObservedOperation, encode_hex, operation_id,
    stable_request_id,
};
use crate::metrics::{core_metrics, record_object_lifecycle};

/// Minimum usable transfer timeout. If the server's lock timeout minus the
/// safety margin falls below this, we use this floor to avoid instant timeouts.
const MIN_TRANSFER_TIMEOUT: Duration = Duration::from_secs(10);

/// Safety margin subtracted from the server's lock timeout. The client must
/// finish the RDMA transfer before the server releases the lock.
const LOCK_TIMEOUT_MARGIN: Duration = Duration::from_secs(60);

fn record_remote_read_objects(
    request_id: &str,
    namespace: &str,
    blocks: &[TransferBlockInfo],
    outcome: &'static str,
) {
    record_object_lifecycle("remote_read", "remote_cpu_pool", outcome, blocks.len());
    for block in blocks {
        let key = BlockKey::new(namespace.to_string(), block.block_hash.clone());
        let bytes = block
            .slots
            .iter()
            .map(|slot| slot.k_size.saturating_add(slot.v_size))
            .sum::<u64>();
        debug!(
            "object_lifecycle: event=remote_read request_id={} object_id={} location=remote_cpu_pool outcome={} bytes={}",
            request_id,
            object_id(&key),
            outcome,
            bytes
        );
    }
}

/// RDMA remote block fetch backing store.
///
/// When all requested blocks are missing locally, queries MetaServer for their
/// location, picks the best remote node, and uses gRPC + RDMA READ to fetch them.
pub(crate) struct RdmaFetchStore {
    metaserver_client: Arc<MetaServerClient>,
    rdma_transport: Arc<RdmaTransport>,
    allocate_fn: AllocateFn,
    advertise_addr: String,
    /// Lazy gRPC channel cache keyed by remote address. Tonic channels multiplex
    /// requests over a single HTTP/2 connection; cloning is cheap.
    grpc_channels: Arc<DashMap<String, EngineClient<Channel>>>,
    /// Singleflight group to deduplicate concurrent RDMA handshakes to the
    /// same remote address. Without this, N concurrent fetches to the same
    /// peer would each create QPs and race on the server, causing all but
    /// the last handshake's QPs to be invalidated.
    connect_group: Arc<Group<String, ()>>,
    issue23_experiment: Option<Arc<Issue23CausalExperiment>>,
}

impl RdmaFetchStore {
    pub(crate) fn new(
        metaserver_client: Arc<MetaServerClient>,
        rdma_transport: Arc<RdmaTransport>,
        allocate_fn: AllocateFn,
        advertise_addr: String,
        issue23_experiment: Option<Arc<Issue23CausalExperiment>>,
    ) -> Self {
        info!("RDMA remote fetch enabled (advertise={})", advertise_addr);
        Self {
            metaserver_client,
            rdma_transport,
            allocate_fn,
            advertise_addr,
            grpc_channels: Arc::new(DashMap::new()),
            connect_group: Arc::new(Group::new()),
            issue23_experiment,
        }
    }

    /// Query MetaServer for the best remote node that holds a prefix of `hashes`.
    /// Returns `(node_addr, prefix_len)`, or `None` if no remote node has any.
    pub(crate) async fn query_prefix(
        &self,
        req_id: &str,
        namespace: &str,
        hashes: &[Vec<u8>],
    ) -> Option<(String, usize)> {
        if hashes.is_empty() {
            return None;
        }

        let nodes = match self.metaserver_client.query_prefix(namespace, hashes).await {
            Ok(n) => n,
            Err(e) => {
                warn!("MetaServer query failed for remote fetch: {e}");
                return None;
            }
        };

        let best = nodes
            .iter()
            .filter(|n| n.node != self.advertise_addr)
            .max_by_key(|n| n.prefix_len)?;

        let remote_prefix_len = best.prefix_len as usize;
        let prefix_len = if let Some(experiment) = &self.issue23_experiment {
            match experiment.frozen_remote_prefix_len(req_id, hashes, remote_prefix_len) {
                Ok(prefix_len) => prefix_len,
                Err(error) => {
                    warn!("Frozen remote prefix validation failed: {error}");
                    return None;
                }
            }
        } else {
            remote_prefix_len
        };
        if prefix_len == 0 {
            return None;
        }

        debug!(
            "Remote prefix query: namespace={namespace} best_node={} prefix={prefix_len}/{} remote_prefix={remote_prefix_len}",
            best.node,
            hashes.len()
        );

        Some((best.node.clone(), prefix_len))
    }

    /// Fetch `hashes` from `remote_addr`.
    pub(crate) async fn fetch_blocks(
        &self,
        remote_addr: &str,
        req_id: &str,
        namespace: &str,
        hashes: &[Vec<u8>],
    ) -> PrefetchResult {
        rdma_fetch_task(
            &self.rdma_transport,
            &self.allocate_fn,
            &self.grpc_channels,
            &self.connect_group,
            remote_addr,
            req_id,
            &self.advertise_addr,
            namespace,
            hashes,
            self.issue23_experiment.clone(),
        )
        .await
    }
}

/// Execute RDMA fetch against a single remote node.
///
/// 1. Ensure RDMA connection (singleflight per remote_addr)
/// 2. gRPC QueryBlocksForTransfer RPC (connection reuse, no handshake)
/// 3. RDMA READ all block segments + build SealedBlocks
/// 4. ReleaseTransferLock (fire-and-forget, non-blocking)
#[allow(
    clippy::too_many_arguments,
    reason = "RDMA task arguments are the per-fetch context passed from the scheduler"
)]
async fn rdma_fetch_task(
    rdma: &Arc<RdmaTransport>,
    allocate_fn: &AllocateFn,
    grpc_channels: &DashMap<String, EngineClient<Channel>>,
    connect_group: &Group<String, ()>,
    remote_addr: &str,
    req_id: &str,
    advertise_addr: &str,
    namespace: &str,
    block_hashes: &[Vec<u8>],
    issue23_experiment: Option<Arc<Issue23CausalExperiment>>,
) -> PrefetchResult {
    let t0 = Instant::now();

    // 1. Ensure RDMA connection (singleflight: at most one handshake per remote_addr)
    let connect_start = Instant::now();
    if let Err(e) = ensure_connected(
        connect_group,
        rdma,
        grpc_channels,
        remote_addr,
        advertise_addr,
    )
    .await
    {
        warn!("RDMA connect to {remote_addr} failed: {e}");
        core_metrics()
            .rdma_fetch_total
            .add(1, &[KeyValue::new("status", "error")]);
        return Vec::new();
    }
    let connect_elapsed = connect_start.elapsed();

    // 2. gRPC QueryBlocksForTransfer (connection already established)
    let query_start = Instant::now();
    let (client, response) = match query_remote_blocks(
        grpc_channels,
        remote_addr,
        namespace,
        block_hashes,
        advertise_addr,
        req_id,
    )
    .await
    {
        Ok(cr) => cr,
        Err(e) => {
            warn!("Remote query to {remote_addr} failed: {e}");
            core_metrics()
                .rdma_fetch_total
                .add(1, &[KeyValue::new("status", "error")]);
            return Vec::new();
        }
    };
    let query_elapsed = query_start.elapsed();

    let transfer_session_id = response.transfer_session_id.clone();

    // 3. RDMA READ all blocks + build SealedBlocks
    let transfer_timeout = transfer_timeout_from_server(response.lock_timeout_secs);
    let blocks = response.blocks;
    let total_bytes: u64 = blocks
        .iter()
        .flat_map(|b| &b.slots)
        .map(|s| s.k_size + s.v_size)
        .sum();
    record_remote_read_objects(req_id, namespace, &blocks, "started");
    let (result, transfer_timing) = match fetch_blocks_via_rdma(
        rdma,
        allocate_fn,
        namespace,
        remote_addr,
        advertise_addr,
        &blocks,
        transfer_timeout,
        req_id,
        issue23_experiment,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            record_remote_read_objects(req_id, namespace, &blocks, "error");
            warn!("RDMA transfer from {remote_addr} failed: {e}");
            rdma.engine().invalidate_connection(remote_addr);
            spawn_release_lock(client, transfer_session_id);
            core_metrics()
                .rdma_fetch_total
                .add(1, &[KeyValue::new("status", "error")]);
            return Vec::new();
        }
    };
    record_remote_read_objects(req_id, namespace, &blocks, "ok");

    // 4. Release transfer lock (fire-and-forget: spawns a detached task)
    spawn_release_lock(client, transfer_session_id);

    let elapsed = t0.elapsed();
    let mb = total_bytes as f64 / (1024.0 * 1024.0);
    let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
    let throughput_mib_s = if elapsed.as_secs_f64() > 0.0 {
        mb / elapsed.as_secs_f64()
    } else {
        0.0
    };
    info!(
        "RDMA fetch summary: req_id={req_id} remote={remote_addr} blocks={}/{} slots={} descs={} slabs={} bytes_mib={mb:.1} total_ms={elapsed_ms:.2} tp_mib_s={throughput_mib_s:.0}",
        result.len(),
        block_hashes.len(),
        transfer_timing.slot_count,
        transfer_timing.transfer_desc_count,
        transfer_timing.numa_slab_count,
    );
    info!(
        "RDMA fetch stages: req_id={req_id} remote={remote_addr} connect_ms={:.2} query_ms={:.2} build_transfer_tasks_ms={:.2} submit_transfer_ms={:.2} rdma_wait_ms={:.2} rebuild_ms={:.2}",
        connect_elapsed.as_secs_f64() * 1000.0,
        query_elapsed.as_secs_f64() * 1000.0,
        transfer_timing.build_transfer_tasks.as_secs_f64() * 1000.0,
        transfer_timing.submit_transfer.as_secs_f64() * 1000.0,
        transfer_timing.rdma_wait.as_secs_f64() * 1000.0,
        transfer_timing.rebuild.as_secs_f64() * 1000.0,
    );
    let m = core_metrics();
    let ok = &[KeyValue::new("status", "ok")];
    m.rdma_fetch_total.add(1, ok);
    m.rdma_fetch_duration_seconds
        .record(elapsed.as_secs_f64(), ok);
    m.rdma_fetch_bytes.add(total_bytes, ok);
    result
}

/// Ensure an RDMA connection to `remote_addr` exists, using singleflight to
/// deduplicate concurrent handshakes to the same peer.
///
/// On the first call for a given remote, one task performs the full handshake
/// (prepare QPs → gRPC metadata exchange → complete connection). Concurrent
/// callers wait for that handshake to finish and then reuse the connection.
/// If the handshake fails, the error is returned to the leader and waiting
/// callers retry independently (mea try_work semantics).
async fn ensure_connected(
    connect_group: &Group<String, ()>,
    rdma: &RdmaTransport,
    grpc_channels: &DashMap<String, EngineClient<Channel>>,
    remote_addr: &str,
    advertise_addr: &str,
) -> Result<(), String> {
    connect_group
        .try_work(remote_addr.to_string(), async || {
            // Fast path: already connected
            let local_meta = match rdma.engine().get_or_prepare(remote_addr) {
                Ok(ConnectionStatus::Existing) => return Ok(()),
                Ok(ConnectionStatus::Connecting) => {
                    return Err("handshake to this peer already in progress".into());
                }
                Ok(ConnectionStatus::Prepared(m)) => m,
                Err(e) => return Err(format!("RDMA prepare: {e}")),
            };

            // Exchange handshake metadata via the dedicated RdmaHandshake RPC.
            let mut client = get_or_create_channel(grpc_channels, remote_addr)
                .inspect_err(|_| rdma.engine().abort_handshake(remote_addr, &local_meta))?;

            let request = RdmaHandshakeRequest {
                requester_id: advertise_addr.to_string(),
                handshake_metadata: local_meta.to_bytes(),
            };
            let response = client
                .rdma_handshake(request)
                .await
                .map_err(|e| format!("RdmaHandshake RPC failed: {e}"))
                .inspect_err(|_| rdma.engine().abort_handshake(remote_addr, &local_meta))?
                .into_inner();

            // Complete the RDMA connection with the server's QP info.
            finish_handshake(rdma, remote_addr, &local_meta, &response.handshake_metadata)
                .inspect_err(|_| rdma.engine().abort_handshake(remote_addr, &local_meta))?;

            Ok(())
        })
        .await
}

/// One fetched slot: its RDMA-staged segments plus the NUMA node they sit on.
/// The NUMA travels with the slot so a re-served block advertises real topology.
type StagedSlot = (Vec<SegmentAlloc>, NumaNode);
/// A staged block awaiting SealedBlock rebuild: its hash and per-slot allocations.
type StagedBlock = (Vec<u8>, Vec<StagedSlot>);

/// Allocate local memory, build TransferDescs, execute RDMA READ, build SealedBlocks.
async fn fetch_blocks_via_rdma(
    rdma: &Arc<RdmaTransport>,
    allocate_fn: &AllocateFn,
    namespace: &str,
    remote_addr: &str,
    destination_addr: &str,
    blocks: &[TransferBlockInfo],
    transfer_timeout: Duration,
    req_id: &str,
    issue23_experiment: Option<Arc<Issue23CausalExperiment>>,
) -> Result<(PrefetchResult, TransferTiming), String> {
    if blocks.is_empty() {
        return Ok((Vec::new(), TransferTiming::default()));
    }

    // Experiment bundles use one allocation per logical block. During
    // snapshot restore this lets duplicate prefix blocks drop without a small
    // unique suffix pinning an entire request-sized slab. The normal
    // production path retains its faster per-NUMA batch slab.
    let mut block_slabs = if issue23_experiment.is_some() {
        Some(
            blocks
                .iter()
                .map(|block| {
                    allocate_numa_slabs(
                        allocate_fn,
                        sum_segment_bytes_by_numa(std::slice::from_ref(block))?,
                    )
                })
                .collect::<Result<Vec<_>, String>>()?,
        )
    } else {
        None
    };
    let mut numa_slabs = if block_slabs.is_none() {
        allocate_numa_slabs(allocate_fn, sum_segment_bytes_by_numa(blocks)?)?
    } else {
        HashMap::new()
    };
    let numa_slab_count = block_slabs.as_ref().map_or_else(
        || numa_slabs.len(),
        |slabs| slabs.iter().map(HashMap::len).sum(),
    );

    // (block_hash, Vec<(slot_segments, slot_numa)>) — for building SealedBlock afterwards.
    // The per-slot NUMA is preserved so a re-served fetched block advertises real topology.
    let mut block_allocs: Vec<StagedBlock> = Vec::new();
    let mut slot_count = 0usize;
    let build_start = Instant::now();

    // Build the exact logical operation manifest before the transport
    // boundary. NonNull pointers are converted to integer addresses before
    // any await so the frozen cohort can move between async tasks safely.
    let (mut operations, rdma_batch, mut timing) = {
        let mut all_descs: Vec<TransferDesc> = Vec::new();
        let stable_id = issue23_experiment
            .as_ref()
            .map(|_| stable_request_id(req_id))
            .transpose()?;
        let mut operations = Vec::new();

        for (block_index, block_info) in blocks.iter().enumerate() {
            slot_count += block_info.slots.len();
            let mut slot_allocs = Vec::with_capacity(block_info.slots.len());
            let slabs = if let Some(per_block) = block_slabs.as_mut() {
                &mut per_block[block_index]
            } else {
                &mut numa_slabs
            };

            for (slot_index, slot) in block_info.slots.iter().enumerate() {
                let mut segments = Vec::new();
                let numa = NumaNode(slot.numa_node);

                // K segment
                if slot.k_size > 0 {
                    let len = usize::try_from(slot.k_size)
                        .map_err(|_| format!("K size exceeds usize: {}", slot.k_size))?;
                    let (local_ptr, alloc) = alloc_segment_from_slab(slabs, numa, len, "K")?;
                    let remote_ptr = NonNull::new(slot.k_ptr as *mut u8)
                        .ok_or_else(|| "remote K ptr is null".to_string())?;
                    all_descs.push(TransferDesc {
                        local_ptr,
                        remote_ptr,
                        len,
                    });
                    if let Some(stable_id) = stable_id.as_deref() {
                        operations.push(ObservedOperation {
                            operation_id: operation_id(
                                stable_id,
                                &block_info.block_hash,
                                slot_index,
                                "k",
                            ),
                            block_hash_hex: encode_hex(&block_info.block_hash),
                            slot_index,
                            segment: "k".into(),
                            source: remote_addr.to_string(),
                            destination: destination_addr.to_string(),
                            transfer_type: "load".into(),
                            logical_payload_bytes: len,
                            qp_index: operations.len() % 2,
                            local_addr: local_ptr.as_ptr() as u64,
                            remote_addr: remote_ptr.as_ptr() as u64,
                        });
                    }
                    segments.push(SegmentAlloc {
                        ptr_addr: local_ptr.as_ptr() as u64,
                        alloc,
                        size: len,
                    });
                }

                // V segment (split KV)
                if slot.v_size > 0 && slot.v_ptr != 0 {
                    let len = usize::try_from(slot.v_size)
                        .map_err(|_| format!("V size exceeds usize: {}", slot.v_size))?;
                    let (local_ptr, alloc) = alloc_segment_from_slab(slabs, numa, len, "V")?;
                    let remote_ptr = NonNull::new(slot.v_ptr as *mut u8)
                        .ok_or_else(|| "remote V ptr is null".to_string())?;
                    all_descs.push(TransferDesc {
                        local_ptr,
                        remote_ptr,
                        len,
                    });
                    if let Some(stable_id) = stable_id.as_deref() {
                        operations.push(ObservedOperation {
                            operation_id: operation_id(
                                stable_id,
                                &block_info.block_hash,
                                slot_index,
                                "v",
                            ),
                            block_hash_hex: encode_hex(&block_info.block_hash),
                            slot_index,
                            segment: "v".into(),
                            source: remote_addr.to_string(),
                            destination: destination_addr.to_string(),
                            transfer_type: "load".into(),
                            logical_payload_bytes: len,
                            qp_index: operations.len() % 2,
                            local_addr: local_ptr.as_ptr() as u64,
                            remote_addr: remote_ptr.as_ptr() as u64,
                        });
                    }
                    segments.push(SegmentAlloc {
                        ptr_addr: local_ptr.as_ptr() as u64,
                        alloc,
                        size: len,
                    });
                }

                slot_allocs.push((segments, numa));
            }

            block_allocs.push((block_info.block_hash.clone(), slot_allocs));
        }

        if all_descs.is_empty() {
            let timing = TransferTiming {
                build_transfer_tasks: build_start.elapsed(),
                slot_count,
                numa_slab_count,
                ..TransferTiming::default()
            };
            return Ok((Vec::new(), timing));
        }

        let transfer_desc_count = all_descs.len();
        let rdma_batch = if issue23_experiment
            .as_ref()
            .is_some_and(|experiment| experiment.is_active())
        {
            Some(
                rdma.engine()
                    .prepare_batch_transfer(TransferOp::Read, remote_addr, &all_descs, Some(0))
                    .map_err(|e| format!("RDMA prepare_batch_transfer failed: {e}"))?,
            )
        } else {
            None
        };

        let timing = TransferTiming {
            build_transfer_tasks: build_start.elapsed(),
            transfer_desc_count,
            slot_count,
            numa_slab_count,
            ..TransferTiming::default()
        };
        (operations, rdma_batch, timing)
    };

    if let Some(experiment) = &issue23_experiment {
        let stable_id = experiment.capture_bundle(req_id, &operations)?;
        if experiment.is_active() && experiment.backend() == Issue23Backend::LocalCopy {
            for operation in &mut operations {
                operation.remote_addr = experiment.local_source(
                    &operation.block_hash_hex,
                    operation.slot_index,
                    &operation.segment,
                    operation.logical_payload_bytes,
                )?;
            }
        }
        if experiment.is_active() {
            // Do not cancel a prepared bundle after it crosses the cohort
            // gate: the coordinator owns raw staged addresses until the
            // common completion callback fires. The arm-level controller has
            // a fail-closed deadline and tears down the process on a missing
            // cohort, which is the only memory-safe cancellation boundary.
            let completion = experiment
                .gate_bundle(
                    Arc::clone(rdma),
                    remote_addr.to_string(),
                    req_id.to_string(),
                    stable_id,
                    operations,
                    rdma_batch,
                )
                .await?;
            timing.submit_transfer = completion.submit;
            timing.rdma_wait = completion.eligible_wait + completion.transport_wait;
        } else {
            let submit_start = Instant::now();
            let receivers = {
                let descs: Vec<TransferDesc> = operations
                    .iter()
                    .map(|operation| TransferDesc {
                        local_ptr: NonNull::new(operation.local_addr as *mut u8)
                            .expect("allocated local pointer must be non-null"),
                        remote_ptr: NonNull::new(operation.remote_addr as *mut u8)
                            .expect("queried remote pointer must be non-null"),
                        len: operation.logical_payload_bytes,
                    })
                    .collect();
                rdma.engine()
                    .batch_transfer_async(TransferOp::Read, remote_addr, &descs)
                    .map_err(|e| format!("RDMA batch_transfer_async failed: {e}"))?
            };
            timing.submit_transfer = submit_start.elapsed();
            let wait_start = Instant::now();
            wait_for_rdma(receivers, transfer_timeout).await?;
            timing.rdma_wait = wait_start.elapsed();
        }
    } else {
        // Production path: build descriptors from the staged layout and submit
        // exactly as before when no experiment configuration is present.
        let submit_start = Instant::now();
        let receivers = {
            let mut descs = Vec::new();
            for (block_info, (_, slot_allocs)) in blocks.iter().zip(&block_allocs) {
                for (slot_info, (segments, _)) in block_info.slots.iter().zip(slot_allocs) {
                    for (segment_index, segment) in segments.iter().enumerate() {
                        let remote_addr = if segment_index == 0 {
                            slot_info.k_ptr
                        } else {
                            slot_info.v_ptr
                        };
                        descs.push(TransferDesc {
                            local_ptr: NonNull::new(segment.ptr_addr as *mut u8)
                                .expect("allocated local pointer must be non-null"),
                            remote_ptr: NonNull::new(remote_addr as *mut u8)
                                .expect("queried remote pointer must be non-null"),
                            len: segment.size,
                        });
                    }
                }
            }
            rdma.engine()
                .batch_transfer_async(TransferOp::Read, remote_addr, &descs)
                .map_err(|e| format!("RDMA batch_transfer_async failed: {e}"))?
        };
        timing.submit_transfer = submit_start.elapsed();
        let wait_start = Instant::now();
        wait_for_rdma(receivers, transfer_timeout).await?;
        timing.rdma_wait = wait_start.elapsed();
    }

    // Build SealedBlocks from allocated memory
    let rebuild_start = Instant::now();
    let mut result: PrefetchResult = Vec::with_capacity(block_allocs.len());
    for (hash, slot_allocs) in block_allocs {
        let key = BlockKey::new(namespace.to_string(), hash);
        let slots: Vec<(RawBlock, NumaNode)> = slot_allocs
            .into_iter()
            .map(|(segs, numa)| {
                let segments: Vec<Segment> = segs
                    .into_iter()
                    .map(|sa| {
                        let ptr = NonNull::new(sa.ptr_addr as *mut u8)
                            .expect("slab segment pointer must be non-null");
                        Segment::new(ptr, sa.size, sa.alloc)
                    })
                    .collect();
                (RawBlock::new(segments), numa)
            })
            .collect();
        let sealed = Arc::new(SealedBlock::from_slots(slots));
        result.push((key, sealed));
    }
    timing.rebuild = rebuild_start.elapsed();

    Ok((result, timing))
}

async fn wait_for_rdma(
    receivers: Vec<mea::oneshot::Receiver<pegaflow_transfer::Result<usize>>>,
    transfer_timeout: Duration,
) -> Result<(), String> {
    tokio::time::timeout(transfer_timeout, async {
        let mut first_error = None;
        for receiver in receivers {
            match receiver.await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert_with(|| format!("RDMA transfer failed: {error}"));
                }
                Err(_) => {
                    first_error.get_or_insert_with(|| "RDMA transfer channel closed".to_string());
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    })
    .await
    .map_err(|_| "RDMA transfer timed out".to_string())?
}

fn sum_segment_bytes_by_numa(
    blocks: &[TransferBlockInfo],
) -> Result<HashMap<NumaNode, u64>, String> {
    let mut bytes_per_numa: HashMap<NumaNode, u64> = HashMap::new();
    for block_info in blocks {
        for slot in &block_info.slots {
            let numa = NumaNode(slot.numa_node);
            if slot.k_size > 0 {
                let total = bytes_per_numa.entry(numa).or_insert(0);
                *total = total.checked_add(slot.k_size).ok_or_else(|| {
                    format!("numa bytes overflow while summing K segments on {numa}")
                })?;
            }
            if slot.v_size > 0 && slot.v_ptr != 0 {
                let total = bytes_per_numa.entry(numa).or_insert(0);
                *total = total.checked_add(slot.v_size).ok_or_else(|| {
                    format!("numa bytes overflow while summing V segments on {numa}")
                })?;
            }
        }
    }
    Ok(bytes_per_numa)
}

fn allocate_numa_slabs(
    allocate_fn: &AllocateFn,
    bytes_per_numa: HashMap<NumaNode, u64>,
) -> Result<HashMap<NumaNode, NumaSlab>, String> {
    let mut numa_slabs: HashMap<NumaNode, NumaSlab> = HashMap::new();
    for (numa, total_bytes) in bytes_per_numa {
        if total_bytes == 0 {
            continue;
        }
        let allocation = allocate_fn(total_bytes, Some(numa))
            .ok_or_else(|| format!("failed to allocate slab ({total_bytes} bytes) for {numa}"))?;
        let capacity = usize::try_from(total_bytes)
            .map_err(|_| format!("slab size exceeds usize for {numa}: {total_bytes}"))?;
        numa_slabs.insert(
            numa,
            NumaSlab {
                allocation,
                next_offset: 0,
                capacity,
            },
        );
    }
    Ok(numa_slabs)
}

fn alloc_segment_from_slab(
    slabs: &mut HashMap<NumaNode, NumaSlab>,
    numa: NumaNode,
    len: usize,
    segment_kind: &str,
) -> Result<(NonNull<u8>, Arc<crate::pinned_pool::PinnedAllocation>), String> {
    let slab = slabs
        .get_mut(&numa)
        .ok_or_else(|| format!("missing slab for {numa} while allocating {segment_kind}"))?;
    slab.allocate(len, segment_kind)
}

struct NumaSlab {
    allocation: Arc<crate::pinned_pool::PinnedAllocation>,
    next_offset: usize,
    capacity: usize,
}

impl NumaSlab {
    fn allocate(
        &mut self,
        len: usize,
        segment_kind: &str,
    ) -> Result<(NonNull<u8>, Arc<crate::pinned_pool::PinnedAllocation>), String> {
        let end = self.next_offset.checked_add(len).ok_or_else(|| {
            format!(
                "slab offset overflow while allocating {segment_kind}: offset={} len={len} capacity={}",
                self.next_offset, self.capacity
            )
        })?;
        if end > self.capacity {
            return Err(format!(
                "slab exhausted while allocating {segment_kind}: offset={} len={len} capacity={}",
                self.next_offset, self.capacity
            ));
        }

        let ptr = unsafe { self.allocation.as_non_null().as_ptr().add(self.next_offset) };
        self.next_offset = end;
        let ptr = NonNull::new(ptr).ok_or_else(|| "slab pointer is null".to_string())?;
        Ok((ptr, Arc::clone(&self.allocation)))
    }
}

struct SegmentAlloc {
    ptr_addr: u64,
    alloc: Arc<crate::pinned_pool::PinnedAllocation>,
    size: usize,
}

#[derive(Default)]
struct TransferTiming {
    build_transfer_tasks: Duration,
    submit_transfer: Duration,
    rdma_wait: Duration,
    rebuild: Duration,
    transfer_desc_count: usize,
    slot_count: usize,
    numa_slab_count: usize,
}

fn get_or_create_channel(
    cache: &DashMap<String, EngineClient<Channel>>,
    addr: &str,
) -> Result<EngineClient<Channel>, String> {
    if let Some(client) = cache.get(addr) {
        return Ok(client.clone());
    }
    let url = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    };
    let channel = Endpoint::from_shared(url)
        .map_err(|e| format!("invalid remote address: {e}"))?
        .connect_timeout(Duration::from_secs(5))
        .connect_lazy();
    // Match the engine server's 64 MiB message cap: a QueryBlocksForTransfer
    // response carries per-slot transfer descriptors, so a large block batch
    // overflows tonic's default 4 MiB decode limit.
    const MAX_GRPC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
    let client = EngineClient::new(channel)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);
    cache.insert(addr.to_string(), client.clone());
    Ok(client)
}

/// Get/create gRPC channel and call QueryBlocksForTransfer.
async fn query_remote_blocks(
    grpc_channels: &DashMap<String, EngineClient<Channel>>,
    remote_addr: &str,
    namespace: &str,
    block_hashes: &[Vec<u8>],
    advertise_addr: &str,
    req_id: &str,
) -> Result<(EngineClient<Channel>, QueryBlocksForTransferResponse), String> {
    let mut client = get_or_create_channel(grpc_channels, remote_addr)?;

    let request = QueryBlocksForTransferRequest {
        namespace: namespace.to_string(),
        block_hashes: block_hashes.to_vec(),
        requester_id: advertise_addr.to_string(),
        request_id: req_id.to_string(),
    };

    let response = client
        .query_blocks_for_transfer(request)
        .await
        .map_err(|e| format!("QueryBlocksForTransfer RPC failed: {e}"))?
        .into_inner();

    if let Some(st) = &response.status
        && !st.ok
    {
        return Err(format!("remote returned error: {}", st.message));
    }

    Ok((client, response))
}

/// Decode remote handshake metadata and complete the RDMA connection.
fn finish_handshake(
    rdma: &RdmaTransport,
    remote_addr: &str,
    local_meta: &HandshakeMetadata,
    remote_bytes: &[u8],
) -> Result<(), String> {
    if remote_bytes.is_empty() {
        return Err("server returned empty handshake_metadata".into());
    }
    let remote_meta = HandshakeMetadata::from_bytes(remote_bytes)
        .map_err(|e| format!("invalid metadata: {e}"))?;
    rdma.engine()
        .complete_handshake(remote_addr, local_meta, &remote_meta)
        .map_err(|e| format!("{e}"))
}

/// Compute client-side transfer timeout from server's lock timeout.
/// Returns `max(server_timeout - 60s, 10s)` so the client always finishes
/// before the server force-releases the lock.
fn transfer_timeout_from_server(lock_timeout_secs: u32) -> Duration {
    let server = Duration::from_secs(lock_timeout_secs as u64);
    server
        .saturating_sub(LOCK_TIMEOUT_MARGIN)
        .max(MIN_TRANSFER_TIMEOUT)
}

/// Release a transfer lock in a detached task. Does not block the caller.
fn spawn_release_lock(mut client: EngineClient<Channel>, transfer_session_id: String) {
    if transfer_session_id.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let req = ReleaseTransferLockRequest {
            transfer_session_id: transfer_session_id.clone(),
        };
        if let Err(e) = client.release_transfer_lock(req).await {
            warn!("ReleaseTransferLock failed for session {transfer_session_id}: {e}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use pegaflow_proto::proto::engine::TransferSlotInfo;

    fn slot(k_size: u64, v_ptr: u64, v_size: u64, numa: u32) -> TransferSlotInfo {
        TransferSlotInfo {
            k_ptr: 0x1000,
            k_size,
            v_ptr,
            v_size,
            numa_node: numa,
        }
    }

    fn test_allocate_fn(calls: Arc<AtomicUsize>) -> AllocateFn {
        let allocator = Arc::new(crate::pinned_pool::PinnedAllocator::new_global(
            32 * 1024 * 1024,
            1,
            false,
            false,
            None,
        ));
        Arc::new(move |size, _numa| {
            calls.fetch_add(1, Ordering::Relaxed);
            allocator.allocate(NonZeroU64::new(size)?, NumaNode::UNKNOWN)
        })
    }

    #[test]
    fn sum_segment_bytes_by_numa_aggregates_k_and_v() {
        let blocks = vec![
            TransferBlockInfo {
                block_hash: vec![1],
                slots: vec![
                    slot(100, 0, 0, 0),        // contiguous
                    slot(200, 0x2000, 300, 0), // split KV
                ],
            },
            TransferBlockInfo {
                block_hash: vec![2],
                slots: vec![slot(400, 0x3000, 500, 1)],
            },
        ];

        let totals = sum_segment_bytes_by_numa(&blocks).expect("sum bytes");
        assert_eq!(totals.get(&NumaNode(0)), Some(&600)); // 100 + (200 + 300)
        assert_eq!(totals.get(&NumaNode(1)), Some(&900)); // 400 + 500
    }

    #[test]
    fn allocate_numa_slabs_calls_allocator_once_per_numa() {
        let calls = Arc::new(AtomicUsize::new(0));
        let allocate_fn = test_allocate_fn(Arc::clone(&calls));

        let bytes_per_numa = HashMap::from([(NumaNode(0), 1024_u64), (NumaNode(1), 2048_u64)]);
        let slabs = allocate_numa_slabs(&allocate_fn, bytes_per_numa).expect("allocate slabs");

        assert_eq!(slabs.len(), 2);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn slab_allocation_is_contiguous_and_bounded() {
        let calls = Arc::new(AtomicUsize::new(0));
        let allocate_fn = test_allocate_fn(Arc::clone(&calls));

        let mut slabs = allocate_numa_slabs(&allocate_fn, HashMap::from([(NumaNode(0), 1024_u64)]))
            .expect("allocate slabs");
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        let (p1, _a1) =
            alloc_segment_from_slab(&mut slabs, NumaNode(0), 256, "K").expect("alloc p1");
        let (p2, _a2) =
            alloc_segment_from_slab(&mut slabs, NumaNode(0), 128, "V").expect("alloc p2");
        assert_eq!(p2.as_ptr() as usize - p1.as_ptr() as usize, 256);

        let overflow = alloc_segment_from_slab(&mut slabs, NumaNode(0), 1024, "K");
        assert!(overflow.is_err());
    }
}
