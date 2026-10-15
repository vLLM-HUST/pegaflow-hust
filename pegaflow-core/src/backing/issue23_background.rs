use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use pegaflow_proto::proto::engine::engine_client::EngineClient;
use pegaflow_proto::proto::engine::{
    QueryBlocksForTransferRequest, RdmaHandshakeRequest, ReleaseTransferLockRequest,
};
use pegaflow_transfer::{ConnectionStatus, HandshakeMetadata, TransferDesc, TransferOp};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use super::RdmaTransport;
use crate::pinned_pool::PinnedAllocation;

#[derive(Clone, Deserialize)]
pub struct Issue23BackgroundConfig {
    pub remote_addr: String,
    pub namespace: String,
    pub block_hash_hex: String,
    pub rate_bps: u64,
}

#[derive(Default, Serialize)]
pub struct Issue23BackgroundStats {
    pub completed_bytes: u64,
    pub transfers: u64,
    pub elapsed_seconds: f64,
    pub completion_ms: Vec<f64>,
    pub error: Option<String>,
}

pub(crate) struct BackgroundReader {
    stop: Arc<AtomicBool>,
    task: Option<JoinHandle<Issue23BackgroundStats>>,
}

impl Drop for BackgroundReader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

impl BackgroundReader {
    pub(crate) async fn start(
        rdma: Arc<RdmaTransport>,
        allocation: Arc<PinnedAllocation>,
        config: Issue23BackgroundConfig,
        identity: String,
    ) -> Result<Self, String> {
        if config.rate_bps == 0 {
            return Err("Background rate must be positive".into());
        }
        let remote_key = format!("{}#issue23-background", config.remote_addr);
        let url = if config.remote_addr.starts_with("http") {
            config.remote_addr.clone()
        } else {
            format!("http://{}", config.remote_addr)
        };
        let mut client = EngineClient::connect(url)
            .await
            .map_err(|e| e.to_string())?;
        match rdma
            .engine()
            .get_or_prepare(&remote_key)
            .map_err(|e| e.to_string())?
        {
            ConnectionStatus::Existing => {}
            ConnectionStatus::Connecting => {
                return Err("Background handshake is still pending".into());
            }
            ConnectionStatus::Prepared(local) => {
                let result = client
                    .rdma_handshake(RdmaHandshakeRequest {
                        requester_id: format!("{identity}#issue23-background"),
                        handshake_metadata: local.to_bytes(),
                    })
                    .await;
                let response = match result {
                    Ok(response) => response.into_inner(),
                    Err(error) => {
                        rdma.engine().abort_handshake(&remote_key, &local);
                        return Err(error.to_string());
                    }
                };
                let completed = (|| {
                    if !response.status.as_ref().is_some_and(|status| status.ok) {
                        return Err("Background handshake was rejected".to_string());
                    }
                    let remote = HandshakeMetadata::from_bytes(&response.handshake_metadata)
                        .map_err(|e| e.to_string())?;
                    rdma.engine()
                        .complete_handshake(&remote_key, &local, &remote)
                        .map_err(|e| e.to_string())
                })();
                if let Err(error) = completed {
                    rdma.engine().abort_handshake(&remote_key, &local);
                    return Err(error);
                }
            }
        }
        let hash = crate::issue23_causal::decode_hex(&config.block_hash_hex)?;
        let response = client
            .query_blocks_for_transfer(QueryBlocksForTransferRequest {
                namespace: config.namespace,
                block_hashes: vec![hash],
                requester_id: identity,
                request_id: "issue23-background-read".into(),
            })
            .await
            .map_err(|e| e.to_string())?
            .into_inner();
        let session = response.transfer_session_id;
        if !response.status.as_ref().is_some_and(|status| status.ok) {
            if !session.is_empty() {
                client
                    .release_transfer_lock(ReleaseTransferLockRequest {
                        transfer_session_id: session,
                    })
                    .await
                    .map_err(|e| e.to_string())?;
            }
            return Err("Background source query was rejected".into());
        }
        let selected = response
            .blocks
            .iter()
            .flat_map(|block| &block.slots)
            .find(|slot| slot.k_ptr != 0 && slot.k_size > 0)
            .map(|slot| (slot.k_ptr, slot.k_size.min(1024 * 1024) as usize));
        let Some((remote_ptr, bytes)) = selected else {
            client
                .release_transfer_lock(ReleaseTransferLockRequest {
                    transfer_session_id: session,
                })
                .await
                .map_err(|e| e.to_string())?;
            return Err("Background source is absent from the pinned snapshot".into());
        };
        let stop = Arc::new(AtomicBool::new(false));
        let task_stop = Arc::clone(&stop);
        let task = tokio::spawn(async move {
            let started = Instant::now();
            let mut next = started;
            let interval = Duration::from_nanos(
                (bytes as u128 * 8 * 1_000_000_000)
                    .div_ceil(config.rate_bps as u128)
                    .min(u64::MAX as u128) as u64,
            );
            let mut stats = Issue23BackgroundStats::default();
            while !task_stop.load(Ordering::Acquire) {
                if Instant::now() < next {
                    tokio::time::sleep_until(next.into()).await;
                }
                if task_stop.load(Ordering::Acquire) {
                    break;
                }
                let posted = Instant::now();
                let receivers = {
                    let desc = TransferDesc {
                        local_ptr: allocation.as_non_null(),
                        remote_ptr: match std::ptr::NonNull::new(remote_ptr as *mut u8) {
                            Some(pointer) => pointer,
                            None => {
                                stats.error = Some("Background source address is zero".into());
                                break;
                            }
                        },
                        len: bytes,
                    };
                    rdma.engine()
                        .batch_transfer_async(TransferOp::Read, &remote_key, &[desc])
                };
                let receivers = match receivers {
                    Ok(receivers) => receivers,
                    Err(error) => {
                        stats.error = Some(error.to_string());
                        break;
                    }
                };
                let mut done = 0;
                for receiver in receivers {
                    match receiver.await {
                        Ok(Ok(value)) => done += value,
                        other => stats.error = Some(format!("Background CQ error: {other:?}")),
                    }
                }
                if stats.error.is_some() {
                    break;
                }
                if done != bytes {
                    stats.error = Some("Background byte mismatch".into());
                    break;
                }
                stats.completed_bytes += done as u64;
                stats.transfers += 1;
                stats
                    .completion_ms
                    .push(posted.elapsed().as_secs_f64() * 1000.0);
                next = (next + interval).max(Instant::now());
            }
            let released = client
                .release_transfer_lock(ReleaseTransferLockRequest {
                    transfer_session_id: session,
                })
                .await;
            match released {
                Ok(response) => {
                    if !response.into_inner().status.is_some_and(|status| status.ok) {
                        stats
                            .error
                            .get_or_insert("Background source release was rejected".into());
                    }
                }
                Err(error) => {
                    stats.error.get_or_insert(error.to_string());
                }
            }
            stats.elapsed_seconds = started.elapsed().as_secs_f64();
            stats
        });
        Ok(Self {
            stop,
            task: Some(task),
        })
    }

    pub(crate) async fn stop(mut self) -> Result<Issue23BackgroundStats, String> {
        self.stop.store(true, Ordering::Release);
        self.task
            .take()
            .ok_or("Background completion task is absent")?
            .await
            .map_err(|e| e.to_string())
    }
}
