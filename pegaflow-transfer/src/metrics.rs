use std::sync::LazyLock;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use opentelemetry::{KeyValue, global};

use crate::engine::TransferOp;

static READ: LazyLock<[KeyValue; 1]> = LazyLock::new(|| [KeyValue::new("op", "read")]);
static WRITE: LazyLock<[KeyValue; 1]> = LazyLock::new(|| [KeyValue::new("op", "write")]);
static READ_OK: LazyLock<[KeyValue; 2]> =
    LazyLock::new(|| [KeyValue::new("op", "read"), KeyValue::new("status", "ok")]);
static READ_ERROR: LazyLock<[KeyValue; 2]> = LazyLock::new(|| {
    [
        KeyValue::new("op", "read"),
        KeyValue::new("status", "error"),
    ]
});
static WRITE_OK: LazyLock<[KeyValue; 2]> =
    LazyLock::new(|| [KeyValue::new("op", "write"), KeyValue::new("status", "ok")]);
static WRITE_ERROR: LazyLock<[KeyValue; 2]> = LazyLock::new(|| {
    [
        KeyValue::new("op", "write"),
        KeyValue::new("status", "error"),
    ]
});

fn op_attributes(op: TransferOp) -> &'static [KeyValue] {
    match op {
        TransferOp::Read => &*READ,
        TransferOp::Write => &*WRITE,
    }
}

fn result_attributes(op: TransferOp, succeeded: bool) -> &'static [KeyValue] {
    match (op, succeeded) {
        (TransferOp::Read, true) => &*READ_OK,
        (TransferOp::Read, false) => &*READ_ERROR,
        (TransferOp::Write, true) => &*WRITE_OK,
        (TransferOp::Write, false) => &*WRITE_ERROR,
    }
}

fn latency_boundaries() -> Vec<f64> {
    vec![
        0.000_001, 0.000_005, 0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500,
        0.001, 0.002_5, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.0, 5.0, 10.0, 30.0,
        60.0,
    ]
}

struct TransferMetrics {
    batches_enqueued: Counter<u64>,
    batches_dequeued: Counter<u64>,
    batch_enqueue_failures: Counter<u64>,
    batches_completed: Counter<u64>,
    queued_batches: UpDownCounter<i64>,
    active_batches: UpDownCounter<i64>,
    queue_delay_seconds: Histogram<f64>,
    service_duration_seconds: Histogram<f64>,
    descriptors_posted: Counter<u64>,
    bytes_posted: Counter<u64>,
    descriptors_completed: Counter<u64>,
    bytes_completed: Counter<u64>,
    cq_completion_delay_seconds: Histogram<f64>,
}

fn transfer_metrics() -> &'static TransferMetrics {
    static METRICS: LazyLock<TransferMetrics> = LazyLock::new(|| {
        let meter = global::meter("pegaflow-transfer");
        TransferMetrics {
            batches_enqueued: meter
                .u64_counter("pegaflow_rdma_batches_enqueued")
                .with_description("RDMA transfer batches accepted by session worker queues")
                .build(),
            batches_dequeued: meter
                .u64_counter("pegaflow_rdma_batches_dequeued")
                .with_description("RDMA transfer batches dequeued by session workers")
                .build(),
            batch_enqueue_failures: meter
                .u64_counter("pegaflow_rdma_batch_enqueue_failures")
                .with_description("RDMA transfer batches rejected by disconnected session workers")
                .build(),
            batches_completed: meter
                .u64_counter("pegaflow_rdma_batches_completed")
                .with_description("RDMA transfer batches completed by status")
                .build(),
            queued_batches: meter
                .i64_up_down_counter("pegaflow_rdma_queued_batches")
                .with_description("Current RDMA transfer batches waiting in session worker queues")
                .build(),
            active_batches: meter
                .i64_up_down_counter("pegaflow_rdma_active_batches")
                .with_description("Current RDMA transfer batches executing in session workers")
                .build(),
            queue_delay_seconds: meter
                .f64_histogram("pegaflow_rdma_queue_delay")
                .with_unit("s")
                .with_description("Delay from RDMA batch enqueue to session worker dequeue")
                .with_boundaries(latency_boundaries())
                .build(),
            service_duration_seconds: meter
                .f64_histogram("pegaflow_rdma_service_duration")
                .with_unit("s")
                .with_description("RDMA batch service time from dequeue through CQ completion")
                .with_boundaries(latency_boundaries())
                .build(),
            descriptors_posted: meter
                .u64_counter("pegaflow_rdma_descriptors_posted")
                .with_description("RDMA work requests successfully posted")
                .build(),
            bytes_posted: meter
                .u64_counter("pegaflow_rdma_bytes_posted")
                .with_unit("bytes")
                .with_description("RDMA READ or WRITE bytes successfully posted")
                .build(),
            descriptors_completed: meter
                .u64_counter("pegaflow_rdma_descriptors_completed")
                .with_description("Successful RDMA work completions consumed from CQs")
                .build(),
            bytes_completed: meter
                .u64_counter("pegaflow_rdma_bytes_completed")
                .with_unit("bytes")
                .with_description("Successful RDMA READ or WRITE bytes completed")
                .build(),
            cq_completion_delay_seconds: meter
                .f64_histogram("pegaflow_rdma_cq_completion_delay")
                .with_unit("s")
                .with_description("Delay from work-request post through successful CQ completion")
                .with_boundaries(latency_boundaries())
                .build(),
        }
    });
    &METRICS
}

pub(crate) fn record_enqueue(op: TransferOp) {
    let metrics = transfer_metrics();
    let attributes = op_attributes(op);
    metrics.batches_enqueued.add(1, attributes);
    metrics.queued_batches.add(1, attributes);
}

pub(crate) fn record_enqueue_failure(op: TransferOp) {
    let metrics = transfer_metrics();
    let attributes = op_attributes(op);
    metrics.queued_batches.add(-1, attributes);
    metrics.batch_enqueue_failures.add(1, attributes);
}

pub(crate) fn record_dequeue(op: TransferOp, queue_delay: Duration) {
    let metrics = transfer_metrics();
    let attributes = op_attributes(op);
    metrics.queued_batches.add(-1, attributes);
    metrics.active_batches.add(1, attributes);
    metrics.batches_dequeued.add(1, attributes);
    metrics
        .queue_delay_seconds
        .record(queue_delay.as_secs_f64(), attributes);
}

pub(crate) fn record_batch_complete(op: TransferOp, service_duration: Duration, succeeded: bool) {
    let metrics = transfer_metrics();
    metrics.active_batches.add(-1, op_attributes(op));
    let attributes = result_attributes(op, succeeded);
    metrics.batches_completed.add(1, attributes);
    metrics
        .service_duration_seconds
        .record(service_duration.as_secs_f64(), attributes);
}

pub(crate) fn record_post(op: TransferOp, descriptors: usize, bytes: usize) {
    let metrics = transfer_metrics();
    let attributes = op_attributes(op);
    metrics
        .descriptors_posted
        .add(descriptors as u64, attributes);
    metrics.bytes_posted.add(bytes as u64, attributes);
}

pub(crate) fn record_completion(op: TransferOp, bytes: usize, completion_delay: Duration) {
    let metrics = transfer_metrics();
    let attributes = op_attributes(op);
    metrics.descriptors_completed.add(1, attributes);
    metrics.bytes_completed.add(bytes as u64, attributes);
    metrics
        .cq_completion_delay_seconds
        .record(completion_delay.as_secs_f64(), attributes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_labels_keep_operation_and_status_distinct() {
        let cases = [
            (TransferOp::Read, true, "read", "ok"),
            (TransferOp::Read, false, "read", "error"),
            (TransferOp::Write, true, "write", "ok"),
            (TransferOp::Write, false, "write", "error"),
        ];
        for (op, succeeded, expected_op, expected_status) in cases {
            let attributes = result_attributes(op, succeeded);
            assert_eq!(attributes[0], KeyValue::new("op", expected_op));
            assert_eq!(attributes[1], KeyValue::new("status", expected_status));
        }
    }

    #[test]
    fn latency_buckets_cover_microseconds_through_minute_scale() {
        let boundaries = latency_boundaries();
        assert_eq!(boundaries.first().copied(), Some(0.000_001));
        assert_eq!(boundaries.last().copied(), Some(60.0));
        assert!(boundaries.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn instrument_names_do_not_preapply_prometheus_unit_or_counter_suffixes() {
        let source = include_str!("metrics.rs");
        for forbidden in [
            "pegaflow_rdma_batches_enqueued_total",
            "pegaflow_rdma_batches_dequeued_total",
            "pegaflow_rdma_batch_enqueue_failures_total",
            "pegaflow_rdma_batches_completed_total",
            "pegaflow_rdma_descriptors_posted_total",
            "pegaflow_rdma_bytes_posted_total",
            "pegaflow_rdma_descriptors_completed_total",
            "pegaflow_rdma_bytes_completed_total",
        ] {
            assert!(!source.contains(&format!(".u64_counter(\"{forbidden}\")")));
        }
        for forbidden in [
            "pegaflow_rdma_queue_delay_seconds",
            "pegaflow_rdma_service_duration_seconds",
            "pegaflow_rdma_cq_completion_delay_seconds",
        ] {
            assert!(!source.contains(&format!(".f64_histogram(\"{forbidden}\")")));
        }
    }
}
