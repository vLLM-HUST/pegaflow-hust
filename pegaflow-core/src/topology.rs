//! Lightweight global cache for NUMA↔device mapping.
//!
//! Populated during engine initialisation and read by Ascend pinned-memory pool
//! creation to determine which NPU device to activate before `aclrtMallocHost`.

use pegaflow_common::NumaNode;
use std::collections::HashMap;
use std::sync::OnceLock;

static DEVICE_NUMA_MAP: OnceLock<HashMap<i32, NumaNode>> = OnceLock::new();
static DEVICE_SCOPE: OnceLock<Vec<i32>> = OnceLock::new();

fn normalized_scope(devices: &[i32]) -> Result<Vec<i32>, String> {
    if devices.is_empty() || devices.iter().any(|&device| device < 0) {
        return Err("device scope must contain non-negative device IDs".into());
    }
    let mut scope = devices.to_vec();
    scope.sort_unstable();
    scope.dedup();
    if scope.len() != devices.len() {
        return Err("device scope must not contain duplicate IDs".into());
    }
    Ok(scope)
}

/// Restrict device contexts created by this process, without changing CPU pool placement.
///
/// Call before constructing an engine or allocating pinned pools. The scope is
/// immutable for the process lifetime; repeated identical configuration is allowed.
/// IDs use the runtime's numbering (physical IDs when visibility is unrestricted).
pub fn configure_device_scope(devices: &[i32]) -> Result<(), String> {
    let scope = normalized_scope(devices)?;
    match DEVICE_SCOPE.set(scope) {
        Ok(()) => Ok(()),
        Err(scope) if DEVICE_SCOPE.get() == Some(&scope) => Ok(()),
        Err(_) => Err("device scope is already configured differently".into()),
    }
}

/// Reject a device outside the configured process scope before runtime access.
pub fn validate_device_scope(device: i32) -> Result<(), String> {
    if device < 0
        || DEVICE_SCOPE
            .get()
            .is_some_and(|scope| !scope.contains(&device))
    {
        return Err(format!(
            "device {device} is outside the configured device scope"
        ));
    }
    Ok(())
}

fn select_device(
    map: Option<&HashMap<i32, NumaNode>>,
    scope: Option<&[i32]>,
    node: NumaNode,
) -> i32 {
    map.into_iter()
        .flat_map(|map| map.iter())
        .filter(|(dev, numa)| **numa == node && scope.is_none_or(|scope| scope.contains(dev)))
        .map(|(&dev, _)| dev)
        .min()
        .unwrap_or_else(|| scope.map_or(0, |scope| scope[0]))
}

/// Store the device→NUMA mapping discovered during topology detection.
pub(crate) fn init_device_numa_map(map: HashMap<i32, NumaNode>) {
    let _ = DEVICE_NUMA_MAP.set(map);
}

/// Resolve the best NPU device for a given NUMA node.
///
/// Prefer an allowed local device, otherwise the first allowed device. With no
/// configured scope, retain the legacy device-0 fallback. CPU pool placement is
/// deliberately independent of device scope, preserving per-NUMA capacities.
pub(crate) fn resolve_device_for_numa(node: NumaNode) -> i32 {
    select_device(
        DEVICE_NUMA_MAP.get(),
        DEVICE_SCOPE.get().map(Vec::as_slice),
        node,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_scope_rejects_empty_negative_and_duplicate_ids() {
        for devices in [&[][..], &[-1], &[0, 0]] {
            assert!(normalized_scope(devices).is_err());
        }
        assert_eq!(normalized_scope(&[2, 0]).unwrap(), vec![0, 2]);
    }

    #[test]
    fn pinned_pool_devices_stay_in_scope_without_dropping_numa_nodes() {
        let map = HashMap::from([
            (0, NumaNode(6)),
            (2, NumaNode(6)),
            (1, NumaNode(0)),
            (5, NumaNode(2)),
            (4, NumaNode(4)),
        ]);
        for node in [
            NumaNode(0),
            NumaNode(2),
            NumaNode(4),
            NumaNode(6),
            NumaNode::UNKNOWN,
        ] {
            assert!([0, 2].contains(&select_device(Some(&map), Some(&[0, 2]), node)));
            assert_eq!(select_device(Some(&map), Some(&[2]), node), 2);
        }
        assert_eq!(select_device(None, Some(&[2]), NumaNode::UNKNOWN), 2);
        assert_eq!(select_device(Some(&map), None, NumaNode(4)), 4);
        assert_eq!(select_device(None, None, NumaNode::UNKNOWN), 0);
        let local_map = HashMap::from([(0, NumaNode(6)), (2, NumaNode(4))]);
        assert_eq!(
            select_device(Some(&local_map), Some(&[0, 2]), NumaNode(4)),
            2
        );
    }
}
