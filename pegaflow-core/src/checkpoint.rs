/// Isolate background checkpoint fetches across tenants, groups and query drift.
pub(crate) fn checkpoint_request_id(
    instance_id: &str,
    namespace: &str,
    req_id: &str,
    group_id: u32,
    hash: &[u8],
) -> String {
    let hex: String = hash.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "hma:{}:{instance_id}:{}:{namespace}:{}:{req_id}:{group_id}:{hex}",
        instance_id.len(),
        namespace.len(),
        req_id.len()
    )
}

#[cfg(test)]
mod tests {
    use super::checkpoint_request_id;

    #[test]
    fn identity_is_stable_and_includes_every_fetch_dimension() {
        let key = checkpoint_request_id("instance", "ns", "request", 1, &[0, 255]);
        assert_eq!(
            key,
            checkpoint_request_id("instance", "ns", "request", 1, &[0, 255])
        );
        for different in [
            checkpoint_request_id("other", "ns", "request", 1, &[0, 255]),
            checkpoint_request_id("instance", "other", "request", 1, &[0, 255]),
            checkpoint_request_id("instance", "ns", "other", 1, &[0, 255]),
            checkpoint_request_id("instance", "ns", "request", 2, &[0, 255]),
            checkpoint_request_id("instance", "ns", "request", 1, &[0, 254]),
        ] {
            assert_ne!(key, different);
        }
    }

    #[test]
    fn length_prefixes_prevent_separator_collisions() {
        assert_ne!(
            checkpoint_request_id("a:b", "c", "d", 1, &[1]),
            checkpoint_request_id("a", "b:c", "d", 1, &[1])
        );
    }
}
