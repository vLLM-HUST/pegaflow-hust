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

#[derive(Debug)]
pub(crate) struct CheckpointRequest<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) request_id: &'a str,
    pub(crate) group_id: u32,
}

pub(crate) fn parse_checkpoint_request_id(
    raw: &str,
) -> Result<Option<CheckpointRequest<'_>>, String> {
    let Some(mut remaining) = raw.strip_prefix("hma:") else {
        return Ok(None);
    };
    fn field<'a>(remaining: &mut &'a str) -> Result<&'a str, String> {
        let (length, tail) = remaining
            .split_once(':')
            .ok_or("missing HMA field length")?;
        let length: usize = length.parse().map_err(|_| "invalid HMA field length")?;
        let value = tail.get(..length).ok_or("invalid HMA field boundary")?;
        *remaining = tail
            .get(length..)
            .and_then(|s| s.strip_prefix(':'))
            .ok_or("missing HMA field separator")?;
        Ok(value)
    }
    let _instance = field(&mut remaining)?;
    let namespace = field(&mut remaining)?;
    let request_id = field(&mut remaining)?;
    let (group, hash) = remaining
        .split_once(':')
        .ok_or("missing HMA group or hash")?;
    let group_id = group.parse().map_err(|_| "invalid HMA group")?;
    if group_id == 0
        || hash.is_empty()
        || hash.len() % 2 != 0
        || !hash.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err("invalid HMA checkpoint identity".into());
    }
    Ok(Some(CheckpointRequest {
        namespace,
        request_id,
        group_id,
    }))
}

#[cfg(test)]
mod tests {
    use super::{checkpoint_request_id, parse_checkpoint_request_id};

    #[test]
    fn parse_preserves_delimiters_and_rejects_truncated_identity() {
        let raw = checkpoint_request_id(
            "instance:1",
            "模型:ns",
            "cmpl-r-0-deadbeef:g2:checkpoint-1",
            2,
            &[3],
        );
        let parsed = parse_checkpoint_request_id(&raw).unwrap().unwrap();
        assert_eq!(parsed.namespace, "模型:ns");
        assert_eq!(parsed.request_id, "cmpl-r-0-deadbeef:g2:checkpoint-1");
        assert_eq!(parsed.group_id, 2);
        assert!(parse_checkpoint_request_id("hma:100:short").is_err());
        assert!(
            parse_checkpoint_request_id("cmpl-r-0-deadbeef")
                .unwrap()
                .is_none()
        );
    }

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
