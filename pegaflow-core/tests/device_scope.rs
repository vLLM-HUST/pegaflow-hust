//! Process-scope regression test; no accelerator runtime is initialized.

use pegaflow_core::{configure_device_scope, validate_device_scope};

#[test]
fn process_device_scope_is_immutable_and_rejects_unlisted_devices() {
    assert!(configure_device_scope(&[]).is_err());
    assert!(configure_device_scope(&[-1]).is_err());
    configure_device_scope(&[2, 0]).unwrap();
    configure_device_scope(&[0, 2]).unwrap();
    assert!(configure_device_scope(&[0, 1, 2]).is_err());
    assert!(configure_device_scope(&[0, 0]).is_err());
    for device in [0, 2] {
        validate_device_scope(device).unwrap();
    }
    for device in [-1, 1, 3, 4, 5, 6, 7] {
        assert!(validate_device_scope(device).is_err());
    }
}
