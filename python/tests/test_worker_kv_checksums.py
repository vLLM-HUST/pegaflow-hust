"""Opt-in checksum coverage must include every requested registered layer."""
import hashlib
from unittest.mock import MagicMock

import pytest

from .unit_stubs import install_connector_unit_stubs

install_connector_unit_stubs()

from pegaflow.connector import worker as module  # noqa: E402


@pytest.mark.parametrize("all_layers,expected", [(False, 1), (True, 3)])
def test_load_checksum_layer_coverage(monkeypatch, all_layers, expected):
    worker = module.WorkerConnector.__new__(module.WorkerConnector)
    worker._diag_kv_checksum = True
    worker._diag_kv_all_layers = all_layers
    worker._registered_layers = ["layer.0", "layer.1", "layer.2"]
    worker._torch_device = None
    caches = {}
    for name in worker._registered_layers:
        cache = MagicMock()
        cache.__getitem__.return_value.detach.return_value.to.return_value.contiguous.return_value.view.return_value.numpy.return_value.tobytes.return_value = name.encode()
        caches[name] = cache
    worker._diag_kv_caches = caches
    monkeypatch.setattr(module, "_ensure_npu_device_set", lambda device: None)
    monkeypatch.setattr(module, "_device_synchronize", lambda device: None)
    monkeypatch.setattr(module.torch, "uint8", "uint8", raising=False)
    logger = MagicMock()
    monkeypatch.setattr(module, "logger", logger)
    worker._log_diag_kv_checksums("load", ["req"], [14, 13])
    assert logger.info.call_count == expected
    logger.exception.assert_not_called()
    for index, call in enumerate(logger.info.call_args_list):
        name = f"layer.{index}"
        assert call.args[3] == name
        assert call.args[4] == [(block, hashlib.sha256(name.encode()).hexdigest()) for block in (14, 13)]


def test_disabled_checksum_does_not_touch_tensors():
    worker = module.WorkerConnector.__new__(module.WorkerConnector)
    worker._diag_kv_checksum = False
    worker._log_diag_kv_checksums("load", ["req"], [14])
