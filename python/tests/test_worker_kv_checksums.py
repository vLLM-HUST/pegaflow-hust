"""Opt-in checksum coverage must include every requested registered layer."""

import hashlib
from unittest.mock import MagicMock

import numpy as np
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
    worker._cache_registrations = {}
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
        assert call.args[4] == [
            (block, hashlib.sha256(name.encode()).hexdigest()) for block in (14, 13)
        ]


def test_disabled_checksum_does_not_touch_tensors():
    worker = module.WorkerConnector.__new__(module.WorkerConnector)
    worker._diag_kv_checksum = False
    worker._log_diag_kv_checksums("load", ["req"], [14])


@pytest.mark.parametrize(
    "layout,ratio", [("blocks-first", 4), ("KV-first", 4), ("blocks-first", 1)]
)
def test_checksum_covers_complete_logical_block(monkeypatch, layout, ratio):
    data = (
        np.arange(96, dtype=np.uint16).reshape(2, 12, 4)
        if layout == "KV-first"
        else np.arange(48, dtype=np.uint16).reshape(12, 4)
    )

    class HostTensor:
        def __init__(self, array):
            self.array = array

        def __getitem__(self, key):
            return HostTensor(self.array[key])

        def detach(self):
            return self

        def to(self, _device):
            return self

        def contiguous(self):
            return self

        def view(self, _dtype):
            return self

        def numpy(self):
            return self.array

    worker = module.WorkerConnector.__new__(module.WorkerConnector)
    worker._diag_kv_checksum = True
    worker._torch_device = None
    worker._diag_kv_caches = {"layer": HostTensor(data)}
    worker._cache_registrations = {
        "layer": module._KVCacheRegistrationInfo(
            layout=layout,
            num_blocks=12 // ratio,
            bytes_per_block=4 * 2 * ratio,
            kv_stride_bytes=0,
            segments=2 if layout == "KV-first" else 1,
            physical_blocks_per_logical_block=ratio,
        )
    }
    monkeypatch.setattr(module.torch, "uint8", "uint8", raising=False)
    logger = MagicMock()
    monkeypatch.setattr(module, "logger", logger)
    worker._log_diag_kv_checksums("save", ["req"], [1], layer_name="layer")
    expected = data[:, ratio : 2 * ratio] if layout == "KV-first" else data[ratio : 2 * ratio]
    assert logger.info.call_args.args[4] == [(1, hashlib.sha256(expected.tobytes()).hexdigest())]
    logger.exception.assert_not_called()
