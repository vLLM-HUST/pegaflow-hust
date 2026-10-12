from unittest.mock import MagicMock

from .unit_stubs import install_connector_unit_stubs

install_connector_unit_stubs()

from pegaflow.connector.tp_shards import TpShardQueryClient  # noqa: E402
from pegaflow.pegaflow import QueryLoading, QueryReady  # noqa: E402


def test_completed_shard_is_retained_while_sibling_loads():
    first, second = MagicMock(), MagicMock()
    first.query_prefetch.return_value = QueryReady(2, b"first")
    second.query_prefetch.side_effect = [QueryLoading(), QueryReady(2, b"second")]
    client = TpShardQueryClient((first, second))
    assert client.query("instance", [b"a", b"b"], "request", True) is None
    first.release.assert_not_called()
    result = client.query("instance", [b"a", b"b"], "request", True)
    assert result.leases == (b"first", b"second")
    assert first.query_prefetch.call_count == 1
    assert second.query_prefetch.call_count == 2
    assert not client._pending


def test_cancel_releases_completed_shard_lease():
    first, second = MagicMock(), MagicMock()
    first.query_prefetch.return_value = QueryReady(1, b"first")
    second.query_prefetch.return_value = QueryLoading()
    client = TpShardQueryClient((first, second))
    assert client.query("instance", [b"a"], "request", True) is None
    assert client.cancel_pending("instance", "request")
    first.release.assert_called_once_with(b"first")
    second.release.assert_not_called()
    assert not client._pending
