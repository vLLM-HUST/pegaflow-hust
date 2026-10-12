from unittest.mock import MagicMock

import pytest

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


def test_changed_query_releases_cached_shard_before_requery():
    first, second = MagicMock(), MagicMock()
    first.query_prefetch.side_effect = [QueryReady(1, b"old"), QueryReady(1, b"new-first")]
    second.query_prefetch.side_effect = [QueryLoading(), QueryReady(1, b"new-second")]
    client = TpShardQueryClient((first, second))

    assert client.query("instance", [b"old-hash"], "request", True) is None
    result = client.query("instance", [b"new-hash"], "request", True)

    first.release.assert_called_once_with(b"old")
    assert result.leases == (b"new-first", b"new-second")
    assert first.query_prefetch.call_args.args[1] == [b"new-hash"]
    assert not client._pending


@pytest.mark.parametrize(
    ("outcome", "error", "released_second"),
    [
        pytest.param(RuntimeError("RPC failed"), RuntimeError, None, id="rpc-error"),
        pytest.param(object(), TypeError, None, id="invalid-outcome"),
        pytest.param(QueryReady(2, b"invalid"), RuntimeError, b"invalid", id="too-many-hits"),
        pytest.param(QueryReady(1, b""), RuntimeError, None, id="missing-lease"),
    ],
)
def test_failed_poll_releases_every_acquired_lease(outcome, error, released_second):
    first, second = MagicMock(), MagicMock()
    first.query_prefetch.return_value = QueryReady(1, b"first")
    second.query_prefetch.side_effect = [QueryLoading(), outcome]
    client = TpShardQueryClient((first, second))
    assert client.query("instance", [b"a"], "request", True) is None

    with pytest.raises(error):
        client.query("instance", [b"a"], "request", True)

    first.release.assert_called_once_with(b"first")
    if released_second:
        second.release.assert_called_once_with(released_second)
    else:
        second.release.assert_not_called()
    assert not client._pending


def test_ready_miss_is_retained_until_sibling_finishes():
    first, second = MagicMock(), MagicMock()
    first.query_prefetch.return_value = QueryReady(0, b"")
    second.query_prefetch.side_effect = [QueryLoading(), QueryReady(1, b"second")]
    client = TpShardQueryClient((first, second))
    assert client.query("instance", [b"a"], "request", True) is None

    result = client.query("instance", [b"a"], "request", True)

    assert result.num_hit_blocks == 0
    assert result.leases == (b"", b"")
    assert first.query_prefetch.call_count == 1
    second.release.assert_called_once_with(b"second")
    assert not client._pending


def test_retained_shards_are_released_to_the_common_prefix():
    first, second, third = MagicMock(), MagicMock(), MagicMock()
    first.query_prefetch.side_effect = [QueryReady(3, b"first-wide"), QueryReady(1, b"first")]
    second.query_prefetch.return_value = QueryReady(1, b"second")
    third.query_prefetch.side_effect = [
        QueryLoading(),
        QueryReady(2, b"third-wide"),
        QueryReady(1, b"third"),
    ]
    client = TpShardQueryClient((first, second, third))
    assert client.query("instance", [b"a", b"b", b"c"], "request", True) is None

    result = client.query("instance", [b"a", b"b", b"c"], "request", True)

    assert result.num_hit_blocks == 1
    assert result.leases == (b"first", b"second", b"third")
    first.release.assert_called_once_with(b"first-wide")
    third.release.assert_called_once_with(b"third-wide")
    second.release.assert_not_called()
    assert second.query_prefetch.call_count == 1
    assert first.query_prefetch.call_args.args[1] == [b"a"]
    assert not client._pending


def test_cancel_continues_after_a_shard_release_failure():
    first, second, third = MagicMock(), MagicMock(), MagicMock()
    first.query_prefetch.return_value = QueryReady(1, b"first")
    second.query_prefetch.return_value = QueryReady(1, b"second")
    third.query_prefetch.return_value = QueryLoading()
    first.release.side_effect = RuntimeError("release failed")
    client = TpShardQueryClient((first, second, third))
    assert client.query("instance", [b"a"], "request", True) is None

    assert not client.cancel_pending("instance", "request")
    second.release.assert_called_once_with(b"second")
    assert client.cancel_pending("instance", "request")
    assert not client._pending


@pytest.mark.parametrize(
    "other_key", [("instance", "other-request"), ("other-instance", "request")]
)
def test_pending_queries_are_isolated_by_instance_and_request(other_key):
    first, second = MagicMock(), MagicMock()
    first.query_prefetch.side_effect = [QueryReady(1, b"first"), QueryReady(1, b"other")]
    second.query_prefetch.return_value = QueryLoading()
    client = TpShardQueryClient((first, second))
    assert client.query("instance", [b"a"], "request", True) is None
    assert client.query(other_key[0], [b"a"], other_key[1], True) is None

    assert client.cancel_pending("instance", "request")
    first.release.assert_called_once_with(b"first")
    assert other_key in client._pending
    assert client.cancel_pending(*other_key)
    assert first.release.call_args.args == (b"other",)
    assert not client._pending
