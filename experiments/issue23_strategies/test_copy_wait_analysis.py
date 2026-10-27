from analyze_copy_waits import partition, resource_holds


def test_overlap_partition_does_not_count_two_busy_resources_twice():
    assert partition(1, 7, [(0, 3, "source"), (2, 5, "link"), (9, 10, "source")]) == {
        "source": 1,
        "link+source": 1,
        "link": 2,
        "unexplained_or_bookkeeping": 2,
    }


def test_batched_residence_is_not_exclusive_device_ownership():
    arm = {
        "resource_operation_ledger": [
            {
                "key": "a",
                "operation": "D_api",
                "event": "start",
                "seconds": 1,
                "resources": ["gate2"],
            },
            {
                "key": "a",
                "operation": "D_api",
                "event": "finish",
                "seconds": 9,
                "resources": ["gate2"],
            },
        ],
        "batch_iterations": [
            {
                "node": 2,
                "start": 3,
                "end": 4,
                "kind": "compute",
                "pieces": [("a", 1, "decode")],
            }
        ],
    }
    assert resource_holds(arm)["gate2"] == [(3, 4, ("a",), "compute")]
