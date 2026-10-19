import unittest

from pegaflow.strategies.serving_contract import sampling_extra, validate_serving_capabilities


class ServingContractTests(unittest.TestCase):
    def test_native_leaves_default_lookup_untouched(self):
        self.assertEqual(sampling_extra({"policy_native_cache": True}, 128), {})
        with self.assertRaises(ValueError):
            sampling_extra({"policy_native_cache": True, "max_prefix_tokens": 0}, 128)

    def test_controlled_converts_tokens_to_actual_blocks(self):
        options = sampling_extra({"policy_native_cache": False, "max_prefix_tokens": 256}, 128)
        self.assertEqual(options["kv_transfer_params"]["pegaflow_joint"]["max_prefix_blocks"], 2)
        for value in (True, 3, -128):
            with self.assertRaises(ValueError):
                sampling_extra({"policy_native_cache": False, "max_prefix_tokens": value}, 128)

    def test_old_worker_capabilities_are_not_treated_as_native_baseline(self):
        with self.assertRaises(ValueError):
            validate_serving_capabilities({"controlled_local_boundary": True})


if __name__ == "__main__":
    unittest.main()
