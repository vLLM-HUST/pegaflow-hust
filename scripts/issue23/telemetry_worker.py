"""Record actual model work without tensor copies or additional device synchronization."""

import time

from pegaflow.connector.telemetry import telemetry_event
from vllm_ascend.worker.worker import NPUWorker


class TelemetryNPUWorker(NPUWorker):
    def load_model(self):
        super().load_model()
        from vllm.v1.worker import mamba_utils

        original_preprocess = mamba_utils.preprocess_mamba
        context = {"requests": [], "step": 0}

        def preprocess(*args, **kwargs):
            schedule, _, _, _, batch, requests, _, _, _ = args
            rows = []
            for rid in batch.req_ids:
                request = requests[rid]
                rows.append(
                    {
                        "request_id": rid,
                        "computed_tokens": request.num_computed_tokens,
                        "scheduled_tokens": schedule.num_scheduled_tokens.get(rid, 0),
                        "prompt_tokens": request.num_prompt_tokens,
                    }
                )
            context["requests"] = rows
            telemetry_event("model_work", tp_rank=self.rank, step=context["step"], requests=rows)
            return original_preprocess(*args, **kwargs)

        mamba_utils.preprocess_mamba = preprocess
        original_execute = self.model_runner.execute_model

        def execute(*args, **kwargs):
            context["step"] += 1
            context["requests"] = []
            started = time.perf_counter_ns()
            try:
                result = original_execute(*args, **kwargs)
            except BaseException as error:
                telemetry_event(
                    "model_step_error",
                    tp_rank=self.rank,
                    step=context["step"],
                    error_type=type(error).__name__,
                    requests=context["requests"],
                )
                raise
            telemetry_event(
                "model_step_returned",
                tp_rank=self.rank,
                step=context["step"],
                cpu_span_ns=time.perf_counter_ns() - started,
                requests=context["requests"],
                clock_scope="host_execute_model_return",
            )
            return result

        self.model_runner.execute_model = execute
        telemetry_event(
            "model_telemetry_installed",
            tp_rank=self.rank,
            device_sync_added=False,
            tensor_copy_added=False,
        )
