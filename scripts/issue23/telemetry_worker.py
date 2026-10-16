"""Record actual model work without tensor copies or additional device synchronization."""

import time

from pegaflow.connector.telemetry import telemetry_event
from vllm_ascend.worker.worker import NPUWorker


class TelemetryNPUWorker(NPUWorker):
    def load_model(self):
        super().load_model()
        context = {"requests": [], "step": 0}
        original_execute = self.model_runner.execute_model

        def execute(*args, **kwargs):
            context["step"] += 1
            context["requests"] = []
            schedule = args[0] if args else kwargs.get("scheduler_output")
            scheduled = getattr(schedule, "num_scheduled_tokens", {})
            new = {r.req_id: r for r in getattr(schedule, "scheduled_new_reqs", [])}
            known = getattr(self.model_runner, "requests", {})
            for rid, tokens in scheduled.items():
                request = known.get(rid) or new.get(rid)
                prompt = getattr(request, "prompt_token_ids", None)
                context["requests"].append(
                    {
                        "request_id": rid,
                        "computed_tokens": getattr(
                            request, "num_computed_tokens", None
                        ),
                        "scheduled_tokens": tokens,
                        "prompt_tokens": getattr(request, "num_prompt_tokens", None)
                        or (len(prompt) if prompt is not None else None),
                    }
                )
            telemetry_event(
                "model_work",
                tp_rank=self.rank,
                step=context["step"],
                total_scheduled_tokens=sum(scheduled.values()),
                active_sequences=len(scheduled),
                requests=context["requests"],
            )
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
