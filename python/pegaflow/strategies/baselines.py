"""Paper-formula strategies, with serving-system adaptations named explicitly."""

from .model import Decision


class NativeStrategy:
    name = "vllm-pegaflow-native"

    def choose(self, snapshot, forecast=None):
        ps = sorted(worker.node for worker in snapshot.workers if worker.role == "P")
        ds = sorted(worker.node for worker in snapshot.workers if worker.role == "D")
        if not ps or not ds:
            raise ValueError("Both prefill and decode pools are required")
        index = snapshot.request.sequence
        return Decision(self.name, ps[index % len(ps)], ds[index % len(ds)], None, True,
                        "Round-robin proxy; unmodified vLLM/PegaFlow cache lookup; no custom planner")


class MemServeStrategy:
    name = "memserve-eq12-adapter"
    source = "https://arxiv.org/html/2406.17565#S5.SS3.SSS1"

    def choose(self, snapshot, forecast=None):
        options = snapshot.feasible()
        local = [plan for plan in options if plan.source == plan.p
                 and plan.p_prefix == snapshot.worker(plan.p).local_prefix
                 and not plan.layout_actions]
        if not local:
            raise ValueError("Each MemServe P comparison requires its local-only plan")
        chosen = min(local, key=lambda plan: (
            snapshot.worker(plan.p).queue_service_seconds + plan.p_first_seconds,
            plan.p, snapshot.worker(plan.d).queue_service_seconds, plan.d))
        saving = [plan for plan in options if plan.p == chosen.p and plan.d == chosen.d
                  and plan.p_prefix >= chosen.p_prefix and not plan.layout_actions
                  and plan.history_copy_seconds <= chosen.p_first_seconds - plan.p_first_seconds]
        if saving:
            chosen = min(saving, key=lambda plan: (-plan.p_prefix, plan.history_copy_seconds,
                                                  plan.source))
        return Decision(self.name, chosen.p, chosen.d, chosen, False,
                        "MemServe Eq1/Eq2 strategy adapter: measured exec/copy costs; "
                        "least-work D selector; not MemPool or full MemServe", len(local) + len(saving))


def lmetric_score(new_and_queued_prefill_tokens, post_admission_batch_size):
    """The paper's multiplication kernel; callers supply engine indicators.

    No serial-queue count is silently substituted for an actual batch size.
    A full PD adapter and the paper's hotspot detector are not yet qualified.
    """
    for value in (new_and_queued_prefill_tokens, post_admission_batch_size):
        if type(value) is not int or value < 0:
            raise ValueError("LMetric indicators must be nonnegative integers")
    return new_and_queued_prefill_tokens * post_admission_batch_size
