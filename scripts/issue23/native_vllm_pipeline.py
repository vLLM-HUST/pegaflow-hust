"""Native vLLM scheduling over the simulator's P/D publication and transfer events."""

from types import SimpleNamespace

from native_vllm_adapter import NativePoolView, NativeVllmEngine
from pd_pipeline_simulator import PipelineSimulation
from step_simulator import Job, features


class NativePipelineSimulation(PipelineSimulation):
    supports_native_scheduler = True

    def __init__(self, workload, profile, config=None):
        super().__init__(workload, profile, config)
        if self.config.get("engine_policy", "fcfs") != "fcfs":
            raise ValueError(
                "Native engine policy is configured with native_scheduler_options.policy"
            )
        if not self.overlap:
            raise ValueError("Native scheduler backend requires copy_compute_overlap=True")
        model_path = self.config.get("native_model_path")
        if not model_path:
            raise ValueError("native_model_path must identify the local model configuration")
        self.reserve_pages = False
        self.native_queries = {}
        self.native_engines = []
        limit = profile["models"]["D"].get("metadata", {}).get("max_model_tokens", 32768)
        for node in range(self.node_count):
            role = "P" if node < self.p_count else "D"
            callbacks = SimpleNamespace(
                query=lambda request, computed, n=node: self.native_query(n, request, computed),
                allocated=lambda request, blocks, external, n=node: self.native_allocated(
                    n, request, blocks, external
                ),
                finished=lambda request, blocks, n=node: n < self.p_count,
            )
            self.native_engines.append(
                NativeVllmEngine(
                    model_path=model_path,
                    num_blocks=len(self.pools[node].blocks),
                    block_tokens=self.block_tokens,
                    max_sequences=self.config.get(
                        role + "_max_sequences", self.config.get("max_sequences", 128)
                    ),
                    token_budget=self.p_tokens if role == "P" else self.d_tokens,
                    max_model_len=limit,
                    enable_prefix_caching=self.config.get("GPU_APC", True),
                    scheduler_options=self.config.get("native_scheduler_options"),
                    transfer=callbacks,
                )
            )
        self.pools = [NativePoolView(engine) for engine in self.native_engines]

    def prepare(self, key, node):
        if (key, node) not in self.pipeline_ready:
            return super().prepare(key, node)
        live = self.jobs[key]
        if node in live["engine_jobs"]:
            raise RuntimeError("Native engine request submitted twice")
        job = Job(self.request(key, node))
        live["engine_jobs"][node] = job
        role = "P" if node < self.p_count else "D"
        live["stages"][role] = {
            "prepare": self.now,
            "engine_queued": self.now,
            "node": node,
        }
        if role == "D":
            live["stages"][role].update(
                NPU_initial_prefix_blocks=0, target_prefix_blocks=0, restore_bytes=0
            )
        self.preparing.add((node, key))
        spec = self.specs[key]
        self.native_engines[node].add(
            key,
            job.request.prompt_tokens,
            job.request.output_tokens,
            self.now,
            prefix_hashes=list(job.request.prefix_blocks),
            prompt_token_ids=spec.get("prompt_token_ids"),
            cache_salt=spec.get("cache_salt"),
            priority=spec.get("priority", 0),
        )

    def native_query(self, node, request, computed):
        if node < self.p_count:
            return 0, False
        key = request.request_id
        gpu = computed // self.block_tokens
        row, live = self.rows[key], self.jobs[key]
        source = live["P"]
        stage = live["stages"]["D"]
        stage.setdefault("query_start", self.now)
        state = self.native_queries.get((node, key))
        if state is not None:
            if state["status"] == "querying" or self.now < state.get("ready_at", 0):
                return None, False
            return max(0, state["target"] - gpu) * self.block_tokens, state["target"] > gpu
        target = gpu
        available = min(len(row["hashes"]), (request.num_prompt_tokens - 1) // self.block_tokens)
        if self.config.get("handoff_policy", "full") == "full":
            target = available
        else:
            while target < available and (
                row["hashes"][target] in self.host[node].keys
                or row["hashes"][target] in self.host[source].keys
            ):
                target += 1
        target = max(gpu, target)
        stage.update(NPU_initial_prefix_blocks=gpu, target_prefix_blocks=target)
        if target == gpu:
            self.native_queries[node, key] = {"status": "ready", "target": target}
            return 0, False
        missing = [h for h in row["hashes"][gpu:target] if h not in self.host[node].keys]
        if any(h not in self.host[source].keys for h in missing):
            if (source, key) in self.publications:
                return None, False
            raise ValueError("Producer CPU cache cannot supply native handoff")
        if missing:
            self.native_queries[node, key] = {"status": "querying", "target": target}
            self.host[source].pins.update(missing)
            a, b = self.profile.get("discovery_model", {}).get("coefficients", [0, 0])
            delay = self.profile.get("query_fixed_s", 0) + a + b * len(missing) / 64
            self.event(
                self.now + delay,
                "pipeline_fetch",
                key,
                (node, gpu, target, source, missing),
            )
            return None, False
        self.native_queries[node, key] = {"status": "ready", "target": target}
        return (target - gpu) * self.block_tokens, True

    def restore(self, key, node, gpu, target):
        self.native_queries[node, key] = {
            "status": "ready",
            "target": target,
            "ready_at": self.now + self.idle_poll,
        }
        self.ensure_cycle(node)

    def native_allocated(self, node, request, blocks, external):
        if not external:
            return
        state = self.native_queries[node, request.request_id]
        if state.get("status") == "loading":
            raise RuntimeError("Native restoration allocated twice")
        target = state["target"]
        gpu = target - external // self.block_tokens
        state.update(status="loading", gpu=gpu, native_block_ids=blocks.get_block_ids())
        self.host[node].pins.update(self.rows[request.request_id]["hashes"][gpu:target])
        self.jobs[request.request_id]["stages"]["D"].update(
            NPU_initial_prefix_blocks=gpu,
            restore_bytes=(target - gpu) * self.block_bytes,
            native_restore_block_ids=blocks.get_block_ids(),
        )
        self.fetch_ready[node].append((request.request_id, gpu, target))

    def cycle(self, node):
        while self.loaded[node] and self.loaded[node][0]["feedback_ready"] <= self.now:
            batch = self.loaded[node].popleft()
            for key, _, _ in batch["items"]:
                self.jobs[key]["stages"]["D"].update(
                    restore_end=batch["end"],
                    restore_observed=self.now,
                    completion_feedback_s=self.now - batch["end"],
                )
                self.native_queries[node, key]["status"] = "loaded"
                self.native_engines[node].signal_transfer(key)
        self.native_engines[node].flush_transfers()
        if self.fetch_ready[node]:
            items = list(self.fetch_ready[node])
            self.fetch_ready[node].clear()
            self.copy_sequence += 1
            batch = {
                "batch": self.copy_sequence,
                "node": node,
                "items": items,
                "queued": self.now,
                "blocks": sum(target - gpu for _, gpu, target in items),
                "request_count": len(items),
            }
            for key, _, _ in items:
                self.jobs[key]["stages"]["D"].update(
                    restore_batch=batch["batch"], restore_queued=self.now
                )
            self.copy_queue[node].append(batch)
        if node not in self.flow and self.copy_queue[node]:
            self.start_copy(node)

    def extra_event(self, kind, key, data):
        if kind == "copy_complete":
            node = data[0]
            flow = self.flow.get(node)
            if flow is not None:
                flow["feedback_ready"] = self.now + self.idle_poll
        return super().extra_event(kind, key, data)

    def schedule(self, node):
        if node in self.engine_busy:
            return
        self.current_compute_node = node
        self.cycle(node)
        engine = self.native_engines[node]
        if not engine.scheduler.get_num_unfinished_requests():
            return
        assignments = engine.schedule()
        self.cycle(node)
        if not assignments:
            engine.complete()
            return
        role = "P" if node < self.p_count else "D"
        scheduled = []
        for key, count, before in assignments:
            live = self.jobs[key]
            job = live["engine_jobs"][node]
            stage = live["stages"][role]
            if "engine_start" not in stage:
                stage.update(engine_start=self.now, cached_tokens=before)
                self.preparing.discard((node, key))
            scheduled.append((job, count, before))
        row = features(
            [(before, count, job.request.prompt_tokens) for job, count, before in scheduled]
        )
        duration = self.role_models[role].duration(row)
        self.running[node] = [
            self.jobs[r.request_id]["engine_jobs"][node] for r in engine.scheduler.running
        ]
        self.waiting[node].clear()
        step = {
            "node": node,
            "start": self.now,
            "end": self.now + duration,
            **row,
            "scheduled": {key: count for key, count, _ in assignments},
            "running": len(engine.scheduler.running),
            "waiting": len(engine.scheduler.waiting),
            "scheduler_backend": "vllm",
            "native_preemptions": engine.preemptions,
        }
        self.steps.append(step)
        self.peak_running[node] = max(self.peak_running[node], len(engine.scheduler.running))
        self.engine_busy.add(node)
        self.engine_until[node] = self.now + duration
        self.event(self.now + duration, "step_done", str(node), (node, scheduled))
        self.reserve_cpu_step(node, role, step)
        self.set_compute(node, True)

    def step_done(self, node, scheduled):
        self.set_compute(node, False)
        result = self.native_engines[node].complete()
        for job, _, _ in scheduled:
            key = job.request.id
            state = result["states"][key]
            job.computed, job.generated = state["computed"], state["outputs"]
            if job.first_s is None and job.generated:
                job.first_s = (
                    self.now
                    + self.role_models["P" if node < self.p_count else "D"].data["response_delay_s"]
                )
                if node >= self.p_count:
                    self.jobs[key]["first"] = job.first_s
            if key in result["finished"]:
                job.finish_s = (
                    self.now
                    + self.role_models["P" if node < self.p_count else "D"].data["response_delay_s"]
                )
                self.event(job.finish_s, "request_engine_done", key, node)
        self.running[node] = [
            self.jobs[r.request_id]["engine_jobs"][node]
            for r in self.native_engines[node].scheduler.running
        ]
        self.event(
            self.now + self.role_models["P" if node < self.p_count else "D"].data["dispatch_gap_s"],
            "engine_ready",
            str(node),
            node,
        )

    def engine_done(self, key, node):
        if node < self.p_count and "P_publish_model" not in self.profile:
            self.jobs[key]["stages"]["P"]["engine_end"] = self.now
            self.host[node].add(self.rows[key]["hashes"])
            self.pools[node].release(self.jobs[key]["engine_jobs"][node])
            self.jobs[key]["D"] = self.p_count + self.route(key, "D")
            self.prepare(key, self.jobs[key]["D"])
            return
        return super().engine_done(key, node)

    def run(self):
        try:
            result = super().run()
            result["schema"] = "pegaflow-native-vllm-pipeline/v1"
            result["native_scheduler"] = [engine.identity for engine in self.native_engines]
            result["summary"]["native_preemptions_each"] = [
                engine.preemptions for engine in self.native_engines
            ]
            result["summary"]["D_total_prefill_tokens"] = sum(
                s["prefill_tokens"] for s in self.steps if s["node"] >= self.p_count
            )
            result["settings"]["reserve_restore_pages"] = "native KVCacheManager"
            result["settings"]["scheduler_backend"] = "vllm"
            result["scope"] = (
                "Native vLLM scheduling with calibrated simulated compute and asynchronous cache services"
            )
            result["limitations"].append(
                "Native GPU eviction counts are not collected; GPU_evictions_each is null"
            )
            return result
        finally:
            for engine in self.native_engines:
                engine.shutdown()
