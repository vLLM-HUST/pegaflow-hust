"""Directed transfer paths and max-min payload sharing across finite links."""

import math
from collections import defaultdict
from dataclasses import dataclass


@dataclass(frozen=True)
class TransferPath:
    links: tuple[str, ...]
    latency_s: float = 0
    transport: str = "rdma"


class NetworkTopology:
    def __init__(self, specification=None, *, default_bytes_per_s):
        self.explicit = specification is not None
        self.default_bytes_per_s = default_bytes_per_s
        self.capacities = {}
        self.paths = {}
        if not self.explicit:
            return
        for name, link in specification["links"].items():
            bandwidth = link["bandwidth_GBps"]
            latency = link.get("latency_us", 0)
            if (
                not math.isfinite(bandwidth)
                or bandwidth <= 0
                or not math.isfinite(latency)
                or latency < 0
            ):
                raise ValueError(
                    "Network links need positive bandwidth and nonnegative finite latency"
                )
            self.capacities[name] = bandwidth * 1e9
        for route in specification["routes"]:
            pair = route["source"], route["target"]
            links = tuple(route["links"])
            if pair in self.paths or not links or len(set(links)) != len(links):
                raise ValueError(
                    "Network routes need unique host pairs and nonrepeated links"
                )
            if set(links) - self.capacities.keys():
                raise ValueError("Route refers to an unknown network link")
            latency_us = route.get("latency_us", 0)
            if not math.isfinite(latency_us) or latency_us < 0:
                raise ValueError("Route latency must be nonnegative and finite")
            latency_us += sum(
                specification["links"][name].get("latency_us", 0) for name in links
            )
            self.paths[pair] = TransferPath(
                links,
                latency_us / 1e6,
                route.get("transport", "cpu_copy" if pair[0] == pair[1] else "rdma"),
            )

    def path(self, source, target):
        if self.explicit:
            if (source, target) not in self.paths:
                raise ValueError(f"No network route for {source!r} -> {target!r}")
            return self.paths[source, target]
        links = f"{source}:out", f"{target}:in"
        for name in links:
            self.capacities[name] = self.default_bytes_per_s
        return TransferPath(links)


def max_min_rates(paths, capacities):
    """Progressively fill flow rates, freezing flows at each saturated link."""
    active = set(paths)
    budgets = {name: capacities[name] for links in paths.values() for name in links}
    rates = dict.fromkeys(paths, 0.0)
    while active:
        counts = defaultdict(int)
        for identifier in active:
            for name in paths[identifier]:
                counts[name] += 1
        if not counts:
            raise ValueError("Every flow needs a finite-capacity path")
        increments = {name: budgets[name] / count for name, count in counts.items()}
        delta = min(increments.values())
        for identifier in active:
            rates[identifier] += delta
        for name, count in counts.items():
            budgets[name] = max(0, budgets[name] - delta * count)
        saturated = {
            name for name, value in increments.items() if value <= delta + 1e-6
        }
        active = {
            identifier
            for identifier in active
            if not saturated.intersection(paths[identifier])
        }
    return rates
