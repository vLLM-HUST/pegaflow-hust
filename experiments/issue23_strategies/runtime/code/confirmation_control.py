"""Strict three-arm ownership contract, independent of serving phase labels."""

from __future__ import annotations

ARMS = tuple((name+"-"+case,policy) for case in ("300","150","600")
             for name,policy in (("B-native","baseline"),("T-pending","treatment")))
SERVING_PHASE = "baseline"


class Sequence:
    def __init__(self):
        self.cursor = 0
        self.active = None

    def next_arm(self, body):
        if self.active is not None or self.cursor >= len(ARMS) or body != {"phase": SERVING_PHASE}:
            raise ValueError("Invalid confirmation start order/body or active owned workers")
        return ARMS[self.cursor][0]

    def started(self, arm):
        if arm != ARMS[self.cursor][0] or self.active is not None:
            raise ValueError("Confirmation arm was not the next fresh arm")
        self.active = arm

    def check_stop(self, body):
        if self.active is None or body != {"phase": SERVING_PHASE}:
            raise ValueError("No matching owned confirmation arm")

    def stopped(self):
        if self.active is None:
            raise ValueError("No owned confirmation arm to finish")
        self.active = None
        self.cursor += 1

    def finish(self, body):
        if body != {} or self.active is not None or self.cursor != len(ARMS):
            raise ValueError("Incomplete confirmation cannot finish")

