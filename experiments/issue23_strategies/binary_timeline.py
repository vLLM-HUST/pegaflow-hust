"""Exact addition of finite binary64 durations, without a fitted time quantum.

Every binary64 value is an integer multiple of 2**-1074. Integer arithmetic
therefore preserves the supplied primitive costs exactly, even when identical
iterations are grouped differently. This does not make estimated costs exact.
Only the public endpoint is rounded back to binary64.
"""

import math

SCALE = 1 << 1074


def ticks(value):
    value = float(value)
    if not math.isfinite(value):
        raise ValueError("A timeline requires finite binary64 inputs")
    numerator, denominator = value.as_integer_ratio()
    return numerator << (1074 - (denominator.bit_length() - 1))


def seconds(value):
    return value / SCALE
