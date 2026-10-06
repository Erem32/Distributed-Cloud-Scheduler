"""Application retry timing, independent of transport and runtime configuration."""

import math
import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Capped exponential equal jitter; persisted outcomes choose the delay once."""

    base_seconds: float = 1
    max_seconds: float = 60

    def __post_init__(self) -> None:
        for value in (self.base_seconds, self.max_seconds):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or not 0.001 <= value <= 300
            ):
                raise ValueError("Retry delays must be finite and between 0.001 and 300 seconds")
        if self.base_seconds > self.max_seconds:
            raise ValueError("Maximum retry delay must be at least the base delay")

    def delay_seconds(self, attempt_number: int, *, sample: float | None = None) -> float:
        """Use an explicit sample in deterministic tests; production samples once."""
        if (
            isinstance(attempt_number, bool)
            or not isinstance(attempt_number, int)
            or not 1 <= attempt_number <= 20
        ):
            raise ValueError("attempt_number must be an integer between 1 and 20")
        value = random.random() if sample is None else sample
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 0 <= value <= 1
        ):
            raise ValueError("sample must be finite and between 0 and 1")
        cap = min(self.base_seconds * 2 ** (attempt_number - 1), self.max_seconds)
        return float(cap * (0.5 + 0.5 * value))
