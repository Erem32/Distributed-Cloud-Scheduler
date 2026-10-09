"""Pure submission identity rules, independent of transport and persistence."""

from __future__ import annotations

import hashlib
import json
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from distributed_cloud_scheduler.jobs import JobCreate

IDEMPOTENCY_KEY_PATTERN = r"^[A-Za-z0-9._:-]{1,128}$"
_KEY_PATTERN = re.compile(IDEMPOTENCY_KEY_PATTERN)


class IdempotencyConflict(Exception):
    """An existing submission key identifies different validated request content."""


def validate_idempotency_key(key: str) -> str:
    """Require one bounded ASCII key without trimming or case normalization."""
    if not isinstance(key, str) or _KEY_PATTERN.fullmatch(key) is None:
        raise ValueError("Idempotency-Key must contain 1-128 ASCII letters, digits or ._:-")
    return key


def submission_fingerprint(submission: JobCreate) -> str:
    """Hash validated fields and defaults with stable object order and exact JSON types.

    Array order, strings, integer versus float values, and signed float zero remain
    significant. This hashes the validated request rather than a JSONB round trip.
    """
    fields = submission.model_dump(mode="python", exclude={"scheduled_at", "priority"})
    # Immediate submissions retain their original D011 bytes, including existing keys.
    if submission.scheduled_at is not None:
        fields["scheduled_at"] = submission.scheduled_at.isoformat(timespec="microseconds")
    # Effective default priority preserves all earlier immediate and scheduled digests.
    if submission.priority != 0:
        fields["priority"] = submission.priority
    encoded = json.dumps(
        fields,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
