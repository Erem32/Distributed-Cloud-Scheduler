"""The stable submission identity contract does not need database or HTTP access."""

import re

import pytest

from distributed_cloud_scheduler.idempotency import (
    submission_fingerprint,
    validate_idempotency_key,
)
from distributed_cloud_scheduler.jobs import JobCreate


@pytest.mark.parametrize("key", ["a", "A", "job:7", "a.b_c-7", "a" * 128])
def test_valid_keys_preserve_case_and_content(key: str) -> None:
    assert validate_idempotency_key(key) == key


@pytest.mark.parametrize(
    "key",
    [
        "",
        "a" * 129,
        " leading",
        "trailing ",
        "with space",
        "comma,separated",
        "quote\"",
        "slash/path",
        "tilde~",
        "nonascii-é",
        "a\n",
        "a\r",
        "a\t",
        "a\x00",
    ],
)
def test_invalid_keys_are_rejected_without_normalization(key: str) -> None:
    with pytest.raises(ValueError, match="Idempotency-Key"):
        validate_idempotency_key(key)


def test_object_order_unicode_escaping_and_explicit_defaults_are_equivalent() -> None:
    original = JobCreate.model_validate_json(
        b'{"type":"calculation","payload":{"z":[{"b":2,"a":1}],"a":"\\u00e9"}}'
    )
    replay = JobCreate(
        type="calculation", payload={"a": "é", "z": [{"a": 1, "b": 2}]}, max_attempts=3
    )
    digest = submission_fingerprint(original)
    assert re.fullmatch(r"[0-9a-f]{64}", digest)
    assert submission_fingerprint(replay) == digest


@pytest.mark.parametrize(
    "changed",
    [
        {"type": "sleep", "payload": {"values": [1, 2]}},
        {"type": "calculation", "payload": {"values": [2, 1]}},
        {"type": "calculation", "payload": {"values": [1, 2]}, "max_attempts": 4},
        {"type": "calculation", "payload": {"values": [1, 2], "extra": None}},
        {"type": "calculation", "payload": {"values": [1, 3]}},
    ],
)
def test_changed_submission_fields_produce_different_fingerprints(
    changed: dict[str, object],
) -> None:
    original = JobCreate(type="calculation", payload={"values": [1, 2]})
    assert submission_fingerprint(JobCreate.model_validate(changed)) != submission_fingerprint(
        original
    )


@pytest.mark.parametrize(
    ("left", "right"),
    [(1, 1.0), (0, 0.0), (-0.0, 0.0), (True, 1), (False, 0), (None, "null")],
)
def test_parsed_number_types_signed_zero_and_boolean_types_remain_distinct(
    left: object, right: object
) -> None:
    first = JobCreate.model_validate({"type": "calculation", "payload": {"value": left}})
    second = JobCreate.model_validate({"type": "calculation", "payload": {"value": right}})
    assert submission_fingerprint(first) != submission_fingerprint(second)


def test_float_spellings_with_the_same_parsed_value_are_equivalent() -> None:
    first = JobCreate.model_validate_json(b'{"type":"calculation","payload":{"value":1e0}}')
    second = JobCreate.model_validate_json(
        b'{"type":"calculation","payload":{"value":1.000}}'
    )
    assert submission_fingerprint(first) == submission_fingerprint(second)


def test_large_integer_precision_is_preserved_without_binary_float_rounding() -> None:
    large = 2**100 + 1
    first = JobCreate(type="calculation", payload={"value": large})
    rounded = JobCreate(type="calculation", payload={"value": large - 1})
    floating = JobCreate(type="calculation", payload={"value": float(large)})
    assert first.payload["value"] == large
    assert len(
        {submission_fingerprint(first), submission_fingerprint(rounded),
         submission_fingerprint(floating)}
    ) == 3


def test_strings_are_exact_without_unicode_normalization() -> None:
    first = JobCreate(type="calculation", payload={"value": "é"})
    second = JobCreate(type="calculation", payload={"value": "e\u0301"})
    assert submission_fingerprint(first) != submission_fingerprint(second)
