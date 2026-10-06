"""Small deterministic handlers used to exercise execution infrastructure."""

import asyncio
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, JsonValue, ValidationError

Number = Annotated[int | float, Field(ge=-1_000_000_000, le=1_000_000_000, allow_inf_nan=False)]


class TaskError(Exception):
    """A safe classification; error messages and submitted payloads are not persisted."""

    def __init__(self, code: str, retryable: bool) -> None:
        super().__init__(code)
        self.code = code
        self.retryable = retryable


class CalculationPayload(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    numbers: list[Number] = Field(max_length=1000)


class SleepPayload(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    seconds: float = Field(ge=0, le=10, allow_inf_nan=False)


class ControlledFailurePayload(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    fail_until_attempt: int = Field(ge=0, le=20)


async def execute_task(
    job_type: str, payload: dict[str, JsonValue], attempt: int
) -> dict[str, JsonValue]:
    """Validate a known handler's payload and return bounded JSON metadata."""
    try:
        if job_type == "calculation":
            calculation = CalculationPayload.model_validate(payload)
            return {"sum": sum(calculation.numbers)}
        if job_type == "sleep":
            sleep = SleepPayload.model_validate(payload)
            await asyncio.sleep(sleep.seconds)
            return {"slept_seconds": sleep.seconds}
        if job_type == "controlled_failure":
            failure = ControlledFailurePayload.model_validate(payload)
            if attempt <= failure.fail_until_attempt:
                raise TaskError("controlled_failure", retryable=True)
            return {"attempt": attempt}
    except ValidationError:
        raise TaskError("invalid_payload", retryable=False) from None
    raise TaskError("unknown_task", retryable=False)
