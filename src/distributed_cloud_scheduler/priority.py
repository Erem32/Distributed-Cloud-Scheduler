"""A finite priority head start with deterministic eligibility-based aging."""

from datetime import datetime
from typing import cast

from sqlalchemy import SQLColumnExpression, literal_column
from sqlalchemy.sql import ColumnElement

MAX_PRIORITY = 9
PRIORITY_HEAD_START_SECONDS = 5


def priority_rank(
    eligible_at: SQLColumnExpression[datetime], priority: SQLColumnExpression[int]
) -> ColumnElement[datetime]:
    """Rank eligible work in UTC; older work eventually beats fresh urgent arrivals.

    Fixed UTC conversion keeps interval arithmetic independent of session timezone
    and makes the job expression indexes immutable. Eligibility gates stay separate.
    """
    utc = eligible_at.op("AT TIME ZONE")(literal_column("'UTC'"))
    return cast(
        ColumnElement[datetime],
        utc - priority * literal_column(f"INTERVAL '{PRIORITY_HEAD_START_SECONDS} seconds'"),
    )
