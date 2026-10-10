# Local reliability results: 2026-10-10

All six cohorts completed with reconciled PostgreSQL accounting. The recovery
cohorts reached 900/900 successful jobs after 90 controlled first-attempt failures
and 90 automatic retries. The matching one-attempt-budget controls reached
810/900 successes and 90 expected application dead letters. There were no HTTP
submission errors, unexpected failures, missing records or early retry claims.

Across both cohorts: 1,800 submitted jobs, 1,620 first-attempt successes, 180
failed first-attempt records, 1,890 total attempts and 1,890 unpublished outbox
obligations. The suite took 39.90 seconds including setup and cleanup. These
are controlled task-failure observations on one local machine, not availability
or cloud reliability guarantees.

## Reproduction and evidence

The recorded command was:

```powershell
.venv\Scripts\python.exe scripts\local_reliability.py --output docs/reliability/raw/2026-10-10
```

Use a fresh directory when repeating, such as `--output reliability-results/local-01`.
The [methodology](README.md) defines workload, bounds, accounting and timing.
The [suite artifact](raw/2026-10-10/suite.json) records commands, environment,
image/source/script hashes, settings, run order, summaries and cleanup.
Adjacent `run-01.json` through `run-06.json` retain individual indexed HTTP/UUID
mappings and PostgreSQL job/attempt/outbox rows. Expected control failures remain
in the raw evidence and final counts.

Execution occurred from 14:46:59 to 14:47:39 Europe/Warsaw (UTC+02:00); raw
timestamps use UTC. The source revision was
`20197064f1d8e8c453056c83634cf3317dda8d27`. All 22 application Python files
matched the cached runtime image byte-for-byte. The new reliability script was
uncommitted during measurement; its SHA-256 and the shared benchmark helper's
SHA-256 are recorded separately. No application code or dependencies changed.

## Environment and configuration

| Item | Recorded value |
| --- | --- |
| Host | Windows 11 build 26300; Intel Core i5-14600KF; 14 cores / 20 logical CPUs; 31.72 GiB RAM |
| Docker | Desktop / Engine 28.5.1, local named pipe; WSL2 kernel 6.6.87.2 |
| Docker VM | 20 CPUs; 15.48 GiB RAM |
| Runtime | Python 3.12.15; FastAPI 0.143.0; Pydantic 2.14.0; Uvicorn 0.54.0 |
| Persistence | PostgreSQL 17.11; SQLAlchemy 2.0.54; psycopg 3.3.6; Alembic 1.20.0; revision 0009 |
| Driver | Python 3.13.1; httpx 0.28.1 |
| Resource ceilings | Database 2 CPU/512 MiB; API and worker 1 CPU/256 MiB each; swap disabled |
| Worker | One fresh continuous polling process per cohort, concurrency 5, idle poll 0.05 s; no warmup |
| Submission | Ten concurrent submitters; 300 immediate priority-zero jobs/cohort |
| Retry | Ordinary equal jitter with base/max 1 second; no runtime random seed |
| Fault membership | Exactly 30/300 seeded first-attempt failures; seeds 20261010/20261011/20261012 |
| Budgets/deadlines | Recovery maximum 3 attempts, control maximum 1; 60 s/cohort, 900 s/suite |

Both images were cached locally and used with `--pull never`; no image was built
or downloaded. Docker inspection confirmed the configured CPU/memory/swap limits
and loopback-only API/database bindings during execution. The existing demo and
unrelated containers remained stopped throughout measurement. Other host activity
was not isolated or continuously measured. No cloud, hosted runner or paid service
was used.

## Outcomes and reconciliation

| Cohort, three repetitions | First-attempt successes | Final successes | Dead letters | Attempts | Retries | READY outboxes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Recovery, budget 3 | 810/900 | 900/900 | 0/900 | 990 | 90 | 990 |
| Exhaustion control, budget 1 | 810/900 | 810/900 | 90/900 | 900 | 0 | 900 |

All recovered faults had one `FAILED` attempt with
`{"code": "controlled_failure", "retryable": true}`, followed by one successful
attempt 2 and `{"attempt": 2}`. Healthy jobs had one successful attempt and
`{"attempt": 1}`. Each control fault had exactly one failed attempt and a final
`DEAD_LETTERED` job with `attempts_exhausted` context; its dead-letter timestamp
matched that attempt's finish. All histories remained generation zero.

Mean attempts/job were 1.10 in recovery and 1.00 in control. Automatic retries/job
were 0.10 and 0.00. First-attempt job failure was 10% in both cohorts. Failed
attempt fractions were 90/990 = 9.09% and 90/900 = 10%; these use a different
denominator from job-level fault/dead-letter rates. Recovery conditional on the
injected fault was 90/90 in the recovery cohort; this finite result does not prove
a universal recovery probability.

Every job's index/UUID, configuration, final state, result, attempt count and
timestamps matched its history. Initial outboxes matched creation; each replacement
outbox's availability equaled the failed attempt's persisted retry due time.
All outboxes were unpublished `READY` with zero publication attempts. This is
application dead-letter and durable-intent accounting, not broker DLQ evidence.

## Recovery time and repetition variation

Recovery spans first-failure outcome to final-success outcome. Each row uses 30
recovered-fault observations and nearest-rank percentiles from attempt timestamps:

| Seed | Cohort window s | Recovery p50 s | p95 s | p99 s |
| --- | ---: | ---: | ---: | ---: |
| 20261010 | 3.911 | 0.987 | 1.583 | 1.709 |
| 20261011 | 3.783 | 1.015 | 1.739 | 1.741 |
| 20261012 | 3.901 | 1.012 | 1.726 | 1.730 |

Recovery ranged from 0.606 to 1.741 seconds across the 90 observations. The
following values are medians of the three per-run percentiles, not pooled
percentiles. Components reconcile for every individual recovered job:

| Recovery component | p50 s | p95 s | p99 s |
| --- | ---: | ---: | ---: |
| Chosen backoff | 0.730 | 0.957 | 0.987 |
| Due-to-next-claim delay | 0.252 | 0.899 | 0.988 |
| Final claim-to-outcome | 0.0115 | 0.0141 | 0.0144 |
| Total recovery | 1.012 | 1.726 | 1.730 |

Component percentiles need not sum to the total percentile: they can select
different jobs. Observed chosen backoffs were 0.500282–0.995101 seconds, within
the configured 0.5–1 second interval. No retry began before due. Due-to-claim delay
reached 1.027656 seconds during the finite burst, so retry backoff alone does not
define time to success. The exhaustion control has no recovery samples or
recovery-time percentiles; zero would be misleading.

Successful lifecycle time spans database creation to final successful outcome.
Client completion includes all 300 terminal jobs/cohort, including control dead
letters. Values below are medians of per-run statistics:

| Cohort | Successful lifecycle p50/p95/p99 s | Client terminal observation p50/p95/p99 s |
| --- | --- | --- |
| Recovery | 1.709 / 2.814 / 2.970 | 1.762 / 2.857 / 3.023 |
| Control | 1.646 / 1.960 / 1.993 | 1.692 / 1.997 / 2.043 |

Lifecycle quantiles use 300 successes/recovery run and 270/control run. The
control's dead-letter outcomes remain included in accounting and client terminal
observations. All request and timing distributions remain available in the raw
summaries. PostgreSQL timestamps precede commits; these are persisted-state
intervals containing transaction overhead. Client timings include database polling
and query overhead. Fresh workers and no warmup prevent steady-state interpretation.

## Validation, cleanup and limits

A separate 40-job Docker/HTTP/PostgreSQL smoke produced the expected 20 recovery
successes/two retries and 18 control successes/two dead letters. An intentional
1 ms cohort deadline exited nonzero, retained incomplete evidence and cleaned up.
The full suite also cleaned up all owned resources. Independent Docker inventory
checks preserved all 11 pre-existing container IDs, 26 volume names and six
network names after each invocation.

Independent analysis reproduced all six full summaries from raw rows, including
fault schedules, error/outcome accounting, individual recovery components and all
nearest-rank timing distributions. Both script hashes and all 22 application
source hashes matched. The local virtual-environment `python scripts/check.py`
passed Ruff, strict mypy for 22 source files and all 1,125 tests without skips,
including 144 new harness cases; pytest took 128.36 seconds. `alembic check` reported
no schema drift. The integration database was used only for the standard
isolated-schema tests, separately from disposable experiment databases.

Only fault membership is seeded. Runtime jitter, UUIDs, execution order and
timing vary. Each recovery run has 30 samples; p99 is its observed maximum,
not an SLA estimate. This tests one controlled retryable handler failure and
budget exhaustion. Broker delivery, external effects, worker loss, dependency
outages, cloud deployment and production availability were not measured here.
The [existing failure tests](../../tests/integration) provide separate local
semantics evidence. See the [portfolio guide](../PORTFOLIO.md) for CV wording
and a short local demo.
