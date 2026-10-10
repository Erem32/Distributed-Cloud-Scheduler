# Local reliability experiment

This experiment measures recovery from controlled task failures and exhaustion
of the application's execution budget. It uses the existing handler, API,
PostgreSQL and continuous polling worker. It runs entirely on the developer's
machine, requires no cloud account, downloads no images and uses no hosted service.

## Reproduce

Use the installed development virtual environment and a running local Docker
engine with the existing `postgres:17` and `distributed-cloud-scheduler:local`
images. The harness verifies that the runtime image's application source matches
the working tree. A stale image fails preflight; the harness never pulls or builds.
Use a fresh directory for each invocation:

```powershell
.venv\Scripts\python.exe scripts\local_reliability.py --output reliability-results/local-01
```

For a small prerequisite/accounting/cleanup smoke:

```powershell
.venv\Scripts\python.exe scripts\local_reliability.py --jobs 20 --repetitions 1 --output reliability-results/smoke-01
```

The harness creates uniquely named, owned PostgreSQL/API/worker containers and
a network. Published database/API ports bind only to `127.0.0.1`. Migrations
run explicitly. Each cohort begins with an empty private database; only the
harness's private database is reset. Cleanup removes owned containers, their
anonymous PostgreSQL volume and the network on success, ordinary failure or
interruption. Existing demo resources and data remain separate. If the OS or
process is forcibly stopped, inspect the artifact's owned-resource names before
removing leftovers. Never reuse the committed raw-result directory as output.

## Workload and controls

| Parameter | Value |
| --- | --- |
| Handler | Existing `controlled_failure` |
| Jobs per cohort | 300 |
| Fault selection | Exactly 30 submission indices, sampled without replacement |
| Seeds | 20261010, 20261011, 20261012 |
| Faulted payload | `{"fail_until_attempt": 1}` |
| Healthy payload | `{"fail_until_attempt": 0}` |
| Recovery cohort | Maximum attempts 3 |
| Exhaustion control | Maximum attempts 1; same seed/fault indices |
| Repetitions/order | Three paired repetitions; alternating cohort order |
| Worker | One continuous process, concurrency 5, idle poll 0.05 seconds |
| HTTP | Ten submitters, finite unpaced burst |
| Submission | Immediate, priority 0, no idempotency key |
| Execution retry | Equal jitter, base/max 1 second; first backoff 0.5–1 second |
| Database pool | Unchanged: 5 retained connections + 10 overflow per engine |
| Database limits | 2 CPUs, 512 MiB |
| API/worker limits | 1 CPU and 256 MiB each |
| Deadlines | 60 seconds/cohort; 900 seconds/suite |

Only fault membership is seeded. The exact submission-index-to-UUID mapping and
the selected retry due times are saved. UUIDs, execution order, runtime jitter
and timing remain variable. No production failure switch or jitter change is
introduced. This workload creates retryable `controlled_failure` errors only;
unknown, permanent or infrastructure errors invalidate the run.

CLI hard limits allow 10–1,000 jobs/cohort in multiples of ten, 1–5 repetitions,
at most 120 seconds/cohort and 900 seconds/suite. The default suite submits 1,800
jobs; the largest permitted configuration submits 10,000. CPU/memory limits and
deadlines bound resource use. This is a reliability observation, with no warmup
or capacity claim. The separate [performance experiment](../benchmarks/README.md)
measures concurrency scaling.

## Accounting and acceptance

Expected counts for each default seed:

| Cohort | First-attempt success | Final success | Application dead letters | Attempts | Automatic retries | Outboxes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Budget 3 | 270 | 300 | 0 | 330 | 30 | 330 |
| Budget 1 control | 270 | 270 | 30 | 300 | 0 | 300 |

Initial success means a successful attempt-1 record, not a snapshot before all
retries begin: submissions and retries can overlap. A failed attempt remains
`FAILED` in history even when its final job is `DEAD_LETTERED`. The control's
expected dead letters demonstrate budget exhaustion and count as valid
experiment observations. Unexpected terminal outcomes, HTTP failures, missing
or extra records, early retries and nonterminal jobs at the deadline fail the
experiment. Partial evidence is retained and the command exits nonzero.

Each submission index must map to one unique job. The harness reconciles job
type/payload/budget, generation, attempt count, result/error, final timestamps,
retry metadata and dead-letter context with contiguous attempt records and
dispatch obligations. Recovery faults must have one retryable failure followed
by a successful attempt 2 with result `{"attempt": 2}`. Control faults must
have one retryable failed attempt, no chosen retry due time, and job-level
`attempts_exhausted` context. Healthy results must be `{"attempt": 1}`.

The polling worker executes directly from PostgreSQL. Initial and replacement
outbox rows stay `READY`, with no publication attempts. Replacement availability
must equal the chosen retry due time. These rows establish durable dispatch
intent; this experiment does not measure publication or a broker DLQ.

## Timing definitions

| Measure | Boundary |
| --- | --- |
| Persisted recovery | First failed attempt's finish → final successful attempt's finish |
| Chosen backoff | First failure's finish → persisted `next_attempt_at` |
| Due-to-claim delay | Persisted retry due → second attempt's start |
| Final claim-to-outcome | Second attempt's start → second attempt's finish |
| Successful lifecycle | Job creation → final successful outcome |
| Client completion | Monotonic per-request start → first terminal database observation |

For every recovered fault, the three recovery components must sum to persisted
recovery. Retry start cannot precede its due time. The job's `started_at` describes
its latest attempt; attempt history supplies the first-attempt timestamps.

Database timestamps are sampled before their transactions commit. Recovery and
claim-to-outcome include persistence/transaction overhead and exclude the final
commit; they are not pure handler duration. Database intervals use one clock.
Client completion uses a separate monotonic clock and includes polling/query
overhead. No interval subtracts a host wall clock from a database timestamp.

All p50/p95/p99 values use nearest rank: sort `n` finite nonnegative durations
and select one-based rank `ceil(p * n)`. Reports retain counts and individual
observations. Each default recovery cohort has only 30 samples, making its p99
the observed maximum. A control with no recovered faults has absent recovery
quantiles, not zero recovery time. Repetition ranges expose timing variation.

## Evidence and limitations

Raw artifacts record machine/runtime/image/source metadata, commands, exact
configuration, the reliability and shared-helper script hashes, fault schedules,
HTTP/client timings and PostgreSQL job/attempt/outbox snapshots. A run succeeds
only after accounting is reconciled; the suite records owned-resource cleanup.
See [measured results](RESULTS.md) for actual counts and timings.

This establishes outcomes for finite seeded handler failures under the recorded
retry budgets on one local host. It does not establish production availability,
an SLA, external-side-effect idempotence, broker delivery, dependency outages,
worker crashes or cloud behavior. Existing failure tests cover additional local
semantics, but their guarantees must be assessed separately. The same host and
Docker virtualization affect timings; runtime jitter prevents exact timing
reproduction. An exhausted job is not automatically reopened; manual generation
recovery is a separate API operation.
