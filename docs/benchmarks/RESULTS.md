# Local benchmark results: 2026-10-09

All 24 measured runs completed: 7,200 jobs succeeded, with zero HTTP submission
errors, failed jobs, missing attempts or accounting mismatches. Each measured job
had its expected result, one successful generation-zero attempt and one READY
dispatch obligation. Another 480 warmup jobs were excluded. The suite took
314.76 seconds including setup, warmup, sampling and cleanup.

The largest median throughput was 70.91 jobs/s for the 50 ms waiting workload at
worker concurrency 10. Raising concurrency to 20 did not improve either workload's
median throughput under these resource limits. These are finite local batches,
not sustained-load or production capacity measurements.

## Reproduction and raw evidence

The recorded invocation was:

```powershell
.venv\Scripts\python.exe scripts\local_benchmark.py --output docs/benchmarks/raw/2026-10-09
```

Use a fresh output directory when repeating, such as `--output benchmark-results/local-01`.
The [methodology](README.md) defines
workloads, bounds, clocks, percentile calculation, resource sampling and cleanup.
The [suite metadata and summaries](raw/2026-10-09/suite.json) record configuration,
commands, image identities, source hashes, versions and actual run order.
The adjacent `run-01.json` through `run-24.json` retain individual requests,
monotonic observations, PostgreSQL job/attempt/outbox records, warmup and Docker
resource samples. No rows were removed to improve the report.

Measurement occurred on 2026-10-09 from 19:37:33 through 19:42:47 Europe/Warsaw
(UTC+02:00). Raw timestamps use UTC. The application source revision was
`ec467896d1ee5a6855755271e3175c860e97e7f6`; all 22 installed application Python
files matched the checkout byte-for-byte. The new harness was not yet committed
when run, so its SHA-256 is recorded separately in the suite metadata.

## Environment and fixed configuration

| Item | Measured environment |
| --- | --- |
| Host | Windows 11, build 26300; Intel Core i5-14600KF, 14 cores / 20 logical CPUs |
| Host RAM | 34,055,766,016 bytes (31.72 GiB) |
| Docker | Docker Desktop / Engine 28.5.1; local named pipe; WSL2 kernel 6.6.87.2 |
| Docker VM | 20 CPUs; 16,616,964,096 bytes RAM (15.48 GiB) |
| Application | Python 3.12.15; FastAPI 0.143.0; Pydantic 2.14.0; Uvicorn 0.54.0 |
| Persistence | PostgreSQL 17.11; SQLAlchemy 2.0.54; psycopg 3.3.6; Alembic 1.20.0, revision 0009 |
| Driver | Host Python 3.13.1; httpx 0.28.1; SQLAlchemy 2.0.54; psycopg 3.3.6 |
| Worker/API limit | One CPU and 256 MiB each; memory swap disabled |
| Database limit | Two CPUs and 512 MiB; memory swap disabled |
| Work | 300 measured jobs/run, 20 warmup, three repetitions, ten HTTP submitters |
| Runtime | One continuous worker, 0.05 s idle poll, default pool 5 + 10 overflow |

Both images were already available locally and were used with `--pull never`.
Their full immutable image IDs appear in the raw metadata. The existing local
API/worker/scheduler/database/emulator demo remained running on the same host;
other host activity was not isolated or continuously measured. No cloud service,
hosted runner or paid load-testing service was used.

## Throughput and persisted lifecycle latency

Each row is the median of three per-run statistics. The range shows the smallest
and largest per-run throughput. Latencies are milliseconds from PostgreSQL job
creation to outcome timestamp, sampled before the respective commits. Each
per-run percentile is the exact nearest-rank value of its 300 job observations;
the table does not pool the repetitions into a new percentile.

| Workload | Concurrency | Jobs/s median (range) | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Calculation | 1 | 43.54 (41.59–51.03) | 2341.04 | 4136.86 | 4253.30 |
| Calculation | 5 | 67.05 (66.05–80.00) | 1151.54 | 1714.43 | 1772.36 |
| Calculation | 10 | 67.55 (62.97–78.99) | 1286.23 | 1974.12 | 2060.40 |
| Calculation | 20 | 66.19 (63.24–68.79) | 1196.52 | 2039.22 | 2097.38 |
| Sleep 50 ms | 1 | 14.38 (14.18–14.40) | 9535.51 | 18152.13 | 18932.12 |
| Sleep 50 ms | 5 | 54.04 (54.03–57.53) | 1959.55 | 3395.23 | 3507.96 |
| Sleep 50 ms | 10 | 70.91 (70.88–71.53) | 1276.73 | 2118.23 | 2176.17 |
| Sleep 50 ms | 20 | 69.23 (63.50–70.07) | 1466.40 | 2326.73 | 2468.22 |

Every cell's measured failure rate was 0/900. This does not establish a universal
zero failure rate. Long lifecycle latency at concurrency 1 mostly represents
queueing behind the finite burst, rather than slow individual task execution.

## Distinct latency boundaries

The following p95 values are also medians of three per-run statistics. Request
and observed completion use the client monotonic clock; queue and claim-to-outcome
use PostgreSQL timestamps. Claim-to-outcome includes transaction and event-loop
overhead, and excludes the final outcome commit; it is not pure handler duration.
All component p50/p95/p99 values are retained in the raw summaries.

| Workload | Concurrency | POST p95 ms | Queue p95 ms | Claim-to-outcome p95 ms | Observed completion p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Calculation | 1 | 107.20 | 4125.92 | 13.98 | 4216.74 |
| Calculation | 5 | 108.87 | 1691.26 | 44.93 | 1814.86 |
| Calculation | 10 | 134.93 | 1910.61 | 77.77 | 2056.04 |
| Calculation | 20 | 134.72 | 1955.23 | 173.42 | 2166.69 |
| Sleep 50 ms | 1 | 107.49 | 18092.27 | 61.46 | 18245.56 |
| Sleep 50 ms | 5 | 106.55 | 3333.57 | 74.39 | 3464.02 |
| Sleep 50 ms | 10 | 119.47 | 2034.43 | 117.81 | 2193.68 |
| Sleep 50 ms | 20 | 119.71 | 2186.60 | 209.52 | 2421.63 |

## Sampled resource usage

Values below are medians of each repetition's sampled peak. Memory is MiB of
working set, excluding inactive file cache. CPU 100% represents one core;
PostgreSQL's two-core ceiling is 200%. Collection intervals overlap a measured
batch but can finish after it and include idle time. Each run had at least one
sample completed before its final observation. Most high-concurrency runs had
only two or three overlapping samples, which cannot establish precise busy peaks.

| Workload | Concurrency | API CPU % / MiB | Worker CPU % / MiB | DB CPU % / MiB | Samples/run median |
| --- | ---: | ---: | ---: | ---: | ---: |
| Calculation | 1 | 94.55 / 67.36 | 70.52 / 61.44 | 53.17 / 69.75 | 4 |
| Calculation | 5 | 96.69 / 66.32 | 99.35 / 62.09 | 69.27 / 80.04 | 2 |
| Calculation | 10 | 97.11 / 66.61 | 98.98 / 62.06 | 69.56 / 93.23 | 2 |
| Calculation | 20 | 99.16 / 66.23 | 99.78 / 64.08 | 75.41 / 97.91 | 2 |
| Sleep 50 ms | 1 | 60.53 / 65.85 | 20.43 / 60.37 | 38.77 / 64.29 | 11 |
| Sleep 50 ms | 5 | 98.35 / 66.32 | 69.11 / 61.71 | 42.43 / 66.21 | 3 |
| Sleep 50 ms | 10 | 81.07 / 65.86 | 99.38 / 62.18 | 68.71 / 81.18 | 2 |
| Sleep 50 ms | 20 | 80.50 / 65.25 | 99.11 / 62.97 | 75.05 / 93.24 | 2 |

The sampler requested a two-second interval; actual Docker command durations
and collection times are retained per sample. Some calls took longer than two
seconds. Throughput excludes waiting for the final sampler command to finish.
The API and worker stayed below their memory ceilings in every sampled observation.
Complete host, Docker-engine and driver resource use was not measured.

## Scaling and bottleneck interpretation

The waiting workload's median throughput rose 4.93 times from concurrency 1 to
10. At concurrency 1, claim-to-outcome p50 was 59.06 ms for a 50 ms timer; job
queueing dominated the full burst's lifecycle latency. More slots allowed waits
to overlap and reduced queue latency. At concurrency 10 and 20, the worker's
median sampled CPU peak was about 99% of one core, while the database was well
below its two-core ceiling. These observations support the worker's one-CPU
budget as a likely constraint in this configuration; adding execution slots
supplied no additional CPU. The samples cannot isolate a single root cause.

Calculation median throughput improved about 1.55 times from concurrency 1 to
10 and then plateaued. Both API and worker sampled peaks approached their one-core
ceilings at higher concurrency. The tiny handler cannot make useful use of more
slots indefinitely: the persisted execution path still requires database claims,
history, outcomes and retry/lease checks. Calculation claim-to-outcome p50 grew
from 10.62 ms at concurrency 1 to 114.21 ms at 20. Sleep's corresponding value
grew from 59.06 ms to 137.02 ms despite the unchanged 50 ms timer.

These observations support CPU-limited application orchestration and increased
concurrent transaction/event-loop overhead as likely contributors to nonlinear
scaling. They do not isolate the precise CPU function, connection-wait duration
or lock cost. The unchanged worker pool admits at most 15 simultaneous database
connections, so concurrency 20 can encounter connection waiting; this experiment
does not separately quantify that contribution. The API also has a one-core budget
and reaches high sampled usage during submission bursts. A new, separately scoped
profiling experiment would be needed before choosing a specific optimization.

Calculation throughput varied substantially across repetitions. Reversing the
configuration order in repetition 2 reduces a simple fixed-order effect, but does
not isolate host load, cache changes or virtualization. Three repetitions and
300 jobs/run provide empirical tail observations, not a statistical service-level
guarantee. The timers do not simulate all real I/O behavior, and no CPU-heavy,
broker, scheduler-fairness, multi-replica or cloud capacity conclusion follows.

## Harness validation

Before the full suite, a real Docker/HTTP/PostgreSQL smoke used 20 jobs, five
warmup jobs, one repetition, both workloads and concurrency 1/20. All 80 measured
jobs reconciled successfully. Its short runs validate the harness, not performance.
A separate calculation smoke with `--run-timeout 0.001 --suite-timeout 60`
returned nonzero with a recorded TimeoutError and retained its incomplete
PostgreSQL/request evidence. Both invocations reported completed cleanup.

The full suite removed all owned containers and its network, including PostgreSQL's
anonymous data volume. An independent before/after check found the same 26 existing
Docker volume names and the same five original demo container IDs. No benchmark
containers or networks remained. Focused tests cover 132 statistical, accounting,
isolation, cancellation, partial-result and cleanup cases; final quality-gate
results are recorded in [PLAN](../PLAN.md).
