# Local performance experiment

This experiment measures the existing API and PostgreSQL polling worker using
finite, deterministic batches. It runs locally, uses no cloud account or hosted
service, and never downloads an image. Results describe this workload and machine;
they are not a production capacity or cloud scalability claim.

## Reproduce

Use the project's installed development virtual environment, a running local
Docker engine, and already available `postgres:17` and
`distributed-cloud-scheduler:local` images. The harness checks that the application
image's Python source matches the working tree. If the image is stale, explicitly
build it locally using the documented setup before benchmarking; the harness
never pulls or rebuilds an image itself. Existing demo containers may remain up.

```powershell
.venv\Scripts\python.exe scripts\local_benchmark.py --output benchmark-results/local-01
```

Choose a fresh output directory for each execution; existing results are not
overwritten. The committed `raw/2026-10-09` directory contains the original
measurements and cannot be reused as output. For a small prerequisite/cleanup
smoke, use a separate fresh path:

```powershell
.venv\Scripts\python.exe scripts\local_benchmark.py --jobs 20 --warmup 5 --repetitions 1 --concurrency 1 20 --workloads calculation sleep --output <fresh-smoke-directory>
```

The harness owns disposable, uniquely named PostgreSQL/API/worker containers and
a Docker network. PostgreSQL and API ports bind only to `127.0.0.1`. Migrations
run explicitly before requests. Only its own database is reset between batches.
It removes owned containers, the PostgreSQL anonymous volume and the network,
including on ordinary failure or interruption. Existing demo data is untouched.
An OS shutdown or forcibly killed harness can prevent cleanup; inspect its recorded
owned resource names before removing any leftover resource.

## Workload and controls

| Parameter | Value |
| --- | --- |
| Calculation | `numbers` 0 through 999; expected sum 499500 |
| Waiting task | Existing `sleep` handler, 0.05 seconds |
| Jobs | 300 measured + 20 excluded warmup per run |
| Repetitions | 3 for each workload/concurrency pair; 24 measured runs |
| Worker | One continuous polling process; concurrency 1, 5, 10, 20 |
| HTTP load | 10 simultaneous submitters; no intentional arrival pacing |
| Submission | Immediate, priority 0, one allowed attempt, no idempotency key |
| Worker idle poll | 0.05 seconds |
| Database pool | Existing SQLAlchemy defaults: 5 retained, 10 overflow per engine |
| Database limits | 2 CPUs, 512 MiB |
| API limits | 1 CPU, 256 MiB |
| Worker limits | 1 CPU, 256 MiB |
| Run/suite deadlines | 60 seconds per batch; 900 seconds total |
| Resource samples | Docker CPU percentage and memory working set, approximately every 2 seconds |

Defaults submit at most 7,680 jobs. CLI hard limits bound jobs to 1,000/run,
warmup to 100/run, repetitions to five, batch duration to 120 seconds and suite
duration to 900 seconds. This includes a drain window; it is not an open-loop
steady-arrival or sustained saturation experiment. Warmup primes connections,
handlers and imports and is excluded from measured job counts and latency.

## Measurement definitions

| Measure | Boundary |
| --- | --- |
| Successful throughput | Reconciled successful jobs / monotonic first-submission-to-final-observation window |
| Submission latency | Client monotonic time immediately before POST through response body receipt |
| Queue latency | PostgreSQL job creation timestamp through attempt claim timestamp |
| Claim-to-outcome latency | Attempt start through persisted outcome timestamp |
| Persisted lifecycle latency | Job creation through final outcome timestamp |
| Client-observed completion | Per-request monotonic start through first terminal DB observation |

PostgreSQL creation, claim and outcome timestamps are sampled before their
transactions commit. Thus these database intervals do not include the final
outcome commit, and claim-to-outcome also contains claim transaction and handler
overhead; it is not pure handler duration. Client completion includes the database
polling interval and query overhead. Database intervals never mix host and database
wall clocks. Request/completion/throughput use one monotonic client clock.

p50/p95/p99 use the exact nearest-rank sample method: sort `n` finite,
nonnegative durations and select rank `ceil(p * n)` (one-based). Per-run samples
and counts are retained. Repetition summaries compare medians and ranges of the
per-run statistics, rather than hiding run variance in one pooled percentile.
The existing Prometheus histograms are not used to derive these sample quantiles.

Resource measurements come from Docker's per-container CPU percentage and
memory working set. Only collection intervals overlapping the measured batch
contribute to resource summaries; a collection may finish after the batch, and
the raw records count these samples explicitly. CPU percentage is relative to
one core (100% means one CPU), and can exceed 100% for PostgreSQL's two-CPU limit.
Memory working set excludes
inactive file cache. These are sampled observations, not complete CPU accounting
or guaranteed memory peaks. Sampling consumes local resources and can overrun
its target interval; retain actual collection times/durations and sample counts.
In a sub-second batch, Docker may establish its CPU baseline after most work
finishes, and an overlapping sample can include post-completion idle time. The
small smoke validates sampling and cleanup, not busy CPU peaks or a bottleneck.

Each measured accepted job must reconcile with exactly one generation-zero
successful attempt, the expected result and one durable dispatch outbox row.
The polling path does not consume outbox rows; no dispatcher/broker is measured.
HTTP errors, unexpected terminal states, missing/extra records, timeout and
resource-sampling failures are retained as failed/incomplete evidence and cause
a nonzero exit. They do not count as successful acceptance runs.

## Interpretation boundaries

Calculation is tiny Python validation/summation work, not a CPU-heavy benchmark.
Sleep simulates asynchronous waiting, not actual external I/O. Changes in worker
concurrency may reach API submission, connection-pool, transaction/lock, CPU or
Docker limits. A throughput plateau alone cannot prove a specific root cause.
Queue growth is expected during a finite burst. Run duration includes load
generation and terminal observation, so it is a workload-specific pipeline rate.

This experiment does not measure scheduler fairness, delayed admission, Pub/Sub,
external side effects, multiple worker replicas or cloud deployment. Background
host activity and the existing demo share the machine. Short runs and 300 samples
limit tail estimates. Increasing concurrency is not a guarantee of linear scaling.

See the [measured results and raw evidence](RESULTS.md). The report includes each
configuration's repetition range, separate latency boundaries, resource usage
and the measured scaling limits.
