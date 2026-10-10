# Portfolio and interview guide

Present this as a local distributed-backend learning project with measurable
correctness and performance evidence. The repository name includes “Cloud”, but
the implemented demo runs locally. Pub/Sub adapters were exercised with a local
emulator; cloud deployment, IAM and production operation were not validated.

## Suggested CV wording

**Distributed Task Scheduler — Python, FastAPI, PostgreSQL, SQLAlchemy, Docker**

- Built a PostgreSQL-backed task scheduler with transactional job/outbox writes,
  atomic concurrent claiming, lease/token ownership, submission idempotency,
  delayed jobs, bounded retries and generation-fenced manual recovery.
- Verified failure and concurrency semantics with real PostgreSQL tests and a
  local Pub/Sub emulator; documented migrations, attempt history, structured
  logging and bounded Prometheus metrics.
- Benchmarked worker concurrency 1/5/10/20 over 7,200 measured local jobs: median
  batch throughput for the 50 ms waiting workload rose from 14.38 to 70.91 jobs/s
  as worker concurrency rose from 1 to 10 under a one-CPU worker limit; concurrency 20
  gave no further median improvement.
- Ran a seeded 10% failure experiment over 1,800 jobs: all 90 injected faults
  recovered in the retry-enabled cohorts; matching one-attempt controls produced
  90 expected dead letters, reconciled with PostgreSQL history and outbox records.

Choose two or three bullets to fit your CV. The measured throughput is a finite-batch
pipeline rate on the recorded host, not production capacity. Use the
[benchmark report](benchmarks/RESULTS.md) when explaining the numbers and the
[reliability report](reliability/RESULTS.md) when discussing controlled retries.
Link your repository beside the project title. Do not describe this as a hosted
service or add a C++ claim.

## Three-to-five-minute local walkthrough

Prepare the local demo using the [README setup](../README.md#local-setup).
With the two images already cached and `.env` configured, these commands start
the documented demo without pulling or rebuilding:

```powershell
docker compose --profile demo up -d --wait --pull never --no-build db
docker compose --profile demo run --rm --pull never api python -m alembic upgrade head
docker compose --profile demo up -d --wait --pull never --no-build api worker scheduler
```

Open [Swagger UI](http://127.0.0.1:8000/docs). Use “Try it out” and show:

1. `GET /ready`: the API verifies its PostgreSQL connection.
2. `POST /jobs`, with a fresh `Idempotency-Key` such as `interview-calculation-01`
   (change its suffix for each presentation):

   ```json
   {"type": "calculation", "payload": {"numbers": [1, 2, 3]}, "max_attempts": 3, "priority": 0}
   ```

   Read the returned ID with `GET /jobs/{job_id}` until `SUCCEEDED`; result is
   `{"sum": 6}`. Repeat the same submission/key: HTTP 200 returns the same job.
   Change the numbers with the same key: HTTP 409 rejects conflicting content.
3. Submit this without an idempotency key:

   ```json
   {"type": "controlled_failure", "payload": {"fail_until_attempt": 1}, "max_attempts": 3, "priority": 0}
   ```

   Inspect `GET /jobs/{job_id}/attempts`: attempt 1 is `FAILED` with a retryable
   controlled error and persisted retry due; attempt 2 is `SUCCEEDED` with result
   `{"attempt": 2}`. The terminal job retains its attempt count.
4. Submit the same controlled payload with `max_attempts: 1`. Inspect its
   `DEAD_LETTERED` status, `attempts_exhausted` context and one failed attempt.
   Explain that budget exhaustion is explicit and retained in PostgreSQL.
5. Point to the raw benchmark/reliability reports and automated failure tests.
   Open [API metrics](http://127.0.0.1:8000/metrics) directly if useful; this
   endpoint is outside Swagger and reports that API process only.

For a scheduled-job example, supply an explicit future UTC `scheduled_at` when
submitting a calculation. Show `SCHEDULED` before due and the successful result
after the separate scheduler admits it. Generate a timestamp about 20 seconds
ahead with the installed interpreter:

```powershell
.venv\Scripts\python.exe -c "from datetime import UTC, datetime, timedelta; print((datetime.now(UTC)+timedelta(seconds=20)).isoformat())"
```

Manual `POST /jobs/{job_id}/retry` takes `{"expected_generation": 0}` for a
`FAILED` or `DEAD_LETTERED` generation-zero job. It opens generation 1 and
preserves earlier history; repeating generation 0 conflicts. The payload and budget remain the
same. Reopening the one-attempt controlled failure above therefore dead-letters
again: resetting a generation does not fix a permanently repeated workload.
Positive worker-loss recovery has separate integration/emulator evidence.

After presenting, `docker compose --profile demo stop` stops the demo and retains
its data. Follow the README teardown instructions only when you intend to remove
the demo containers or database volume.

## Engineering discussion

| Question | Evidence and explanation |
| --- | --- |
| Why PostgreSQL? | Authoritative job state, row locks, constraints and transactional attempt/outbox changes; [D002](DECISIONS.md). |
| How are duplicates handled? | Matching submission keys replay retained identity; claim tokens fence stale completions. At-least-once transport does not guarantee exactly-once external side effects. |
| What survives failure? | Committed job, attempt and dispatch intent; leases allow recovery. In-memory `LocalQueue` contents do not survive process loss. |
| How do retries work? | Persisted due times, bounded budgets, retained failed attempts and generation-fenced manual reopening; [decisions](DECISIONS.md). |
| What does priority guarantee? | Bounded preference with aging among eligible unlocked rows; no preemption, global broker ordering or wall-time service guarantee. |
| Why does scaling flatten? | The worker sampled CPU near its one-core budget, suggesting a constraint; pool/lock waiting was not separately measured, so the exact bottleneck remains unproven. |
| What remains outside scope? | Cloud deployment/IAM, fleet metrics, durable broker guarantees, retention policy, production availability and exactly-once external effects. |

Use [PLAN](PLAN.md) for exact acceptance evidence and
[PROJECT_SPEC](PROJECT_SPEC.md) for scope. A strong explanation connects one
transaction/ownership rule to the failure it prevents, then identifies its limit.
Optional C++ work is unnecessary for demonstrating this Python backend.
