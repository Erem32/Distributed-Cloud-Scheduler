# Architecture decisions

Record significant decisions here with context, alternatives, reasoning, and
consequences. Update or supersede entries when evidence changes a decision.
Accepted decisions describe choices, not proof of implemented behavior; see
[PLAN.md](PLAN.md) for implementation and validation status.

## D001 — Local-first delivery in validated milestones

**Date:** 2026-10-05

**Status:** Accepted

**Decision:** Establish engineering tooling, then FastAPI/PostgreSQL, job
persistence, local execution, and controlled concurrency before introducing
queue adapters, Pub/Sub, or GCP infrastructure. Keep one application project
with eventual API, worker, and scheduler entry points.

**Context:** The project must demonstrate distributed-systems reasoning while
remaining reproducible without a cloud account. Its initial repository contains
only a README heading and development rules.

**Alternatives:** Build the final cloud system immediately; create multiple
service repositories; begin with Kubernetes or a generic execution framework.

**Reasoning:** Small milestones isolate correctness questions and keep failures
diagnosable. A shared project avoids deployment complexity before independent
services offer a measured benefit. Local execution provides a second genuine
transport use case before adding an adapter contract.

**Consequences:** No cloud dependencies in the initial foundation. A milestone
cannot finish without its acceptance checks and current documentation. Terraform
and deployment were originally reserved for a stable application model; D019 now
defers them indefinitely under the user's zero-cost scope. Significant scope
changes need a new decision.

## D002 — PostgreSQL owns authoritative job state

**Date:** 2026-10-05

**Status:** Accepted

**Decision:** Use PostgreSQL locally and Cloud SQL for PostgreSQL in the cloud.
Persist authoritative jobs, attempts, and ownership/idempotency data in the
database. Queue messages primarily identify jobs rather than duplicate their
authoritative state.

**Context:** Concurrent workers, duplicate submissions, retries, and scheduled
jobs need durable invariants and explainable atomic transitions.

**Alternatives:** In-memory state; SQLite as a replacement for PostgreSQL tests;
broker messages as the only job record; an additional state database.

**Reasoning:** Database transactions, constraints, and row locks provide concrete
tools for these invariants. PostgreSQL row locks protect rows until the
transaction ends; the exact claiming design still needs concurrency tests.
See [PostgreSQL explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html).

**Consequences:** Tests of transaction/locking behavior must use real PostgreSQL;
in-memory doubles cannot establish database correctness. Broker receipt is not
execution ownership. Database schema and claim rules must be designed before
jobs run concurrently. A committed job and a published message are not one atomic
operation; D009 resolves this gap with a transactional outbox. D007 defines
database execution ownership and its concurrency validation.

## D003 — Typed Python API with async SQLAlchemy and psycopg

**Date:** 2026-10-05

**Status:** Accepted

**Decision:** Use Python 3.12+, FastAPI/Pydantic, SQLAlchemy 2.x async engines and
sessions with psycopg 3, and Alembic for explicit schema migrations. Use
environment-based settings; create/dispose application resources through the
application lifecycle. Each concurrent operation owns its own session and
transaction boundaries are explicit.

**Context:** The requested stack supports an asynchronous API and later bounded
I/O concurrency. The initial design should remain small and typed.

**Alternatives:** Synchronous-only database access; asyncpg; a second framework;
automatic schema creation on application startup.

**Reasoning:** SQLAlchemy's psycopg dialect supports both sync and async engines,
allowing one driver family for application and migration tooling. Its async
session represents stateful transaction work and must not be shared between
concurrent tasks. See [psycopg dialect support](https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg)
and [AsyncSession concurrency](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#using-asyncsession-with-concurrent-tasks).

**Consequences:** Async code requires deliberate session lifetimes and awaited
I/O. CPU-bound work will require separate evaluation; asyncio alone does not
provide CPU parallelism. Migrations are an explicit command/deployment step,
not an implicit side effect of starting every replica. No generic repository
framework or dependency injection framework is introduced.

## D004 — Pub/Sub push to Cloud Run is the provisional cloud target

**Date:** 2026-10-05

**Status:** Provisional; validate in messaging and reliability milestones

**Decision:** Target an authenticated Pub/Sub push subscription invoking a Cloud
Run worker HTTP endpoint. Keep PostgreSQL authoritative. Design for duplicate
delivery and perform durable outcome handling before acknowledging a successful
execution, subject to the retry model chosen later.

**Context:** The requested GCP stack favors managed request-driven execution.
The initial diagram is a target, not a reliability guarantee.

**Alternatives:** Pub/Sub pull workers; a permanently running worker fleet;
Kubernetes; another broker. Revisit push if measured job-duration or recovery
requirements make its request model unsuitable.

**Reasoning:** Push delivery naturally reaches an HTTP worker. Pub/Sub treats
specified HTTP success statuses as acknowledgements and redelivers after a
negative response or acknowledgement deadline expiry; individual push message
deadlines cannot be extended. See [Pub/Sub push behavior](https://docs.cloud.google.com/pubsub/docs/push).
Pub/Sub's exactly-once delivery feature supports pull, not push; application
idempotency remains necessary. See [exactly-once subscription support](https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery).

**Consequences:** An acknowledgement lost after database commit may cause another
delivery. Job duration, acknowledgement deadlines, Cloud Run request limits,
concurrency, and connection budgets need joint validation. Authenticated delivery
and service IAM belong to deployment; end-user accounts remain outside scope.
No exactly-once side-effect claim follows from this choice.

The following choices govern this architecture:

- **Durable dispatch (D009):** a transactional outbox closes the commit/publish gap;
  publication failures and expired publisher leases remain replayable.
- **Ownership and recovery (D007/D008):** atomic database claims and fenced
  completion reject duplicate IDs while another worker owns execution. Expired
  execution becomes an inspectable failure; D012 retries it while budget remains.
- **Retry ownership (D012):** PostgreSQL counts application attempts, persists
  backoff/history and atomically commits replacement dispatch before retry ACK.
- **Dead-letter state:** broker forwarding versus application terminal state,
  reconciliation, and manual retry eligibility.
- **Scheduling and priority:** due-job claiming, multiple schedulers, restart
  behavior, practical ordering limits, and a suitable scheduler deployment model.

Dead-letter behavior and general scheduling/priority remain open. D010 defines
the initial acknowledgement policy; D012 adds application retries and ACK after
durable retry intent. Later milestones must add decisions and regression tests.
Local outbox/ownership validation does not establish cloud delivery behavior.

## D005 - Portable development checks and API startup

**Date:** 2026-10-05

**Status:** Accepted

**Decision:** Use a setuptools `src` package, a project virtual environment and
`python scripts/check.py` as the single portable quality command. Start the API
with `python -m distributed_cloud_scheduler`. Its asyncio Runner selects a
selector loop on Windows; tests use the same factory through pytest-asyncio 1.4's
loop-factory hook. Other platforms retain their default loop.

**Context:** Development must work on this Windows workspace and in Linux
containers. psycopg's async driver does not support Windows' default Proactor
loop. A plain Uvicorn invocation can select that incompatible loop.

**Alternatives:** Require Make and a POSIX shell; introduce another package
manager; document Linux-only startup; globally mutate deprecated asyncio policies.

**Reasoning:** Standard Python tools minimize setup. A scoped Runner makes the
runtime loop explicit without changing global process policy. The same loop in
tests exercises the runtime's database compatibility. See
[psycopg asyncio compatibility](https://www.psycopg.org/psycopg3/docs/advanced/async.html)
and [pytest-asyncio loop factories](https://pytest-asyncio.readthedocs.io/en/stable/how-to-guides/multiple_loops.html).

**Consequences:** Install the package before testing. Use the module entry point
on Windows; don't assume every third-party server launcher selects a compatible
loop. pytest-asyncio 1.4+ is a development requirement. The command runs Ruff,
strict application mypy and pytest; database/container validation remains an
additional milestone requirement.

## D006 - Persist jobs before introducing execution

**Date:** 2026-10-05

**Status:** Accepted

**Decision:** Milestone 2 persists a UUID job, task identifier, JSON object payload,
status, attempt budget and execution timestamps. New submissions remain PENDING.
Retain the proposed six-state vocabulary with a database CHECK constraint; define
execution/retry transitions in later milestones. Introduce scheduled time and
priority when their behavior is implemented.

**Context:** Persistence and its public contract need independent validation before
worker ownership or queue transport can complicate failure diagnosis.

**Alternatives:** Execute as a FastAPI background task; publish at submission time;
add every eventual field immediately; introduce a generic repository framework.

**Reasoning:** Small typed data-access functions and PostgreSQL constraints suffice
for current use. Validate payloads as finite JSON objects, limited to 64 KiB of
compact UTF-8 JSON. Reject NUL characters and invalid Unicode before JSONB storage:
[PostgreSQL JSONB restrictions](https://www.postgresql.org/docs/17/datatype-json.html)
are narrower than all syntactically valid JSON. Created timestamps come from the
database and use timezone-aware columns.

**Consequences:** POST returns a persisted job, not an execution promise. Milestone
2 introduced no broker or worker; D007-D009 add execution and durable dispatch
incrementally. Database constraints guard status, task identifier, JSON
object shape and attempt bounds. List pagination initially uses bounded offset
and limit, sorted by creation time and UUID descending. This is simple to inspect
but pages can shift under concurrent inserts; it does not provide a snapshot
across requests. Revisit keyset pagination only when requirements justify it.

## D007 - Short database claims and fenced local completion

**Date:** 2026-10-05

**Status:** Accepted for the local execution milestone

**Decision:** Select eligible jobs in a short transaction with `FOR UPDATE SKIP
LOCKED`, record RUNNING with a unique claim token, lease expiry and incremented
attempt count, then commit before execution. Completion is a compare-and-set
update requiring RUNNING, the matching token and an unexpired lease. Completion
first acquires the row lock and then samples the database clock, so a lease that
expires during lock contention cannot authorize a stale write. Persist bounded
deterministic handler results or classified errors.

**Context:** Concurrent claim contenders must not share ownership, but holding
database transactions open while tasks execute wastes connections and locks.
A claim without crash detection can strand RUNNING jobs indefinitely.

**Alternatives:** Select then update without locks; execute inside a transaction;
use an in-memory ownership flag; introduce a broker before proving local claims.

**Reasoning:** PostgreSQL owns the invariant and the token fences stale database
completion. Lease timestamps use the database clock. Ownership and execution are
separate, explainable steps and require real-database contention tests.

**Consequences:** PENDING -> RUNNING -> SUCCEEDED or FAILED is the implemented
local path. D009 adds QUEUED -> RUNNING and ID-specific claiming using the same
ownership rules. A task exception, invalid task payload, timeout or expired owner is
recorded as FAILED. Expired owners are made inspectable by worker polling;
re-execution/backoff waits for the deliberate retry model in milestone 8.
These initial terminal-error semantics are superseded by D012; its completion
also finalizes attempt history and retry dispatch in the fenced transaction.
Milestone 4 adds bounded concurrent work and graceful shutdown. Existing leases
exceed the bounded task timeout by a margin; defer periodic renewal until longer
execution requirements justify the extra moving part.
A token does not fence an external side effect; handlers are deterministic and
no exactly-once side-effect guarantee is claimed.

## D008 - Bounded worker loops and graceful draining

**Date:** 2026-10-05

**Status:** Accepted

**Decision:** Run a configurable fixed number of async polling loops in each
worker process, sharing an engine with operation-scoped sessions. Shutdown stops
new admission, allows in-flight work to drain for a configurable grace period,
then cancels remaining work with bounded best-effort fenced cleanup. CLI signal
handlers request that shutdown on SIGINT/SIGTERM. `--once` retains its single-job
meaning regardless of configured concurrency.

**Context:** Unbounded task creation can overload the database and complicates
shutdown. Current deterministic handlers have bounded execution times and do not
require another processing framework.

**Alternatives:** Spawn a task for every pending row; share an AsyncSession;
introduce multiprocessing for I/O handlers; renew leases continuously despite
task budgets already fitting within them.

**Reasoning:** Fixed loops give a clear per-process admission bound. PostgreSQL
claims coordinate independent processes. Stop/drain/cancel can be tested with
barriers instead of timing guesses. Leases exceed task timeouts, so periodic
renewal adds no current capability and is deferred.

**Consequences:** Database connection budgets still multiply with process count;
deployment must size replicas and pools jointly. Cleanup and driver cancellation
can extend shutdown beyond the grace period; unrecoverable cleanup leaves a lease
for subsequent polling recovery. Database connection/pool failures pause polling
and retry; unexpected worker errors propagate after resource cleanup. Future
CPU-bound work requires an explicit isolation design.

## D009 - Transactional outbox before broker integration

**Date:** 2026-10-05

**Status:** Accepted

**Decision:** Persist a dispatch outbox row with each job in the same transaction.
A separate dispatcher leases rows, publishes job identifiers outside transactions,
then marks publication accepted using its current token and lease. Publish failures and
expired publisher ownership remain retryable; completed local jobs can have their
unneeded dispatch rows explicitly skipped. Introduce only an async publisher
protocol plus a local/test queue adapter. Local database polling remains supported.

**Context:** Database commit and broker publication cannot be one atomic operation.
Publishing directly after commit can silently strand a job if the process crashes.

**Alternatives:** Best-effort publish after commit; publish before persistence;
hold a database transaction open across a network call; add a general message bus.

**Reasoning:** PostgreSQL gives a durable dispatch obligation in the existing
transaction. Lease-based retry closes the loss window without another database.
Publishing before marking completion can replay a message after a crash, which
the existing job claim rules must handle.

**Consequences:** Dispatch is at least once; duplicate messages remain expected.
Publication attempts are separate from job execution attempts. The in-memory
queue is a local/test adapter, not a durable broker guarantee. The direct polling
runtime remains cloud-independent. Broker retry and execution retry policy are
still separate decisions; this milestone does not automatically retry FAILED jobs.
Publication retry uses capped exponential backoff with sanitized error codes;
retry attempts do not consume the execution attempt budget. Revision 0004 backfills
existing active jobs without altering their execution state. The adapter is invoked
explicitly in tests; a production dispatcher runtime and outbox retention policy
were deferred to the transport/deployment and operations stages. D010 adds the
Pub/Sub dispatcher runtime; its cloud hosting and outbox retention remain deferred.

## D010 - Pub/Sub adapters and acknowledgement after durable outcomes

**Date:** 2026-10-06

**Status:** Accepted for milestone 6; cloud IAM and deployment validation deferred

**Decision:** Publish only canonical UUID ASCII bytes using the asynchronous
Google Pub/Sub client behind the existing publisher protocol. Disable implicit
SDK RPC retries and bound each RPC; the durable outbox owns publication retries.
Use a separate HTTP push worker runtime and validate wrapped envelopes against
the expected subscription, strict base64 data, a canonical UUID and an 8 KiB
request body limit. Load task data and execution attempts from PostgreSQL.

Execute within the request, using existing token/lease claims and fenced outcome
commits. Admit at most WORKER_CONCURRENCY requests per process, returning 503
immediately when full. Bound total request work separately from the handler
deadline. A targeted expired owner is durably FAILED/worker_lost before ACK;
other jobs are untouched. Current FAILED outcomes remain terminal even when
their classification is retryable; milestone 8 owns execution retry policy.

HTTP 204 acknowledges a persisted terminal outcome, including duplicate delivery
and handler failure. Active/contended ownership, rejected completion, exhausted
capacity, request timeout and dependency failures return 503. Unknown IDs return
404, malformed envelopes 400 and oversized bodies 413. These responses request
redelivery; malformed/unknown messages can repeat indefinitely until operational
dead-letter policy is configured. They are not silently treated as successful jobs.

**Context:** Pub/Sub push acknowledges only 102, 200, 201, 202 or 204. Its
subscription acknowledgement deadline also bounds the HTTP request, and individual
push deadlines cannot be extended. ACKing a RUNNING duplicate before the owner
commits could remove the redelivery needed to observe a crashed owner's lease.
See [push behavior](https://docs.cloud.google.com/pubsub/docs/push) and
[subscription deadlines](https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions).

**Alternatives:** ACK immediately and rely on background work; ACK every unclaimed
ID; treat broker delivery attempts as application attempts; put the endpoint in
the public submission API; add custom JWT verification before deployment design;
use the high-level threaded batching publisher for single leased obligations.

**Reasoning:** Awaiting execution keeps Cloud Run work within an active request,
and PostgreSQL remains authoritative. Separate runtimes allow service IAM to
protect the worker without making the submission API private. An async unary
publisher avoids a thread/batch lifecycle and makes bounded transport failures
visible to the outbox. A cancelled RPC can still have been accepted by the broker;
ambiguous acceptance remains at least once and is safe only through shared claims.

**Consequences:** Local emulator HTTP does not authenticate senders. Future
deployment requires a private Cloud Run worker, Pub/Sub push OIDC identity with
run.invoker, token-creation permissions, and the correct audience. Subscription
matching is routing validation, not authentication. Cloud Run verifies the OIDC
token through service IAM; see [authenticated push](https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions)
and [Cloud Run Pub/Sub tutorial](https://docs.cloud.google.com/run/docs/tutorials/pubsub).
Default task/request budgets are 15/45 seconds; provision at least 60-second
subscription and Cloud Run request deadlines to include cancellation cleanup.
Database cancellation/connection cleanup can still extend latency. Replicas,
connection budgets and dispatcher hosting must be designed at deployment.
The [emulator](https://docs.cloud.google.com/pubsub/docs/emulator) validates SDK
publication and push delivery locally, but lacks IAM and production guarantees.
No database migration, automatic execution retry, submission idempotency or cloud
provisioning is introduced by this decision.

## D011 - Submission identity belongs to the job transaction

**Date:** 2026-10-06

**Status:** Accepted for milestone 7

**Decision:** Accept one optional `Idempotency-Key` header containing 1-128 ASCII
letters, digits or `._:-`. Keys are exact, case-sensitive and global within this
database; no account/tenant scope exists. Retain the key while its job exists,
without expiry or cleanup in this milestone. Store a unique nullable key and its
SHA-256 request fingerprint on `jobs`, with paired NULL/format CHECK constraints.
Neither field appears in public job representations.

Fingerprint the validated submission including effective defaults, using compact
UTF-8 JSON with sorted object keys. Object order, request whitespace, Unicode
escape spelling and omitted/explicit default max_attempts are equivalent. Arrays
remain ordered and strings exact, without Unicode normalization. Preserve parsed
numeric representations: integer 1 differs from float 1.0 and signed float zero
differs from unsigned float zero. Float spellings yielding the same parsed float
match. This application canonicalization does not claim RFC 8785 compliance or
coerce large integers to binary64. Store the original fingerprint permanently;
JSONB normalization and later execution state must not change submission identity.

Within one transaction insert the job using PostgreSQL ON CONFLICT DO NOTHING
against the key. Only the winning insert adds an outbox obligation. After a
conflict, read the committed key in a separate statement at READ COMMITTED and
compare the original fingerprint. Return 201 for new jobs, 200 with current job
state for matching replay, and sanitized 409 for differing content. Reject invalid
or repeated header fields with 422, even when repeated values match. No-key
submissions retain their existing independent 201 behavior.

**Context:** A retry after a lost HTTP response must not create another job or
dispatch obligation. Independent replicas must share the identity decision, and
failure after insertion must roll back the key, job and outbox together.
PostgreSQL READ COMMITTED can suppress an ON CONFLICT DO NOTHING insert due to a
concurrent transaction outside the insert's snapshot. A subsequent statement sees
its commit; a combined insert/select CTE might not. See
[PostgreSQL isolation](https://www.postgresql.org/docs/17/transaction-iso.html) and
[SQLAlchemy PostgreSQL upserts](https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#insert-on-conflict-upsert).

**Alternatives:** Process-local key caches; a separate key table with expiration;
SELECT-before-INSERT; a no-op upsert UPDATE; swallowing all IntegrityErrors;
snapshotting and replaying the original HTTP response; numeric coercion that can
erase large-integer distinctions.

**Reasoning:** Nullable job metadata preserves historical submissions and uses
the existing database transaction. A unique constraint serializes contenders
without worker locks or a second retention lifecycle. Returning current state
lets a replay observe execution that happened since submission, while immutable
submission fingerprints distinguish conflicts. Existing claim tokens/leases
already fence transport duplicates and remain the execution authority.

**Consequences:** Clients should generate sufficiently unique keys, such as UUIDs,
and reuse a key only for the same validated content. A global key is not a secret
or an authorization mechanism. There is no time-based reuse guarantee; job deletion
or a future retention policy would need an explicit decision. Uniqueness waits
can add latency; transaction cancellation must release incomplete submissions.
Retrying the same key resolves a lost or ambiguous commit response. Future
submission fields or fingerprint changes need an explicit compatibility decision
for existing key bindings before changing this contract.
Migration 0005 adds only nullable metadata and constraints; downgrade removes
idempotency bindings while preserving jobs/outbox. Duplicate deliveries across
replicas and polling retain D010 acknowledgement behavior. No automatic execution
retry or exactly-once external side-effect guarantee follows from submission identity.

## D012 - PostgreSQL owns execution retries and their durable dispatch

**Date:** 2026-10-06

**Status:** Accepted for milestone 8; validation evidence recorded in PLAN

**Decision:** Application attempts, recorded in PostgreSQL, govern execution retries.
Broker deliveries and outbox publication attempts do not consume execution budgets.
Each claim inserts one RUNNING job_attempts record atomically with ownership and
attempt_count. Fenced completion or expired-owner recovery finalizes that record.
Transient failures schedule RETRY_WAIT while budget remains; permanent failures
and exhaustion become terminal FAILED. Worker loss, cancellation, handler timeout,
internal error and controlled failure are transient; invalid payload/unknown task
are permanent. DEAD_LETTERED and manual retry remain milestone 9.

The outcome transaction persists next_attempt_at on job/history and adds a new
outbox obligation available at that same time. Choose capped exponential equal
jitter once: cap = min(base * 2^(attempt-1), maximum), delay in [cap/2, cap].
Defaults are base 1 second and maximum 60 seconds; both accept 0.001-300 seconds
to retain positive timing at database precision. Claims use the database clock
and reject early retries even when an old message arrives. Publication leaves
RETRY_WAIT intact; retry state changes only when a due claim succeeds. A restart
or duplicate does not recalculate timing or consume a new attempt.

Push ACKs both terminal outcomes and committed RETRY_WAIT, including early retry
duplicates. Its new outbox obligation survives a lost ACK or process restart.
Active ownership/contention, failed/expired completion and dependency failures
still return 503. This supersedes D010's terminal-only ACK and terminal treatment
of retryable errors; envelope, admission and timeout boundaries stay unchanged.
Expired recovery schedules only the delivered job in push and a bounded batch in
polling, retaining token/lease fencing. No execution sleeps inside a request to
wait for retry backoff, and no background task is needed to retain retry intent.
After flushing outcome/history/outbox writes, completion rechecks the database
clock and rolls back if its lease elapsed during a write lock wait. A due retry
whose claim is locked remains a 503 rather than acknowledging uncommitted ownership.

Expose next_attempt_at in job reads and ordered attempt history through
GET /jobs/{id}/attempts, bounded by the current 20-attempt maximum. Ownership
tokens remain internal. History begins with this migration; backfill only known
active RUNNING ownership, never fabricate completed pre-migration attempts.
Existing FAILED jobs remain terminal. Downgrade maps newly waiting retries to
FAILED before restoring the old lifecycle and removes timing/history; it retains
all other job fields and all publication records.

**Context:** Broker redelivery alone cannot provide an authoritative application
attempt budget or restart-stable backoff. A retry ACK without durable replacement
dispatch could strand work; leaving a transient message outstanding can cause
unnecessary requests while the application backoff is pending.

**Alternatives:** Broker delivery count as authority; NACK until backoff elapsed;
in-process timers; a new retry scheduler/runtime; resetting an existing outbox row.

**Reasoning:** New delayed obligations reuse the tested outbox without letting a
publisher for the previous attempt finalize a new generation. A dedicated wait
state separates execution eligibility from publication state and protects due
gating across transport/polling races. One atomic outcome transaction preserves
history, job state and retry intent through partial failure.

**Consequences:** A dispatcher must run for push retries to progress; polling can
claim due retries directly. New obligations may be skipped after polling completes
the job. Independent replicas may have different settings, but the committing
finalizer's persisted due time governs the next attempt. Clock and lock contention
can delay execution beyond the due time. Retries may repeat handler side effects;
ownership fencing still does not establish exactly-once external effects. Migration
requires coordinated runtime upgrades; old binaries cannot interpret RETRY_WAIT.

## D013 - Terminal exhaustion and generation-fenced manual recovery

**Date:** 2026-10-06

**Status:** Accepted for milestone 9; validation evidence recorded in PLAN

**Decision:** Retryable failure at max_attempts becomes DEAD_LETTERED with final
safe error, database-clock dead_lettered_at and reason attempts_exhausted.
Permanent failures remain FAILED. This supersedes D012's terminal exhaustion
classification; its retry scheduling, history and acknowledgement model stays in
effect. Attempt records describe individual execution success/failure; job state
describes the generation's exhaustion decision.

Manual recovery requires POST /jobs/{id}/retry with strict expected_generation.
Under a job row lock, compare the generation and allow only FAILED/DEAD_LETTERED;
increment generation, clear current ownership/outcome/timing/dead-letter context,
reset attempt_count and insert an immediate outbox obligation atomically. Return
202 for the winner, 409 for stale/ineligible/overflow commands, 404 for missing
jobs and 422 for invalid bodies. Generations are nonnegative PostgreSQL integers,
initially zero. Each has the unchanged max_attempts budget. Preserve immutable
submission data, creation time, key/fingerprint and all earlier records. Permanent
failures may recur until a handler fix makes the original immutable input valid.

History keys are (job_id, generation, attempt_number). Read history chronologically
with bounded limit/offset and optional generation filter. Execution claims and
outbox obligations carry generation. Current token/lease and generation fence
completion; an old publisher can finalize its own accepted row but cannot queue
a reopened generation. Claiming skips obsolete publication obligations. Manual
recovery never locks/updates old outbox rows while holding the job lock, preserving
the existing publisher's outbox -> job lock order. ID-only delivery remains valid:
the database authorizes the current eligible generation, irrespective of which
publication produced a duplicate ID.

Broker dead-letter topics quarantine undeliverable/unacknowledged messages.
Pub/Sub forwarding is best effort and its attempt threshold is approximate;
application attempt_count and terminal state never derive from that threshold.
A lost ACK may cause forwarding after successful execution, so inspection must
join the original job ID to current database state before any intentional recovery.
Do not feed a wrapped forwarded message blindly back into the normal push adapter.
Production DLQ topic/subscription/IAM/monitoring configuration belongs to milestone
15. Google's emulator supports dead-letter forwarding, but cannot validate cloud
IAM and production guarantees; this milestone validates application dead letters
and manual recovery through normal live emulator delivery. See Google's
[dead-letter responsibilities](https://docs.cloud.google.com/pubsub/docs/dead-letter-topics)
and [emulator limitations](https://docs.cloud.google.com/pubsub/docs/emulator).

**Context:** Resetting attempt_count against the former two-column history key
would collide with previous attempts. A state-only manual retry can also accept
an old delayed command after a newer generation fails, reopening work twice.

**Alternatives:** Delete prior history; maintain one lifetime attempt ordinal;
create a separate job for recovery; rely on broker delivery counts; add a separate
manual-command idempotency key/cache.

**Reasoning:** Generations retain bounded per-run attempt semantics and preserve
the job's submission identity. Required generation comparison makes concurrent
and delayed commands explicit without another identity lifecycle. A lost response
is resolved by reading current generation; repeating the old command conflicts.
The existing outbox retains recovery intent through restart/publication failure.

**Consequences:** History/outbox retention remains future work; repeated explicit
recovery can grow retained history. This does not promise exactly-once external
effects. Migration 0007 preserves old records/statuses as generation zero rather
than guessing historical exhaustion; legacy dead-letter metadata may be unknown.
Coordinate runtime upgrades. Downgrade refuses if any job, attempt or obligation
has positive generation, preventing history loss or silent reversal of recovery.
Stop runtimes for schema migration. Downgrade acquires exclusive table locks before
checking generations; the check and schema removal share one transaction, closing
the race with an in-flight manual recovery.

## D014 - Durable scheduled admission and compatible submission identity

**Date:** 2026-10-06

**Status:** Accepted for milestone 10; validation evidence recorded in PLAN

**Decision:** Add optional aware scheduled_at, normalized to UTC before persistence
and fingerprinting. HTTP timestamps are RFC 3339 with explicit offset/Z, seconds
and at most six fractional digits. Reject naive/date-only/numeric/invalid/leap-second/UTC-overflow
input. Past dates are accepted. Every non-NULL date starts SCHEDULED; omitted/NULL
dates retain immediate PENDING + outbox behavior. No recurrence or schedule edits.

SCHEDULED is durable admission responsibility and initially has no outbox. A
transport-independent scheduler samples the PostgreSQL clock and selects due
SCHEDULED rows in scheduled_at/id order with a bounded batch and FOR UPDATE SKIP
LOCKED. The same transaction changes them to PENDING and inserts one fresh
current-generation outbox per job. Admission makes no network call and needs no
persisted lease. Row locks and state transition prevent duplicate admission;
rollback/cancellation retains intent, and a crash after commit retains dispatch.
The due partial index bounds selection to unadmitted work. Skipped rows are
revisited; no strict global admission or execution ordering is promised.

Polling and ID claims never execute SCHEDULED. Initial-generation PENDING/QUEUED
claims independently check the original due time against the database clock.
Push ACKs SCHEDULED at any due time: successful admission supplies fresh outbox
delivery; rollback leaves the durable row for a later scheduler pass. Due execution
contention continues to NACK. Defensively skip any publication obligation observed
while SCHEDULED; admission creates its own fresh one. Retry timing still belongs to
next_attempt_at. Manual recovery preserves the original schedule/identity and opens
an immediate new generation under D013, with no second initial wait.

Fingerprint NULL/omitted dates by excluding scheduled_at entirely, retaining every
existing D011 digest byte. Non-NULL dates contribute a normalized UTC ISO timestamp
with six fractional digits. Equivalent offsets/spellings identify the same instant;
different dates conflict under one key. Keep the original digest and timestamp
through admission, automatic retry and manual recovery. Migration 0008 leaves all
legacy dates NULL and preserves jobs, keys, history and obligations unchanged.
Downgrade locks all three tables before checking and refuses when any original
schedule remains, including completed work, protecting date/key interpretation.

The scheduler CLI admits one bounded batch with --once or runs a local polling
loop, owning its engine and using portable stop/drain/cancel handling. Continuous
mode retries database outages; --once surfaces errors. Settings bound batch size,
poll interval and shutdown grace. Deployment must choose managed invocation or
appropriate always-running CPU for admission if cloud deployment is ever
separately authorized; local polling is not evidence that an idle Cloud Run
service will execute background work.

**Context:** Pub/Sub cannot be the authority for arbitrary delayed eligibility.
An early duplicate ID must neither execute future work nor erase its future
dispatch responsibility. Extending submission content must not break retained keys.

**Alternatives:** In-process timers; immediately publishing future jobs and relying
on repeated NACKs; a future-dated initial outbox with no explicit admission state;
scheduler leases/network work in a transaction; recomputing existing fingerprints.

**Reasoning:** Explicit scheduled intent gives the scheduler one cohesive database
operation and keeps transport/execution boundaries unchanged. Atomic admission
reuses the outbox, with no broker dependency or long-lived ownership. NULL omission
provides an explicit compatibility rule for old keys without guessing old payloads
after JSONB normalization.

**Consequences:** A scheduler must run even for already-due timestamped submissions.
The PostgreSQL clock defines eligibility; host wall clocks do not. Execution can
be later due to polling/locks/outages/dispatch/capacity, and clock changes affect due
eligibility. Original scheduling metadata has no retention/cleanup API yet; guarded
downgrade is deliberately unavailable while it exists. Coordinated stopped-runtime
upgrades remain required. No priority, cloud provisioning or new dependency is added.

## D015 - Bounded priority preference with readiness-based aging

**Date:** 2026-10-06

**Status:** Accepted for milestone 11; validation evidence recorded in PLAN

**Decision:** Add immutable strict integer priority 0..9, default 0. Give each level
five seconds of preference, at most 45 seconds. Among due, unlocked candidates,
order by eligibility timestamp in UTC minus priority * five seconds, then eligibility
timestamp and UUID. This is linear aging with a fixed head start: older eligible
work eventually ranks ahead of fresh highest-priority arrivals without score updates,
process-local counters or a fairness coordinator. This replaces the date-only
selection order in D014, while retaining all due/ownership/admission semantics.

Persist one internal jobs.ready_at for the current readiness episode. Submission
sets it to greatest(database creation time, scheduled_at), or creation time for
immediate work. Past dates cannot fabricate age. Admission and publication retain
it; automatic retry resets it to its persisted next_attempt_at, and manual recovery
resets it to the current PostgreSQL time. Keep original priority, schedule, hash
and history through all generations. The field is absent from public input/reads;
GET /jobs remains ordered by creation time, independent of selection preference.

Scheduler admission and polling execution rank jobs using ready_at. Due/status,
attempt budgets and ownership remain independent eligibility gates. A high rank
never advances future eligibility. ID-specific claims/push continue to claim the
delivered eligible ID without consulting other jobs' priorities.

Dispatch joins authoritative immutable Job.priority and ranks READY by available_at
and reclaimable PUBLISHING by lease_expires_at. Publication backoff and expired
leases start a new dispatch waiting episode, separate from execution readiness.
Lock FOR UPDATE OF dispatch_outbox SKIP LOCKED; the join must not lock jobs or
reverse established outbox-before-job finalization order. Keep broker IDs and
outbox schema unchanged; no redundant priority snapshot is required.

Rank in UTC explicitly before subtracting the interval. PostgreSQL timestamptz
interval arithmetic depends on session timezone; timestamp arithmetic after
AT TIME ZONE 'UTC' provides an immutable expression for the two partial job rank
indexes. Match PostgreSQL's typed index representation in metadata so ordinary
Alembic drift checks remain meaningful. Retain existing due/lease indexes for
eligibility filtering. Dispatch's joined expression needs candidate sorting;
query cost and tuning remain benchmark work, with no performance claim here. See
[PostgreSQL date/time operators](https://www.postgresql.org/docs/17/functions-datetime.html)
and [expression indexes](https://www.postgresql.org/docs/17/indexes-expressional.html).

Exclude effective priority 0 from submission fingerprint content, retaining all
earlier D011/D014 bytes for omitted or explicit defaults. Include nonzero integers
in immutable identity; changing priority under a retained key conflicts. Migration
0009 adds only priority/default/range, readiness and rank indexes. Backfill retry
readiness from next_attempt_at, recovered generation readiness from its earliest
current-generation outbox creation time, and other rows from greatest(creation,
schedule); fall back to the initial baseline when no recovery obligation exists.
Earlier closed attempt readiness is not reconstructed. Preserve every existing
field, original digest, attempt and obligation. Downgrade acquires exclusive locks
on all three tables before guarding and refuses while any nonzero priority remains,
including completed jobs, protecting original request identity. Default-only rows
may return to 0008; its original scheduling downgrade guard still applies.

**Context:** Strict priority can starve low-priority work under sustained arrivals.
Creation time alone gives a newly retried or manually recovered old job unearned
age. In-memory burst counters cannot give independent replicas a shared policy.
Pub/Sub ordering is scoped to ordering keys and is not an application-wide priority
mechanism; see [Google's ordering contract](https://docs.cloud.google.com/pubsub/docs/ordering).

**Alternatives:** Strict descending priority; original creation-time aging; mutable
effective scores; shared fair-selection counters; priority queues/topics or broker
ordering keys; copying priority into every outbox obligation.

**Reasoning:** A fixed head start gives urgency a clear finite meaning. Readiness
episodes keep retries and recovery honest while static ranks need no clock-dependent
index or background updates. Reusing existing claims preserves concurrency fencing;
the immutable join avoids copying submission data into dispatch records.

**Consequences:** Continuously eligible, unlocked work older than the maximum
head start outranks fresh urgent arrivals given advancing database time, available
capacity and selector progress. Older backlog, locks/outages and clock adjustments
can still delay work; no finite wall-time service guarantee follows. Concurrent
selectors, skipped locks, publication and broker arrival/prefetch may reorder work;
running tasks are never preempted. This does not establish global execution order,
resource isolation, tenant fairness or exactly-once effects. The fixed priority
range/quantum and rank indexes form one policy; changing them requires coordinated
schema/design work. No new dependency, cloud resource or hosting policy is introduced.

## D016 - Bounded local observability and redacted lifecycle events

**Date:** 2026-10-09

**Status:** Accepted

**Decision:** Emit lifecycle events as one-line JSON with a fixed allowlist of
fields: event, service, job ID, task type, attempt, status, duration in seconds
and sanitized error code where applicable. Correlate submission, scheduler
admission, dispatch and attempt events by canonical job UUID. Do not log submitted
payloads, HTTP headers, idempotency keys, database URLs, ownership tokens or
arbitrary exception text. Expose `/metrics` in the API and push HTTP applications
using Prometheus text exposition and in-process counters, gauges and cumulative
histograms. Metric labels are limited to finite service/outcome/route/status
vocabularies; never label by job ID, task type, exception, or other user input.

Define `jobs_submitted_total` as submission requests, separating created jobs
from replays; `dispatches_total` as completed publisher attempts
partitioned by accepted/failed; `job_attempts_total` as fenced, durably committed
execution outcomes, including expired-owner recovery; `job_retries_total` and
`job_dead_lettered_total` as committed transitions; `active_executions` as
execution claims currently being processed, including outcome persistence; and
`http_requests_total`/`http_request_duration_seconds` by route template and status.
`job_attempt_duration_seconds` measures handler wall time, excluding claim and
outcome-transaction time, and is observed only after a fenced outcome commits.
Histogram quantiles are estimated from documented cumulative buckets using
Prometheus `histogram_quantile`; throughput is a rate of counters over a selected
window and error/dead-letter rates use completed attempt outcomes as their
denominator.

**Context:** Job payloads and several request fields are caller-controlled or
sensitive. Unbounded metric labels can multiply time-series storage. Existing
runtimes include separate API, polling worker, dispatcher, scheduler and push
processes; no fleet-wide collector or cloud monitoring resource exists yet.

**Alternatives:** Log raw request/exception objects; put UUIDs or task types in
metric labels; add a vendor SDK/exporter now; claim globally aggregated metrics
from process-local state.

**Reasoning:** JSON logs provide portable Cloud Logging ingestion while remaining
easy to inspect locally. Prometheus text is a simple scrape contract with no new
runtime dependency. Explicit label and redaction boundaries protect cardinality
and data exposure. Counters remain honest about each process's observations.

**Consequences:** `/metrics` reports only the current process and does not combine
replicas or provide durable history; fleet aggregation, hosted collection,
retention/alerting and cloud export are deferred to deployment. API and push HTTP
services expose the scrape endpoint. CLI worker/dispatcher/scheduler lifecycle
events still go to structured logs; their in-memory metrics are not remotely
scrapable in this milestone. Histogram quantiles are approximate, bucket-bound
estimates, not exact per-event percentiles. Logs deliberately omit payloads,
arbitrary exception text and secrets, so detailed debugging must use sanitized
error codes and persisted attempt state.

## D017 - One non-root multi-role runtime image

**Date:** 2026-10-09

**Status:** Accepted

**Decision:** Build a single multi-stage Python 3.12 slim image that installs the
runtime project dependencies and runs directly as UID 10001. The default process
is the API; worker and scheduler use their existing Python module CLIs in the same
image. Keep the default local Compose services unchanged and expose long-running
worker/scheduler services only through an opt-in profile. Probe API readiness at
`/ready`; retain `/health` as dependency-independent liveness. Run Alembic as an
explicit one-off command before starting application roles, never automatically
inside replica startup.

**Context:** API, worker and scheduler already have portable entry points and
signal-aware shutdown. The existing API exposes separate liveness and readiness
endpoints, but Compose had no HTTP probe and the image installed its package in
the same stage that became runtime.

**Alternatives:** Build separate role images; run migrations from each replica;
use a shell entrypoint that obscures signal delivery; run polling roles in the
default development Compose startup; add a dependency manager and lockfile only
for this container milestone.

**Reasoning:** One artifact keeps Python/runtime dependencies consistent across
roles. A builder stage avoids carrying build-only files and dev/test dependencies
into the final image. Direct exec-form commands let Uvicorn and the async CLIs
receive SIGTERM as PID 1. Explicit migration execution avoids startup races and
ambiguous failure ownership. The opt-in role profile allows lifecycle validation
without changing the existing local default.

**Consequences:** The project dependency ranges remain the source of runtime
dependency constraints; image builds resolve compatible versions at build time,
so rebuilds are not guaranteed to be byte-for-byte identical. A built OCI image
is the deployment artifact and its digest identifies the exact resolved image.
Compose grants worker/scheduler up to 65 seconds to stop, matching the configured
60-second maximum drain plus margin. Non-HTTP roles have no HTTP healthcheck.

## D018 - Zero-spend boundary for cloud deployment

**Date:** 2026-10-09

**Status:** Superseded by D019

**Decision:** Initially allow local Terraform preparation but prohibit applying
it, enabling GCP APIs, creating resources, deploying services, or running
cloud-side planning/refresh operations. The contemplated target region was
`europe-west1` with an IAM-private API.

**Context:** This is a learning project, and the user explicitly authorized no
GCP spending while selecting Terraform-only preparation. Terraform and `gcloud`
are not installed in the current environment.

**Alternatives:** Provision a trial environment and rely on teardown or budgets;
perform read-only cloud planning against an account; pause all milestone work.

**Reasoning:** Avoiding cloud operations is the only reliable way to respect the
zero-spend constraint. Budgets, teardown, and a plan that contacts GCP are not
equivalent to guaranteeing no cloud-side activity. Local Terraform authoring
does not create GCP resources, but source validation remains blocked until the
Terraform CLI is available.

**Consequences:** Superseded by D019, which defers cloud deployment indefinitely
and replaces this milestone with local-only demo hardening.

## D019 - Cost-free local demo; defer cloud deployment

**Date:** 2026-10-09

**Status:** Accepted

**Decision:** Keep the project demo local-only. Do not provision or run GCP
resources, enable paid APIs, use hosted/billable CI, or require cloud credentials.
Use loopback-bound Docker Compose services for PostgreSQL, API, worker and
scheduler; keep Pub/Sub optional and emulator-only. Preserve the existing default
Compose startup and provide an opt-in profile for the full local demo.

**Context:** The user requires the entire learning/demo project to remain
cost-free and approved replacing the cloud-deployment milestone with local-only
demo hardening. Cloud resources, free-tier limits and hosted-runner quotas cannot
guarantee zero spend.

**Alternatives:** Rely on cloud free tiers, budgets, resource teardown or
unapplied Terraform; continue with GCP as the planned final demonstration.

**Reasoning:** Local PostgreSQL, application processes and the Pub/Sub emulator
exercise the project's behavior without creating cloud resources or relying on
usage quotas. Avoiding cloud and hosted CI is the only reliable way to keep the
project itself from generating cloud/service charges.

**Consequences:** Cloud architecture references are educational/deferred only;
they are not deployment instructions or validated behavior. No Terraform,
cloud IAM or production delivery acceptance is required. Any change that adds a
hosted or billable service needs separate explicit user authorization.

## D020 - Bounded isolated local performance measurements

**Date:** 2026-10-09

**Status:** Accepted for milestone 17; validation evidence recorded in PLAN

**Decision:** Use the existing Python/httpx development dependencies to drive real
loopback HTTP into the already available local runtime image. Create uniquely
named, disposable Docker PostgreSQL/API/polling-worker resources with explicit
CPU/memory/job/time limits and `--pull never`. Leave existing demo services and
data alone. Compare worker concurrency 1, 5, 10 and 20 for finite calculation and
50 ms sleep batches with fixed submission concurrency, excluded warmup and three
repetitions. Keep runtime behavior, handlers and database pooling unchanged.

Persist individual HTTP/monotonic timings and authoritative PostgreSQL job/attempt
timestamps, outcomes and accounting, plus Docker CPU/memory samples. Compute
nearest-rank percentiles from raw samples, rather than estimating them from the
existing histogram buckets. Report successful throughput over the entire measured
submission-and-drain window. Separate request, queue, claim-to-outcome, persisted
lifecycle and client-observed completion timing. Reconcile each expected result
with one generation-zero attempt and one dispatch obligation. Remove only resources
created by this invocation, including their anonymous PostgreSQL volume.

**Context:** The user needs credible, reproducible CV evidence without cloud or
hosted services. Existing API/worker/attempt records and deterministic handlers
already supply a complete measurable path. An active local demo may coexist with
the experiment, and the worker's default pool (5 retained + 10 overflow) may limit
concurrency 20 independently of its execution slots.

**Alternatives:** Hosted load testing; installing a separate load platform; using
in-process ASGI calls; measuring only sleep duration; silently tuning the runtime
or replacing PostgreSQL; treating histogram estimates as exact percentiles.

**Reasoning:** A small finite harness measures the supported local runtime without
new dependencies or application changes. Disposable containers provide explicit
resource ceilings and repeatable state. Raw observations make every reported
quantile and accounting claim reviewable.

**Consequences:** These are finite local batch measurements, not production,
cloud, sustained-arrival or exactly-once guarantees. PostgreSQL timestamps are
sampled before commits; claim-to-outcome includes database/transaction overhead
and is not pure handler duration. Client completion includes polling delay.
Docker CPU and memory are sampled observations (working set excludes cache), not
continuous profiling or complete host resource accounting. Resource collection
intervals must overlap the batch but can finish after it, including idle time;
sub-second smokes establish plumbing, not busy CPU peaks. Calculation is tiny
and sleep simulates waiting; neither establishes CPU-heavy performance, scheduler
fairness, broker capacity or global scalability. Background host activity, Docker
virtualization and finite samples affect results. Unexpected failures and incomplete
runs must be retained/reported honestly, and are not successful acceptance runs.
