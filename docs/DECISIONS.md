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
and deployment follow a stable application model; the queue interface follows
working local execution. Significant scope changes need a new decision.

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
  execution becomes an inspectable failure; automatic re-execution is deferred.
- **Retry ownership:** application attempt count versus broker redelivery, HTTP
  acknowledgements for terminal/transient outcomes, backoff, and exhaustion.
- **Dead-letter state:** broker forwarding versus application terminal state,
  reconciliation, and manual retry eligibility.
- **Scheduling and priority:** due-job claiming, multiple schedulers, restart
  behavior, practical ordering limits, and a suitable scheduler deployment model.

Retry ownership, dead-letter behavior and scheduling/priority remain open.
D010 defines the initial HTTP acknowledgement policy without automatic execution
retries. Later milestones must add specific decisions and regression tests.
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
