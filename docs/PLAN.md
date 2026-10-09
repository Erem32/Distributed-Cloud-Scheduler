# Project status

- **Current milestone:** 15 - free local demo hardening (COMPLETE; awaiting
  approval to start milestone 16).
- **Current state:** Milestones 0-15 pass their local acceptance criteria.
  On Python 3.13.1, `python scripts/check.py` passes Ruff, strict mypy (22 source
  files), and all 849 tests without skips. PostgreSQL is at revision 0009 and
  `alembic check` reports no drift. The live Windows Pub/Sub emulator smoke passes;
  PostgreSQL 17 and the emulator are healthy. The multi-role Python 3.12 image
  builds, runs API/worker/scheduler as UID 10001, and passes readiness and SIGTERM
  lifecycle smokes.
- **Milestone 12 design:** Emit one-line JSON lifecycle events with an allowlisted
  field set and job ID correlation across API submission/retry, scheduler
  admission, dispatch and execution. Never log payloads, headers, idempotency
  keys, database URLs, ownership tokens or arbitrary exception text. Expose
  dependency-free Prometheus text metrics at `/metrics` in the API and push HTTP
  services; metrics are process-local, and only finite service/outcome/route/status
  labels are permitted. Histograms record HTTP request latency and each handler
  attempt's execution duration; p50/p95/p99 are estimated from cumulative buckets
  with Prometheus `histogram_quantile`, not asserted as exact values.
- **Milestone 12 commit:** `f18448f6519eb01f565673e54f6b02426955229b`.
- **Milestone 13 commit:** `7b65a8573067e9417e91c1a95357b8fa0798ab94`.
- **Milestone 13 design:** Preserve real-PostgreSQL assertions for durable state
  and allowed effects; use synchronization/fault injection rather than timing
  assumptions; retain isolated random schemas and mandatory database execution.
  A shared migration fixture upgrades ordinary integration-test schemas to head;
  tests intentionally validating historical revisions retain their own migration
  lifecycle. Existing PostgreSQL and live-emulator suites already cover the
  required failure scenarios, so no duplicate behavior tests or second harness
  were added.
- **Milestone 13 validation:** The focused PostgreSQL integration set passed 266
  tests. `python scripts/check.py` passes Ruff, strict mypy (22 source files), and
  all 849 tests with no skips. The Windows live Pub/Sub SDK/emulator/push smoke
  passed, including terminal and duplicate ACKs, transient retry and exhaustion,
  publisher outage recovery, worker-loss/manual recovery, delayed scheduling and
  restart, priority selection/fairness, and malformed/unknown/oversized messages.
  PostgreSQL regression tests also cover handler/publisher exceptions, lock
  contention, competing schedulers, worker shutdown/loss, duplicate delivery and
  delayed admission.
- **Milestone 14 validation:** Docker Compose validates both default and `roles`
  configurations. The image builds to about 79 MB and runs as UID 10001 without
  pytest, mypy, Ruff, httpx, tests, scripts or `.env`. In an isolated Compose
  project, the explicit migration command reached revision 0009; API `/health`
  and `/ready` returned 200; API, worker and scheduler all started and exited 0
  on SIGTERM. `python scripts/check.py` passes Ruff, strict mypy (22 source files)
  and all 849 tests with no skips.
- **Milestone 15 scope:** User requires the learning/demo experience to remain
  cost-free and approved replacing cloud deployment with local-only demo
  hardening. Do not provision, plan against, or deploy to GCP; do not enable
  paid APIs or use hosted/billable CI. Terraform and `gcloud` are not installed
  and are no longer required for this milestone.
- **Milestone 15 acceptance design:** Preserve the existing default Compose
  services; provide an explicit local demo profile for API, PostgreSQL, worker
  and scheduler; bind application/database ports to loopback; document a full
  local submission-to-terminal-result path and teardown; prove immediate and
  scheduled execution work without GCP credentials or a cloud account. The
  optional Pub/Sub emulator remains local-only.
- **Milestone 15 validation:** Default Compose still selects only `db` and `api`;
  the `demo` profile adds worker and scheduler with all published ports bound to
  `127.0.0.1`. In an isolated local Compose project, migrations reached 0009,
  readiness passed, an immediate calculation completed with result 6, and a
  future scheduled job stayed SCHEDULED until due then completed with result 9.
  No images were pulled, no cloud services were used, and the isolated containers,
  network and volume were removed. Ruff and strict mypy pass; all 849 tests pass
  without skips; `alembic check` reports no drift.
- **Next objective:** Await approval before starting milestone 16. Continue to
  exclude GCP provisioning, hosted CI and paid services.
- **Milestone 14 design:** Build one multi-stage Python 3.12 slim image, install
  only the project's runtime dependencies, and run directly as the existing
  unprivileged UID. Keep the API as the image default and use its existing module
  CLIs for worker and scheduler roles. Compose exposes worker/scheduler only under
  an opt-in profile so default local startup remains unchanged. Keep migrations an
  explicit one-off command, use `/ready` for the API container healthcheck, and
  give long-running worker/scheduler containers a stop grace period that covers
  their configured shutdown maximum.
- **Milestone 14 acceptance:** Build one image; verify it runs as UID 10001, has no
  development/test dependencies or copied secrets, and starts the API, worker and
  scheduler roles. Verify API `/health` and database-backed `/ready`, role
  shutdown on SIGTERM within the configured grace period, and a separate explicit
  migration command. Keep Compose's default API/database behavior unchanged.
- **Session boundary:** User approved milestone 15 as local-only demo hardening
  on 2026-10-09; GCP provisioning, hosted CI and paid services remain prohibited.
- **Git workflow:** Completed milestones 0-5 form one reviewed local checkpoint.
  Each subsequent milestone gets its own local commit after validation/documentation
  and before the continuation checkpoint, as required by AGENTS.md.
- **Updated:** 2026-10-09.

This document is the implementation source of truth. PROJECT_SPEC defines the
product; DECISIONS records significant choices. A milestone becomes COMPLETE only
after its acceptance criteria and validation pass. Stop progression when they do
not; update this document instead of advertising unfinished behavior.

# Architecture snapshot

The supported runtime is FastAPI, PostgreSQL 17, optional bounded local polling
workers, a Pub/Sub dispatcher CLI and a separate HTTP push worker. API/worker
resources belong to lifespan; async SQLAlchemy sessions are
operation-scoped. Explicit Alembic commands manage the schema; revision 0004 adds
the transactional dispatch outbox and backfills existing active jobs; revision
0005 adds optional unique submission keys and paired request fingerprints; revision
0006 adds retry due times and attempt history; revision 0007 adds generation fencing
and final dead-letter context; revision 0008 adds original scheduled dates and
indexed SCHEDULED admission intent. The scheduler CLI admits bounded due batches
using PostgreSQL locks, committing PENDING and a fresh outbox atomically. Revision
0009 adds immutable priority and internal readiness with UTC rank indexes. Scheduler,
polling and publication give each priority level five seconds of preference;
eligibility-based aging limits fresh urgent arrivals' head start to 45 seconds.
ID delivery stays specific and due/ownership gates remain authoritative. The API runs
on Windows or in a non-root Python 3.12 container. GCP has no role in local startup.
Milestone 12 emits allowlisted, one-line JSON lifecycle events correlated by job ID.
The API and push service expose process-local Prometheus text at `/metrics`, with
bounded service/outcome/route/status labels, counters, an active execution gauge and
request/handler histograms. Worker recovery events have no fabricated handler duration.
The CLI worker/dispatcher/scheduler metrics remain in-process and are not remotely
scrapeable; fleet aggregation and cloud collection are deployment work.

The publisher boundary and ID-specific execution path are exercised by real
PostgreSQL tests and live Google SDK/emulator HTTP delivery smokes on Windows and
Linux. Pub/Sub settings and SDK imports stay in transport/runtime modules.
`LocalQueue` remains an in-process test adapter, not a shared CLI broker.
POST /jobs commits immediate job/outbox or scheduled admission intent with HTTP 201,
or returns the current
job with 200 on matching keyed replay and 409 on conflicting content. Key metadata
is internal; database uniqueness coordinates independent API replicas.
Each claim inserts a history record. Transient outcomes with budget remaining
commit RETRY_WAIT and a new delayed outbox; polling and delivery only claim due
retries. Push ACKs committed retry intent and early duplicates, but due contention
remains 503. GET /jobs/{id}/attempts exposes paginated generation/attempt history
without ownership tokens. Exhausted transient errors become DEAD_LETTERED with final
context; permanent failures remain FAILED. Explicit manual recovery compares the
current generation and atomically opens a new budget with immediate dispatch,
preserving immutable submission identity and every earlier attempt/obligation.

```mermaid
flowchart LR
    Client --> API[FastAPI jobs, probes, OpenAPI]
    API -->|Commit immediate dispatch or scheduled intent| DB[(PostgreSQL 17)]
    Scheduler[Scheduler CLI] -->|Admit due jobs and commit outbox| DB
    Worker[Bounded concurrent local workers] -->|Claim and persist outcome| DB
    Worker --> Handlers[Bounded deterministic handlers]
    Dispatcher[Pub/Sub dispatcher CLI] -->|Lease and finalize outbox| DB
    Dispatcher -->|Publish job ID| Queue[Pub/Sub or local emulator]
    Queue --> Delivery[Dedicated HTTP push worker]
    Delivery -->|Claim by ID and persist outcome| DB
    Delivery --> Handlers
    Migrations[Explicit Alembic command] --> DB
```

# Milestones

## 0 - Planning and engineering foundation

- **Status:** COMPLETE.
- **Objective:** Establish an understandable, verifiable starting point.
- **Tasks:** Create AGENTS, PROJECT_SPEC, PLAN, DECISIONS and README; define the
  src package layout, pyproject, development dependencies, ignores and one portable
  verification script. Inspect the roadmap for unnecessary complexity.
- **Design questions:** Keep tooling usable on Windows and Linux without requiring
  Make, a cloud account or a new dependency manager.
- **Acceptance:** Editable installation succeeds on Python 3.12+; package imports;
  `python scripts/check.py` runs Ruff, strict application mypy and pytest successfully;
  docs distinguish implemented functionality from planned work.
- **Validation:** Install into a clean project venv; run the package smoke test and
  standard command; inspect docs and Git diff for consistency.

## 1 - FastAPI and PostgreSQL foundation

- **Status:** COMPLETE.
- **Objective:** Prove a locally runnable service and migration path.
- **Tasks:** Add typed environment configuration, app factory and lifespan, /health,
  /ready, SQLAlchemy async engine/session setup, Alembic baseline, Docker Compose
  PostgreSQL and API, environment example, database integration fixtures and setup docs.
- **Design questions:** Liveness stays independent of PostgreSQL; readiness must
  actually query it and fail within a bounded timeout. Migrations run explicitly,
  independently of API replicas. Do not invent domain tables yet.
- **Acceptance:** API and PostgreSQL start locally; /health returns 200; /ready returns
  200 with the database and 503 without it; engine is disposed at shutdown; baseline
  migrations upgrade, downgrade and re-upgrade; standard checks pass.
- **Validation:** Unit tests for configuration and probes/lifecycle; real PostgreSQL
  connectivity and migration tests; Compose configuration/build/start and HTTP smoke.

## 2 - Job domain and persistence

- **Status:** COMPLETE.
- **Objective:** Submit and inspect jobs without dispatching them.
- **Tasks:** Define a justified lifecycle, Job model, UTC timestamps, JSON payload,
  constraints/indexes and migration; input/output schemas and focused data access;
  POST /jobs, GET /jobs/{id}, and bounded/paginated GET /jobs.
- **Design questions:** Valid types/payload limits, immutable submission data, legal
  transitions, list ordering/pagination, error contract, and future key uniqueness.
- **Acceptance:** Valid submissions persist PENDING jobs; invalid input is rejected;
  missing IDs return 404; listing is bounded and stable; no execution or queue side
  effect occurs; migration and standard checks pass.
- **Validation:** Domain validation tests, real PostgreSQL API CRUD/constraint tests,
  migration round trip and OpenAPI examples.

## 3 - Local execution path

- **Status:** COMPLETE.
- **Objective:** Prove selection -> atomic claim -> execution -> persisted outcome.
- **Tasks:** Add a local worker entry point, small deterministic handlers (calculation,
  bounded sleep, controlled failure), claim transactions and explicit transitions.
  Define stale ownership/crash behavior before claiming reliability.
- **Design questions:** Row locking/CAS, transaction boundaries, ownership tokens,
  execution outside transactions, and time limits.
- **Acceptance:** Claimed jobs succeed or record a classified failure; contenders
  cannot simultaneously own one job; execution does not hold a long DB transaction.
- **Validation:** Success, exception, contention and terminal-transition integration
  tests against PostgreSQL; local end-to-end submission/execution smoke.

## 4 - Concurrent workers

- **Status:** COMPLETE.
- **Objective:** Bound concurrent execution and stop safely.
- **Tasks:** Configure asyncio concurrency, isolate per-task sessions, handle signals,
  drain/cancel deliberately, and implement the chosen stale-claim recovery semantics.
- **Design questions:** I/O vs CPU workloads; pool sizing; lease duration and fencing;
  cancellation and external effects that cannot be rolled back.
- **Acceptance:** Multiple workers handle jobs without double ownership; active work
  stays within the limit; shutdown/recovery behavior is documented and demonstrated.
- **Validation:** Barrier-based contention tests, multiple-process workers, bounded
  concurrency, cancellation and shutdown/restart scenarios.

## 5 - Queue boundary

- **Status:** COMPLETE.
- **Objective:** Separate transport from execution using a small real-use interface.
- **Tasks:** Extract publish/delivery adapters and add a local/test implementation;
  use job IDs in messages; design and implement durable dispatch reconciliation
  (transactional outbox or justified alternative) before cloud publishing.
- **Design questions:** A database commit and publish are not one atomic transaction;
  distinguish broker acknowledgement from job success; avoid generic queue frameworks.
- **Acceptance:** Existing execution tests remain valid; publication interruptions
  cannot silently strand committed jobs; duplicate dispatch is safe at the claim layer.
- **Validation:** Adapter/backpressure tests; real PostgreSQL atomicity, publication
  failure/backoff/replay, concurrent dispatch, duplicate claims and lock-wait expiry
  tests; migration backfill/round trip and live host/container delivery smokes.

## 6 - Google Pub/Sub integration

- **Status:** COMPLETE (local SDK/emulator integration; cloud deployment deferred).
- **Objective:** Deliver persisted job IDs through a Cloud Run-compatible push endpoint.
- **Tasks:** Add publisher and validated push envelope adapter, HTTP acknowledgement
  behavior, local/emulator tests where supported, and a bounded task execution policy.
- **Design questions:** Push authentication, duplicate/out-of-order delivery, message
  size, request deadlines, malformed messages and transport outages.
- **Acceptance:** The real Python Pub/Sub SDK publishes canonical UUID bytes through
  the existing outbox. A wrapped push ID loads authoritative DB state, claims,
  executes and persists an outcome before HTTP 204. Terminal duplicates return 204;
  live/contended ownership, full capacity, request timeout and dependency failures
  return 503. Unknown IDs return 404; malformed/oversized envelopes return 400/413.
  Non-success responses request broker redelivery. Targeted expired ownership becomes
  FAILED/worker_lost before acknowledgement, without re-execution or unrelated recovery.
  No domain GCP imports or new database schema. The public API remains a separate runtime.
- **Validation:** Standard checks with no database skips; publisher/envelope/config
  tests; real PostgreSQL push success/failure, concurrent duplicates, ownership expiry,
  claim/completion failures, lock contention, bounded admission and cancellation tests;
  migration round trip and schema drift; real SDK -> emulator -> live HTTP worker ->
  PostgreSQL smoke on Windows and Linux, including replay and broker outage recovery.
  Cloud smoke is conditional on an authorized cloud environment, absent in this session.

## 7 - Submission idempotency and duplicate delivery

- **Status:** COMPLETE.
- **Objective:** Guarantee one logical submission per key under concurrency.
- **Tasks:** Idempotency-Key, canonical request fingerprint, PostgreSQL uniqueness,
  matching replay and conflicting-key response; strengthen worker duplicate handling.
- **Design questions:** Key scope, retention, normalization and races; exactly-once
  external effects cannot be inferred from one DB claim.
- **Acceptance:** Optional single `Idempotency-Key` accepts 1-128 ASCII letters,
  digits or `._:-`, with exact case-sensitive global scope and no trimming/expiry.
  No-key submissions remain independent HTTP 201 jobs. A new keyed submission
  returns 201; matching replay returns the same job's current representation with
  200; different validated type/payload/max_attempts returns sanitized 409.
  Fingerprints include defaults, sort nested object keys and preserve array/string
  and parsed numeric representation distinctions (1 differs from 1.0; signed float
  zero differs). Invalid/duplicate headers return 422 without creating rows.
  Concurrent submissions leave exactly one job and one outbox obligation per key;
  rollback or cancellation before committing an incomplete insert leaves the key
  reusable. A lost/ambiguous commit response is resolved by retrying the same key.
  Duplicate transport/polling claims
  cannot add an owned attempt or replace a terminal outcome. Existing rows survive
  migration with NULL idempotency metadata, and public responses do not expose keys/hashes.
- **Validation:** Canonicalization/header/OpenAPI unit tests; real PostgreSQL API
  replay/conflict and independent-app contention, observed unique-index waiting
  behind committed/rolled-back winners, outbox failure atomicity and cancellation;
  duplicate delivery across independent push engines and polling; migration 0005
  upgrade/downgrade/re-upgrade preserving jobs/outbox and direct constraint tests;
  standard checks without skips, schema drift, and live Windows/Linux API + emulator
  replay/duplicate smokes. No cloud provisioning or execution retries in this milestone.

## 8 - Failure and retry semantics

- **Status:** COMPLETE.
- **Objective:** Make one deliberate model control execution attempts.
- **Tasks:** Decide retry authority before implementation; record attempt history,
  classify transient/permanent errors, cap attempts and add backoff with bounded jitter.
- **Design questions:** Application attempts vs broker delivery attempts, durable retry
  scheduling, acknowledgement after failure, leases and retry storms.
- **Acceptance:** PostgreSQL owns execution attempts independently of broker delivery
  and publication counts. Each committed claim creates one RUNNING attempt record;
  completion or expired-owner recovery finalizes that record atomically with job state.
  Retryable errors (controlled failure, timeout, internal error, cancellation and
  worker loss) schedule RETRY_WAIT only while attempt_count < max_attempts; otherwise
  FAILED is terminal. Invalid payload/unknown task are permanent and never retried.
  A scheduled retry commits next_attempt_at and a new outbox obligation with that
  same due time in the outcome transaction. Polling and ID claims cannot execute
  before the database due time. Duplicates cannot move due times, add history,
  consume attempts early or overwrite a newer attempt. Old publisher finalization
  cannot bypass due gating. Equal jitter stays within half/full capped exponential
  delay, chosen once and persisted; restart does not recompute it.
  Push returns 204 after committed terminal state or durable RETRY_WAIT, including
  early retry duplicates; active ownership/contention, lost completion, timeout and
  database failures remain 503. Stale tokens stay fenced across retries/recovery.
  GET /jobs/{id}/attempts returns ordered bounded history without ownership tokens;
  job responses include next_attempt_at. Migration preserves existing jobs/outbox,
  leaves historical terminal jobs terminal and backfills only known active ownership.
  No manual retry, dead-letter policy, general delayed submissions or cloud resources.
- **Validation:** Pure backoff/config/classification tests; real PostgreSQL transient
  success, exhaustion/permanent failures, due gating, replicas/polling/push races,
  outcome/outbox/history rollback, restart, crash/lease recovery and stale finalization;
  migration upgrade/downgrade/re-upgrade and constraints; standard venv checks without
  skips, host/container schema drift and live Windows/Linux emulator retry smokes.

## 9 - Dead-letter handling

- **Status:** COMPLETE.
- **Objective:** Make exhausted jobs inspectable and intentionally recoverable.
- **Tasks:** Define application vs broker dead-letter responsibilities; persist final
  failure context; add POST /jobs/{id}/retry with explicit eligible states.
- **Design:** PostgreSQL remains authoritative. A retryable error at the execution
  budget limit becomes DEAD_LETTERED with reason attempts_exhausted, database-clock
  dead_lettered_at and the final safe error/attempt record. Permanent errors remain
  FAILED. Broker dead-letter forwarding quarantines undeliverable transport messages;
  it never determines execution counts or changes job state. Production policy/IAM
  provisioning is deferred indefinitely and excluded from the local demo.
- **Acceptance:** POST /jobs/{id}/retry accepts a strict body containing
  expected_generation (integer 0..2147483647). Only FAILED/DEAD_LETTERED jobs with
  that current generation may reopen. Under one row lock, increment generation,
  reset attempt_count, current outcome/timing/ownership/dead-letter fields and insert
  an immediate outbox obligation in one transaction; preserve type, payload,
  max_attempts, creation time, submission key/fingerprint and all earlier history.
  Return 202; unknown IDs return 404, stale/ineligible/overflow requests 409,
  invalid bodies 422 and unavailable database 503. Concurrent commands have one
  winner; repeating an old command cannot reopen a later terminal generation.
  Generations begin at zero and have separate 1..20 attempt budgets. Claims and
  dispatch records carry generation; stale finalization cannot alter a reopened
  job, and old READY obligations are skipped. ID-only transport may claim the
  current eligible generation because the database owns execution authority.
  GET /jobs/{id}/attempts returns chronological generation/attempt history with
  bounded limit/offset and optional generation filter; ownership tokens stay private.
  Terminal duplicate deliveries ACK without mutation. Migration 0007 preserves
  historical statuses/context as generation zero; unknown legacy dead-letter
  metadata stays NULL. Downgrade refuses after any manual recovery generation exists
  rather than deleting retained history or silently reverting reopened work.
- **Validation:** Standard venv checks with no skips; real PostgreSQL exhaustion,
  permanent failure/worker loss, terminal duplicate delivery, manual success and
  repeated failure, independent-engine concurrent retry, stale CAS commands,
  transaction rollback/cancellation, keyed replay, history pagination, old outbox
  publication and stale-owner fencing. Migration upgrade/downgrade/re-upgrade,
  downgrade guard including concurrent recovery and direct constraints;
  host/container schema drift; Windows/Linux
  live API -> SDK -> emulator -> push dead-letter/manual-recovery smokes.

## 10 - Delayed jobs and scheduler

- **Status:** COMPLETE.
- **Objective:** Dispatch only due jobs with restart-safe scheduling.
- **Tasks:** scheduled_at support, due-row index/query, bounded scheduler batches and
  safe claims/reconciliation; introduce a scheduler runtime entry point.
- **Design:** Optional scheduled_at is an aware datetime normalized to UTC. HTTP
  input uses RFC 3339 with explicit offset/Z and at most six fractional digits;
  naive/date-only/numeric/invalid/leap-second/UTC-overflow input is rejected. Past dates are
  allowed. All non-NULL scheduled submissions enter SCHEDULED with no outbox;
  the row itself is durable admission intent. Omitted/NULL dates keep immediate
  PENDING + outbox behavior. No recurring schedules or schedule-edit API.
- **Acceptance:** Scheduler schedule_due(database, limit=100) uses PostgreSQL clock,
  ordered scheduled_at/id selection, a bounded 1..1000 batch and FOR UPDATE SKIP
  LOCKED. In one transaction it changes due SCHEDULED rows to PENDING and inserts
  exactly one current-generation outbox per admitted job. Rollback/cancellation
  preserves scheduling intent; competing/restarted schedulers cannot double admit.
  SCHEDULED never executes; initial-generation PENDING/QUEUED claims also guard the
  original database due time. Early SCHEDULED ID deliveries ACK durable admission
  intent even when due; admission commit supplies new dispatch, rollback retains
  SCHEDULED. Active/due execution contention still NACKs. Automatic retries retain
  next_attempt_at authority; manual recovery retains the original scheduled_at and
  opens an immediate new generation. Public job reads expose the original UTC date.
  Fingerprints omit NULL/absent scheduled_at, preserving all D011 hashes. Non-NULL
  dates hash a normalized UTC ISO string with six fractional digits; equivalent
  instants replay, different dates conflict, and the original digest remains immutable.
  Scheduler CLI supports --once (one batch) or polling, portable stop/drain/cancel,
  database outage retry and engine cleanup without broker imports. Settings bound
  batch size, poll interval and shutdown timeout. Partial due index and migration
  0008 preserve legacy rows; downgrade locks before checking and refuses while any
  non-NULL scheduled_at remains, including completed jobs, protecting schedule/key
  semantics. Hosted scheduler invocation/CPU policy is out of scope.
- **Validation:** Standard venv checks without skips; strict datetime/hash/config
  and runtime tests; real PostgreSQL future/due gating, bounded ordered admission,
  independent scheduler/worker/push races, restart, atomic rollback/cancellation,
  keyed replay across upgrades, retry/manual recovery and stale generation fencing.
  Migration round trips/direct constraints/concurrent downgrade guard, host/container
  schema drift, and Windows/Linux live API -> scheduler CLI -> dispatcher -> SDK ->
  emulator -> push smokes, including early ID ACK and restart before due admission.

## 11 - Priority scheduling

- **Status:** COMPLETE.
- **Objective:** Implement an honest, limited scheduling priority policy.
- **Tasks:** Evaluate DB dispatch ordering vs broker realities; define ties and starvation
  policy, then add the minimum priority fields/indexes and ordering.
- **Design:** Immutable strict integer priority 0..9, default 0; higher values get
  five seconds of head start per level, at most 45 seconds. Among due, unlocked
  candidates order by (eligibility in UTC minus priority * five seconds), then
  eligibility timestamp and UUID. This static rank equals linear aging without
  timers, mutable scores or process-local counters. Continually eligible unlocked
  work older than the maximum head start ranks ahead of fresh urgent arrivals;
  capacity, a progressing selector and the database clock are required. No finite
  wall-time, global execution-order, preemption or broker-priority guarantee.
- **Acceptance:** Scheduler admission and local polling use internal jobs.ready_at.
  Submission initializes it to greatest(database creation time, scheduled_at), or
  creation time if immediate; past dates cannot invent age. Admission/publication
  retain it. Automatic retry resets it to next_attempt_at; manual recovery resets
  it to current PostgreSQL time while retaining original priority/date/hash/history.
  Existing status, due-time, attempt budget and ownership predicates stay authoritative;
  aging never makes a future job/retry eligible. ID-specific push/claims execute
  their delivered eligible ID regardless of other jobs' priority.
  Dispatch joins immutable Job.priority without locking jobs and uses available_at
  for READY, lease_expires_at for reclaimable PUBLISHING; FOR UPDATE OF dispatch_outbox
  SKIP LOCKED preserves outbox-before-job lock order. Publication backoff/reclaimed
  leases start a new dispatch eligibility episode. No priority is copied into broker
  messages or outbox rows. Equal ranks use eligibility/UUID, and locked rows may reorder.
  Public input/reads expose priority; ready_at remains internal and caller-controlled
  fields remain rejected. Default/omitted priority is excluded from hashing, preserving
  every D011/D014 digest; nonzero priority participates in immutable submission identity.
  Migration 0009 adds default-zero priority/range constraint, internal readiness and
  partial UTC rank indexes for scheduled/available jobs; retain existing due/lease
  indexes. Backfill readiness from retry due times, current-generation recovery outbox
  creation time, or greatest(original creation, schedule). Preserve existing records,
  original fingerprints, history and obligations. Default-only downgrade roundtrips;
  lock all three tables before guarding and refuse any nonzero priority, including
  terminal jobs, to protect original submission identity. Coordinate stopped runtimes.
- **Validation:** Standard venv checks without skips; strict input/OpenAPI/hash tests
  pinned to actual prior immediate/scheduled digests; real PostgreSQL mixed priority,
  rank/timestamp/UUID ties, sustained urgent arrivals with aged-low progress, locked
  rows, independent selector contention, future/retry gating, lease reclamation,
  targeted low-ID execution and retry/manual age reset. Verify ordering independent
  of session timezone and new indexes against PostgreSQL. Migration roundtrips,
  backfill/default/range constraints, concurrent downgrade guard and schema drift;
  Windows/Linux API -> scheduler/worker/dispatcher CLI -> SDK/emulator/push smokes
  demonstrate database selection, without asserting broker arrival/completion order.

## 12 - Observability

- **Status:** COMPLETE.
- **Objective:** Explain job outcomes and latency with useful evidence.
- **Tasks:** Emit allowlisted one-line JSON lifecycle events correlated by job ID;
  provide process-local Prometheus exposition for HTTP services; count submissions,
  dispatch outcomes, attempts, retries and dead letters; track active executions;
  histogram HTTP and handler-attempt latency with bounded labels.
- **Design:** D016 defines event fields, redaction, metric names/semantics,
  histogram buckets and process-local scope. Cloud aggregation/export remains
  deployment work; the API and push worker expose `/metrics`.
- **Acceptance:** Tests prove lifecycle events carry job ID/service/type/attempt/
  status/duration/error-code context without payload, secret, token or exception
  leakage; a created job can be correlated across submission, dispatch and attempt
  events. Metric counters/gauges/histograms reconcile against controlled events,
  labels remain bounded, and `/metrics` returns valid Prometheus text. Document
  throughput as counter rate, error/dead-letter ratios by completed attempts, and
  p50/p95/p99 as bucket-based estimates for request and handler-attempt duration.
- **Validation:** Observability unit tests cover JSON allowlisting, metric
  counter/gauge/histogram reconciliation, bounded labels and API/push `/metrics`.
  Real PostgreSQL tests correlate a created job across API, dispatcher and worker
  logs and verify expired-owner recovery counters without inventing duration data.
  `python scripts/check.py` passes Ruff, strict mypy for 22 source files and all
  849 tests without skips (136.20 seconds). `alembic current` is 0009 and
  `alembic check` reports no drift. The Windows live Pub/Sub SDK/emulator/push smoke
  passes, including retry, dead-letter, recovery, scheduling and priority scenarios.
  PostgreSQL 17 and the Pub/Sub emulator remained healthy.

## 13 - Integration and failure testing

- **Status:** COMPLETE.
- **Objective:** Validate system invariants across realistic interruptions.
- **Tasks:** Audit existing PostgreSQL/broker fixtures and coverage before changing
  them; consolidate only duplicated setup. Retain critical cases for duplicates,
  handler/publisher exceptions, transient retries/exhaustion, lock contention,
  worker loss, delayed admission, competing schedulers and shutdown.
- **Design:** Use PostgreSQL for transaction/locking semantics, disposable isolated
  schemas, deterministic barriers/fault injection and the existing live Pub/Sub
  emulator smoke. Do not substitute SQLite, public network dependencies or mocks
  for persistence/concurrency evidence.
- **Acceptance:** Every listed scenario must be covered by a repeatable test or
  emulator smoke that checks persisted state and permitted side effects. Integration
  tests must execute against PostgreSQL without skips; failures must be reproducible
  with focused selectors and known cloud/at-least-once limitations documented.
  Standard lint/type/test checks and the live broker smoke pass.
- **Validation:** Inventory coverage, add regression tests only for evidenced gaps,
  run focused PostgreSQL and emulator tests, then `python scripts/check.py` and
  inspect final staged diff.

## 14 - Production containerization

- **Status:** COMPLETE.
- **Objective:** Harden the existing development image for deployable runtimes.
- **Tasks:** One reusable image with explicit API/worker/scheduler commands, non-root
  runtime, minimal dependencies, signal handling and appropriate probes. Preserve
  the current default `db` and `api` Compose services; add opt-in worker/scheduler
  roles for local runtime validation.
- **Design:** Python 3.12 slim runtime in a multi-stage build; do not add a dependency
  manager or lockfile solely for the image. Install the package's production
  dependencies from `pyproject.toml`, and rely on the built OCI image as the
  deployment artifact. Run the process directly as UID 10001 so Docker signals
  reach Uvicorn/async role CLIs. API liveness is `/health`, readiness is `/ready`;
  worker and scheduler remain non-HTTP processes. Apply migrations only through an
  explicit one-off command, never at replica startup.
- **Acceptance:** One image runs API, worker and scheduler as UID 10001, contains no
  development/test packages or source-tree secrets, reports healthy API liveness
  and database readiness, and exits cleanly on SIGTERM within its configured
  Compose grace period. Migration remains an explicit one-off operation, and the
  default Compose startup remains `db` plus `api`.
- **Validation:** Default and opt-in Compose configurations validate; the
  multi-stage image builds and runtime inspection confirms UID 10001, no dev/test
  packages and no copied secrets/test files. In an isolated Compose project,
  migration completed to 0009, API `/health` and `/ready` passed, and API, worker
  and scheduler all stopped with exit code 0 on SIGTERM. Standard checks passed:
  Ruff, strict mypy and 849 tests without skips.

## 15 - Free local demo hardening

- **Status:** COMPLETE (cloud deployment deferred indefinitely).
- **Objective:** Make the learning project easy to demonstrate locally without
  paid services, cloud accounts, hosted CI, or provisioned infrastructure.
- **Design:** Keep PostgreSQL authoritative and use local Docker Compose services.
  Add an opt-in `demo` profile to start API, worker and scheduler while preserving
  the existing default `db`/`api` behavior. Keep exposed ports on loopback, run
  migrations explicitly, and keep Pub/Sub emulator delivery optional. Cloud
  adapters remain source-level architecture only and are not part of the demo path.
- **Acceptance:** Compose's default profile remains unchanged; the `demo` profile
  starts local API/database/worker/scheduler. From the documented flow, an
  immediate calculation reaches `SUCCEEDED` with its result, and a future
  scheduled job remains scheduled until due and is then admitted/executed. No GCP
  account, credentials, cloud resource, paid API or hosted runner is needed.
  Document safe shutdown and persistence cleanup. Validate both Compose
  configurations and run `python scripts/check.py`; no cloud command is permitted.
- **Validation:** Inspect resolved Compose profiles/ports and run the demo
  end-to-end against isolated local resources. Run the standard quality command.
  Local image/package downloads may use the network but must not contact or create
  paid cloud resources. Do not claim cloud behavior was validated.
- **Result:** The default and `demo` Compose profiles were checked; loopback
  binding, migration, readiness, immediate execution and scheduled execution all
  passed in an isolated local project. The standard check passed Ruff, strict
  mypy and 849 tests without skips; schema drift check passed. No cloud action
  occurred.

## 16 - Local verification automation

- **Status:** NOT STARTED (hosted runners and deployment automation deferred).
- **Objective:** Preserve reproducible, cost-free local verification.
- **Tasks:** Keep `python scripts/check.py` as the local quality gate. Do not add
  hosted workflows or image publication/deployment that can consume paid quotas
  unless the user separately authorizes the resulting cost/risk.
- **Design questions:** Keep verification local and avoid workflows or tools that
  transfer data to hosted/billable services. Do not add deployment automation.
- **Acceptance:** Local verification remains reproducible with no hosted service.
- **Validation:** Run local quality commands; no hosted workflow or cloud deployment
  is required or authorized.

## 17 - Load tests and benchmarks

- **Status:** NOT STARTED.
- **Objective:** Measure scaling and identify the real bottleneck.
- **Tasks:** Select a justified load tool; define deterministic workloads; compare
  concurrency 1/5/10/20; record raw output and environment/configuration methodology.
- **Design questions:** Separate queue/execution/end-to-end latency, warmup, saturation,
  DB/broker limits, fairness and CPU vs I/O workloads.
- **Acceptance:** Reproducible jobs/s, p50/p95/p99, failure rate and resource measurements;
  honest interpretation explains nonlinear scaling.
- **Validation:** Repeat local benchmark runs with bounded duration and documented
  resource usage and variance; no cloud or paid load-testing service.

## 18 - Reliability experiment

- **Status:** NOT STARTED.
- **Objective:** Quantify recovery under controlled transient failures.
- **Tasks:** Reproducible approximately 10% fault injection; measure initial/eventual
  completion, attempts per job, dead-letter rate and time to success; publish methodology.
- **Design questions:** Seeded fault schedules, independent error categories, incomplete
  observation windows and consistency with the configured retry budget.
- **Acceptance:** Raw results reconcile with DB attempt records; conclusions distinguish
  guarantees from experimental outcomes.
- **Validation:** Controlled runs and accounting invariants using the failure suite.

## 19 - Optional C++ worker experiment

- **Status:** NOT STARTED (OPTIONAL; gated on 0-18 completion).
- **Objective:** Compare equivalent CPU-bound work only if it adds evidence.
- **Tasks:** Choose one hashing/prime/transformation workload and equivalent implementations;
  record environment, compiler/runtime options, correctness and raw benchmark results.
- **Design questions:** IPC/transport overhead and fair inputs/resources; skip entirely if
  it distracts from the primary project.
- **Acceptance:** Correctness-equivalent CPU workloads and honest measurements; no claims
  based on sleep or network-bound work.
- **Validation:** Cross-language result comparisons and repeated controlled benchmarks.

# Recent milestone details

Milestone 11 implements D015: immutable strict priority 0..9, default 0, with a
fixed five-second head start per level. Database selectors rank eligible UTC time
minus priority * five seconds, then eligibility timestamp and UUID. Continually
eligible unlocked work older than the maximum 45-second head start ranks ahead
of fresh urgent arrivals; capacity, advancing database time and selector progress
are required. This is a limited selection policy, without global execution order,
preemption or a finite wall-time service guarantee.

Internal Job.ready_at begins at greatest(database creation, original schedule).
Initial past dates invent no age. Admission/publication retain readiness; automatic
retry resets it to next_attempt_at and manual recovery resets it to current database
time. Priority/date/hash/history persist across generations. Default-priority
selection now orders readiness episodes rather than historical creation or requested
past dates. Listing remains creation-ordered, and ID-specific execution remains
independent of other jobs' priorities. Due/status/budget/ownership gates are unchanged.

Dispatch joins immutable Job.priority, ranking READY by available_at and expired
PUBLISHING by lease_expires_at. Only outbox rows are locked, preserving existing
finalization lock order. Broker messages and outbox schema stay unchanged; joined
sorting cost remains benchmark work. Two partial UTC rank indexes support job
selection while existing due/lease indexes remain.

Effective priority 0 is omitted from fingerprints, preserving every retained
D011/D014 digest. Nonzero priorities contribute immutable submission content.
Migration 0009 backfills known retry/recovery/initial readiness, without changing
existing records or identity. Locked downgrade refuses any nonzero priority,
including completed jobs; default-only records can return to 0008. No new dependency,
cloud resource, recurring schedule or edit API is introduced.

Validation completed on 2026-10-09:

- Standard venv `python scripts/check.py`: Ruff passes, strict mypy passes for 21
  source files and all 842 tests pass without skips in 124.50 seconds. Includes
  25 new priority unit cases and 40 real PostgreSQL priority cases: mixed levels,
  exact 44/45/46-second boundaries, deterministic readiness/UUID ties, 12 selector
  passes with 36 fresh urgent arrivals and aged-low progress, locked-row skipping,
  independent ownership, due/lease gating, targeted low-ID execution, timezone
  independence, retry/manual age resets and pinned pre-upgrade submission replays.
- All 15 PostgreSQL priority migration tests pass legacy record/hash preservation,
  readiness backfill/default/range/null constraints, repeated round trips, usable
  UTC rank indexes across UTC/Warsaw/Lord Howe session timezones, nondefault terminal
  downgrade refusal and concurrent submission fencing. Revision 0008's original
  schedule guard remains intact. Application is at 0009; host and rebuilt container
  schema drift checks report no new operations.
- Windows/Python 3.13.1 and Linux/Python 3.12.15 live API -> scheduler/worker/dispatcher
  CLI -> SDK/emulator -> push smokes pass mixed priority publication, equal-rank UUID ties, targeted
  low-ID execution and aged-low progress during fresh urgent arrival passes.
  Explicit priority-0 replay retains original keyed identity; all prior probes pass
  and disposable resources clean up. Compose configuration and host/container pip
  checks pass; the non-root Linux image builds and runs the same policy.

Cloud scheduler invocation/CPU policy and broker DLQ/IAM are deferred indefinitely.
Local polling and emulator results make no cloud hosting or IAM claim.

The roadmap is intentionally sequential. Basic atomic claiming is required before
concurrency, and durable publication/reconciliation is required before Pub/Sub.
API idempotency at milestone 7 does not defer safe worker ownership from milestone 3.
Failure/concurrency tests begin with the relevant implementation; milestone 13 extends
them and does not postpone correctness testing.

## Milestone 12 observability

D016 is implemented without new runtime dependencies or schema changes. API
submission/replay/manual-retry, scheduler admission, dispatcher finalization,
execution outcomes and expired-owner recovery emit one-line JSON events with
canonical job UUID correlation and allowlisted context. Payloads, headers,
idempotency keys, database URLs, ownership tokens and arbitrary exception text
are excluded.

The API and push worker expose `/metrics`; counters distinguish created/replayed
submissions, accepted/failed dispatch finalizations, committed attempt outcomes,
retry/dead-letter transitions and HTTP requests. `active_executions` is a
process-local gauge. HTTP and handler-attempt histograms use fixed finite buckets;
handler duration excludes claim/outcome persistence, and expired-owner recovery
does not invent a duration. Metric labels are restricted to finite service,
outcome, route-template and HTTP-status vocabularies. Pure ASGI instrumentation
preserves push cancellation cleanup. Process-local CLI metrics are not remotely
scrapeable, and no fleet or cloud exporter is claimed.

Verification on 2026-10-09:

- `python scripts/check.py`: Ruff and strict mypy pass; 849 tests pass without
  skips. The unit suite checks JSON field allowlisting/redaction, bounded labels,
  metric reconciliation and API/push exposition.
- Real PostgreSQL tests correlate API -> dispatcher -> worker events by job ID;
  expired-owner recovery increments committed retry metrics without adding a
  synthetic duration sample. Push cancellation still commits RETRY_WAIT.
- `alembic current` reports 0009; `alembic check` reports no schema drift.
- The Windows Pub/Sub SDK/emulator/push smoke passes its end-to-end retry,
  recovery, scheduling and priority scenarios. No cloud collection was tested.

# Decisions affecting upcoming work

- D001: staged local-first implementation and the specified stack.
- D002: PostgreSQL is authoritative; use explicit transactions and constraints.
- D003: async SQLAlchemy/psycopg with explicit migrations and isolated sessions.
- D004: Pub/Sub push / Cloud Run is provisional; delivery can be duplicated.
- D005: portable tooling and explicit Windows-compatible asyncio loop selection.
- D006: persistence-only job API, strict JSONB inputs and bounded offset pagination.
- D007/D008: database token/lease ownership, bounded loops and graceful draining.
- D009: transactional dispatch outbox, replayable publication and separate attempt budgets.
- D010: separate Pub/Sub runtimes, bounded push requests and ACK after durable terminal state.
- D011: global optional submission keys, original content fingerprints and atomic job/outbox replay.
- D012: application attempt authority, persisted retry timing/history and atomic replacement dispatch.
- D013: explicit dead-letter exhaustion, generation-fenced manual recovery and retained history.
- D014: durable scheduled admission, database-clock eligibility and compatible schedule identity.
- D015: bounded priority preference, eligibility-episode aging and unchanged ID authority.
- D016: redacted lifecycle logs, bounded Prometheus metrics and process-local scope.
- D018: zero-spend boundary; superseded by D019's local-only scope.
- D019: free local demo; GCP provisioning and hosted/billable automation deferred.

# Risks / technical debt

- Milestones 0-14's local acceptance checks pass. Cloud IAM/provisioning/delivery
  are deferred indefinitely; local tests do not validate cloud behavior.
- Cloud IAM, broker DLQ operations and dispatcher/scheduler hosting still need
  deployment/reliability decisions. Priority cannot impose global broker execution order.
- Metrics are process-local; API/push expose scrape endpoints, but CLI-only metrics
  lack remote exposition. Fleet collection/export, retention and alerting remain
  deployment work.
- `LocalQueue` loses buffered messages on process exit. The outbox is durable, but
  marking acceptance requires a transport whose acceptance guarantees fit deployment.
  A dispatcher must be invoked to publish/replay obligations; local tests do not
  establish a durable broker guarantee or a hosted dispatcher.
- Outbox/job retention and cleanup remain future work. Keys have no expiry and
  remain bound while jobs exist; future submission fields/fingerprint changes need
  an explicit compatibility decision for existing key bindings.
- At-least-once delivery does not imply exactly-once external side effects. Keep handlers
  deterministic while ownership and failure semantics are developed.
- History begins at migration 0006; only known active ownership is backfilled.
  A legacy RUNNING record with zero attempts was never produced by application
  claims and requires investigation rather than invented history. Coordinated
  upgrades are required; revision 0006 downgrade stops waiting retries and removes history/timing.
- Milestone 9 introduces retry generations to retain history across budget resets;
  coordinated upgrade is required, and downgrade will be refused after manual recovery.
- Original scheduled_at has no edit/retention API. Migration 0008 downgrade refuses
  while any original date remains, including completed work. PostgreSQL clock defines
  due eligibility; host clock, polling and broker ordering do not define execution time.
- Nonzero priority belongs to retained submission identity; migration 0009 downgrade
  refuses while it exists. The fixed five-second quantum/range and rank indexes
  need coordinated changes. Joined dispatch sorting still needs benchmark evidence.
- Host tests use Python 3.13.1; container runtime smokes use Python 3.12.15. This
  evidence does not establish a full Python-version test matrix.

# Next actions

1. Finish the local-only demo profile, walkthrough and local end-to-end acceptance.
2. Keep GCP provisioning and hosted/billable automation out of scope unless the user
   explicitly changes the zero-cost requirement.
