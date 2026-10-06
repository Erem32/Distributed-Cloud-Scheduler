# Project status

- **Current milestone:** 8 - failure and retry semantics (COMPLETE).
- **Current state:** Milestones 0-8 are validated locally. PostgreSQL owns bounded
  execution retries, persisted due times and attempt history. Retry state/history/
  outbox commit atomically; Windows/Linux live delivery smokes pass.
- **Next objective:** Milestone 9 - dead-letter handling (NOT STARTED).
- **Session boundary:** Milestone 8 is tied off; ask before starting milestone 9.
- **Git workflow:** Completed milestones 0-5 form one reviewed local checkpoint.
  Each subsequent milestone gets its own local commit after validation/documentation
  and before the continuation checkpoint, as required by AGENTS.md.
- **Updated:** 2026-10-06.

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
0006 adds retry due times and attempt history. The API runs
on Windows or in a non-root Python 3.12 container. GCP has no role in local startup.

The publisher boundary and ID-specific execution path are exercised by real
PostgreSQL tests and live Google SDK/emulator HTTP delivery smokes on Windows and
Linux. Pub/Sub settings and SDK imports stay in transport/runtime modules.
`LocalQueue` remains an in-process test adapter, not a shared CLI broker.
POST /jobs commits new job/outbox obligations with HTTP 201, or returns the current
job with 200 on matching keyed replay and 409 on conflicting content. Key metadata
is internal; database uniqueness coordinates independent API replicas.
Each claim inserts a history record. Transient outcomes with budget remaining
commit RETRY_WAIT and a new delayed outbox; polling and delivery only claim due
retries. Push ACKs committed retry intent and early duplicates, but due contention
remains 503. GET /jobs/{id}/attempts exposes ordered history without ownership tokens.

```mermaid
flowchart LR
    Client --> API[FastAPI jobs, probes, OpenAPI]
    API -->|Commit job and outbox together| DB[(PostgreSQL 17)]
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

- **Status:** NOT STARTED.
- **Objective:** Make exhausted jobs inspectable and intentionally recoverable.
- **Tasks:** Define application vs broker dead-letter responsibilities; persist final
  failure context; add POST /jobs/{id}/retry with explicit eligible states.
- **Design questions:** Broker limits are not authoritative application attempt counts;
  manual retry budget reset, ownership and idempotency.
- **Acceptance:** Exhausted/permanent jobs have an understandable terminal state;
  eligible manual retries enqueue safely; invalid retries are rejected.
- **Validation:** Exhaustion, permanent error, duplicate dead-letter delivery and manual retry.

## 10 - Delayed jobs and scheduler

- **Status:** NOT STARTED.
- **Objective:** Dispatch only due jobs with restart-safe scheduling.
- **Tasks:** scheduled_at support, due-row index/query, bounded scheduler batches and
  safe claims/reconciliation; introduce a scheduler runtime entry point.
- **Design questions:** Multiple schedulers, UTC clock assumptions, transaction locks,
  eventual precision and Cloud Run hosting that does not rely on background CPU.
- **Acceptance:** Future jobs stay unexecuted; due jobs eventually dispatch despite
  scheduler restarts; competing schedulers do not lose jobs.
- **Validation:** Controlled-time tests, competing schedulers and crash-window integration tests.

## 11 - Priority scheduling

- **Status:** NOT STARTED.
- **Objective:** Implement an honest, limited scheduling priority policy.
- **Tasks:** Evaluate DB dispatch ordering vs broker realities; define ties and starvation
  policy, then add the minimum priority fields/indexes and ordering.
- **Design questions:** Pub/Sub has no global execution priority guarantee; prefetch
  and already-running tasks limit ordering.
- **Acceptance:** The documented dispatch policy is demonstrated without promising
  strict global execution order.
- **Validation:** Deterministic mixed-priority/tie tests and sustained-load starvation checks.

## 12 - Observability

- **Status:** NOT STARTED.
- **Objective:** Explain job outcomes and latency with useful evidence.
- **Tasks:** Structured logs with job/type/service/attempt/status/duration/error context;
  counters, active execution, retry/dead-letter counts and latency histograms.
- **Design questions:** Cloud-appropriate exporters, label cardinality, payload/secret
  redaction, process-local vs fleet metrics.
- **Acceptance:** A job can be followed across submission, dispatch and attempts;
  p50/p95/p99 and error/throughput measures have defined meanings.
- **Validation:** Logging/redaction tests, metric invariants and sample end-to-end traces.

## 13 - Integration and failure testing

- **Status:** NOT STARTED.
- **Objective:** Validate system invariants across realistic interruptions.
- **Tasks:** Consolidate PostgreSQL/broker test fixtures and critical end-to-end cases:
  duplicates, exceptions, transient failures, exhaustion, contention, worker crash,
  delayed jobs, scheduler duplication and shutdown. Retain regressions discovered earlier.
- **Design questions:** Deterministic synchronization and fault injection; avoid public
  internet dependencies and mocks that conceal transaction behavior.
- **Acceptance:** Critical scenarios assert durable state and allowed effects; known
  limitations are recorded and reproducible.
- **Validation:** Repeatable integration/failure suite plus all standard checks.

## 14 - Production containerization

- **Status:** NOT STARTED.
- **Objective:** Harden the existing development image for deployable runtimes.
- **Tasks:** One reusable image with explicit API/worker/scheduler commands, non-root
  runtime, minimal dependencies, signal handling and appropriate probes.
- **Design questions:** Migration job separate from replicas; CPU/memory limits,
  Python version and image reproducibility.
- **Acceptance:** Built images run the three relevant roles and stop correctly without
  shipping development dependencies or secrets.
- **Validation:** Image build, runtime/probe/shutdown smoke and configuration inspection.

## 15 - Terraform and GCP deployment

- **Status:** NOT STARTED.
- **Objective:** Provision a minimal explainable cloud system after behavior stabilizes.
- **Tasks:** Required APIs, Artifact Registry, Pub/Sub/DLQ as chosen, Cloud SQL, Cloud Run
  roles, scheduler hosting, IAM and secrets; deploy/runbook and teardown instructions.
- **Design questions:** Least privilege, push OIDC, DB connection budget, public demo
  exposure, regionality and cost; ask before spending materially or irreversible changes.
- **Acceptance:** Terraform validates/plans; a reviewed authorized deployment runs
  submission through terminal state; no credentials in source/state artifacts in Git.
- **Validation:** fmt/validate/plan and cloud end-to-end smoke when credentials and cost
  authorization exist. Without them this milestone remains incomplete.

## 16 - GitHub Actions

- **Status:** NOT STARTED.
- **Objective:** Automate equivalent local quality gates, then authorized deployment.
- **Tasks:** PR lint/types/tests/build with PostgreSQL services; reproduce the standard
  check command; add image publication/deployment separately with workload identity.
- **Design questions:** Protect deployment environments, avoid stored service-account
  keys, cache safely and pin CI dependencies. Tooling is established earlier; CI waits here.
- **Acceptance:** PR checks run reproducibly; deployment automation is validated only
  when a cloud environment exists and publication is authorized.
- **Validation:** Local workflow-equivalent commands and actual workflow run results.

## 17 - Load tests and benchmarks

- **Status:** NOT STARTED.
- **Objective:** Measure scaling and identify the real bottleneck.
- **Tasks:** Select a justified load tool; define deterministic workloads; compare
  concurrency 1/5/10/20; record raw output and environment/configuration methodology.
- **Design questions:** Separate queue/execution/end-to-end latency, warmup, saturation,
  DB/broker limits, fairness and CPU vs I/O workloads.
- **Acceptance:** Reproducible jobs/s, p50/p95/p99, failure rate and resource measurements;
  honest interpretation explains nonlinear scaling.
- **Validation:** Repeat benchmark runs with bounded duration/cost and documented variance.

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

# Current milestone details

Milestone 8 implements D012 with a transport-independent RetryPolicy, RETRY_WAIT
and next_attempt_at, job_attempts history and migration 0006. Equal jitter uses
half/full capped exponential delay (defaults 1/60 seconds; settings 0.001-300).
Each claim atomically increments its attempt count and records ownership. Completion
and expired-owner recovery finalize history and commit job state plus a new
outbox obligation available at the persisted due time. Permanent/exhausted errors
become FAILED; old FAILED jobs remain terminal.

Publication leaves RETRY_WAIT intact. Future old deliveries ACK durable intent;
due contention, active ownership and failed completion request redelivery.
Polling and ID-specific claims use the database due time. Completion samples the
database clock after the job lock and again after flushing all outcome writes,
rolling back if a history/outbox lock wait outlived its lease. Retry scheduling
never relies on sleeping in a request or an in-process background timer.

Public jobs expose next_attempt_at; GET /jobs/{id}/attempts returns up to 20
ordered records without claim tokens/payloads. finished_at describes the latest
attempt even during retry wait, and is reset on the next claim. Revision 0006
backfills only known positive-count RUNNING ownership; completed historical
records are not fabricated. Downgrade maps waiting retries to FAILED and removes
timing/history, preserving other job fields and all publication records. No
manual retry, dead-letter forwarding, general scheduling or cloud provisioning
was introduced. No new dependency was required.

Validation completed on 2026-10-06:

- Project venv python scripts/check.py: Ruff passed, strict mypy passed for 18
  source files, and **586 tests passed without skips** on Windows/Python 3.13.1
  with PostgreSQL 17. The resumed baseline passed 490 tests without skips.
- Pure policy/config tests cover exponential growth, positive jitter bounds,
  hard caps, single sampling and invalid timing; existing classification and
  sanitization tests remain valid.
- Real PostgreSQL tests cover transient success, permanent failure and exact
  exhaustion at budgets 1/3/20, ordered history, atomic outcome/dispatch rollback,
  rejected claim-history insertion, persisted early due gating and keyed replay.
- Eight independent-engine polling/delivery contenders admit one due attempt.
  Old publication cannot accelerate a waiting retry. Restarted push workers ACK
  early duplicates without changing dates/history; due locked retries NACK.
- Concurrent expired completion/recoverers finalize once; stale owners cannot
  overwrite a new attempt. A real outbox INSERT lock wait past the lease rolls
  back outcome/history/retry intent. Independent-process crash recovery schedules
  durable retry and a new worker succeeds while preserving attempt accounting.
- Migration 0005 -> 0006 -> 0005 -> 0006 preserves historical jobs/outbox and
  backfills only known active ownership. Legacy positive-count owners complete
  and recover through production functions after upgrade. Waiting-retry downgrade
  and direct schedule/history/PK/FK/cascade constraints pass.
- Application DB migrated to 0006; host and rebuilt container alembic check find
  no drift. Compose configuration and dependency consistency checks pass.
- Final Windows and rebuilt Linux Python 3.12.15 image (UID 10001) live
  API -> dispatcher -> Google SDK -> emulator -> push smokes pass transient
  RETRY_WAIT/ACK, early duplicate/due gating, success on attempt 2, exhaustion at
  max_attempts 2, permanent unknown-task failure and ordered token-free history.
  Earlier idempotency, failure/duplicate ACK, malformed/unknown/oversized envelope
  and broker outage/publication recovery probes pass. Disposable resources clean up.

Cloud IAM/provisioning remain deployment-stage validation. Manual retry and
dead-letter responsibilities are the next milestone's design.

The roadmap is intentionally sequential. Basic atomic claiming is required before
concurrency, and durable publication/reconciliation is required before Pub/Sub.
API idempotency at milestone 7 does not defer safe worker ownership from milestone 3.
Failure/concurrency tests begin with the relevant implementation; milestone 13 extends
them and does not postpone correctness testing.

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

# Risks / technical debt

- Milestones 0-8's local acceptance checks pass. Cloud IAM/provisioning/delivery remain
  deployment-stage validation, not evidence supplied by the emulator.
- No cloud environment or spending authorization has been established. Cloud rollout
  must not be claimed validated from local tests.
- Cloud IAM, dead-letter mapping, dispatcher/scheduler
  hosting and priority semantics still need deployment/reliability decisions.
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
  upgrades are required; downgrade stops waiting retries and removes history/timing.
- Milestone 9 must decide a lifetime attempt ordinal or retry generations before
  manual budget resets; resetting attempt_count would collide with retained history.
- Host tests use Python 3.13.1; container runtime smokes use Python 3.12.15. This
  evidence does not establish a full Python-version test matrix.

# Next actions

1. Ask for authorization to begin milestone 9; do not start it before the user agrees.
2. If authorized, decide application/broker dead-letter responsibilities, manual
   retry eligibility, history generations and budget-reset semantics before implementation.
3. Preserve retry timing, submission identity, publication fencing and attempt
   history through manual recovery races. Cloud provisioning remains milestone 15.
