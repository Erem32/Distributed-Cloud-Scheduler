# Development workflow

This is a local-first, milestone-driven distributed backend project. Read
`docs/PROJECT_SPEC.md` and `docs/PLAN.md` before substantial changes.

For every implementation task:

1. Inspect relevant existing code before making changes.
2. Design substantial changes before implementation. Define validation and acceptance
   criteria in the active milestone before writing its behavior.
3. Keep `docs/PLAN.md` current when a milestone starts or finishes, ordering or
   assumptions change, dependencies are added, blockers emerge, or new work is found.
   Update the current state and next actions; do not accumulate a development diary.
4. Keep changes focused on the active milestone. Do not modify unrelated behavior
   without a reason. Prefer simple cohesive modules over generic frameworks.
5. Add or update meaningful tests for new or changed behavior. Use real PostgreSQL
   for database/concurrency semantics; SQLite cannot validate its locking behavior.
6. Document significant architecture decisions and changes in `docs/DECISIONS.md`.
   Do not silently change scope or architecture. Cloud adapters must not couple
   domain behavior to GCP.
7. Before completing implementation, use the project's virtual environment and run
   `python scripts/check.py`. This standard command runs `ruff check .`,
   `mypy src`, and `pytest`. Run the milestone's database, migration, container,
   or end-to-end validation too. A skipped integration test
   does not satisfy an integration acceptance criterion.
8. Fix failures caused by the change. Do not advance to a later milestone while
   the current milestone has unmet acceptance criteria. Record environmental
   blockers with the exact remaining validation in `docs/PLAN.md`.
9. Update README/documentation when setup, architecture, public interfaces,
   configuration, deployment, or externally visible behavior changes. README claims
   must describe validated functionality. Keep progress in PLAN, not PROJECT_SPEC.
10. Never commit secrets or hardcode credentials. Supply dummy examples where useful.
    Do not destroy existing Git history or reset user changes.
11. Do not claim functionality works without validating it. Finish by briefly
    summarizing changes, checks and results, and important decisions or trade-offs.
12. At a milestone boundary, tie off the completed work before asking whether to
    continue: validate its acceptance criteria, review these instructions, update
    PLAN status/current architecture/next actions, update affected README/spec and
    decisions, inspect the final diff, and create the milestone commit required
    below. Then ask the user whether to start the
    next milestone unless the user has already asked to stop for the day. Do not
    start it until the user agrees. This checkpoint is an explicit user instruction;
    an earlier general request to continue the project
    does not bypass it.
13. Commit locally every time a milestone is completed, after its acceptance checks
    pass and affected documentation is current. Stage only the milestone's code,
    tests, migrations, configuration and documentation; review the staged diff and
    exclude secrets and unrelated user changes. Use a meaningful commit message
    naming the milestone and its concrete outcome, and report the commit hash.
    Commit before asking to continue or ending the session. This is standing user
    authorization for milestone commits; no separate confirmation is needed.

Use Python 3.12+, FastAPI, Pydantic, SQLAlchemy 2, Alembic and PostgreSQL. Introduce
Pub/Sub, Cloud Run, Cloud SQL, Terraform and deployment automation only at the
planned stages. Swagger/OpenAPI is the user interface; no frontend or account
system. C++ is optional and follows completion and benchmarking of the Python system.
