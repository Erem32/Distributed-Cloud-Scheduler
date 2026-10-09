FROM python:3.12-slim-bookworm AS build

ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /build

COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir --no-compile .

FROM python:3.12-slim-bookworm AS runtime

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY --from=build /opt/venv /opt/venv
COPY alembic.ini ./
COPY migrations ./migrations
RUN useradd --create-home --uid 10001 scheduler

USER 10001:10001

EXPOSE 8000
STOPSIGNAL SIGTERM
CMD ["python", "-m", "distributed_cloud_scheduler", "--host", "0.0.0.0", "--port", "8000"]
