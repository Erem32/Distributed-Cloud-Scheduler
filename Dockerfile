FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN python -m pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 scheduler

COPY alembic.ini ./
COPY migrations ./migrations

USER scheduler

EXPOSE 8000
CMD ["python", "-m", "distributed_cloud_scheduler", "--host", "0.0.0.0", "--port", "8000"]
