# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.9.25 AS uv
FROM python:3.11-slim-bookworm AS python-base

FROM python-base AS builder
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1
WORKDIR /build

# Keep third-party dependencies cached when only application code changes.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project
COPY README.md ./
COPY src/ ./src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

FROM python-base AS runtime
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
RUN groupadd --gid 10001 telegram \
    && useradd --uid 10001 --gid telegram --no-create-home telegram \
    && mkdir -p /data/tmp /opt/service \
    && chown -R telegram:telegram /data
COPY --from=builder /opt/venv /opt/venv
COPY bot.py /opt/service/bot.py
COPY --chmod=0555 deploy/docker-entrypoint.sh /usr/local/bin/docker-entrypoint
USER 10001:10001
WORKDIR /data
ENTRYPOINT ["docker-entrypoint"]
CMD ["python", "/data/bot.py"]
