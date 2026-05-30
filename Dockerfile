# ── Build stage: install deps with uv ────────────────────────
FROM python:3.14-slim-bookworm AS builder

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Cache dep layer before source arrives
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Install project into the same venv
COPY . .
RUN uv sync --frozen --no-dev

# ── Runtime stage ─────────────────────────────────────────────
FROM python:3.14-slim-bookworm

WORKDIR /app

# System binaries first (requires root)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Non-root user
RUN useradd --no-create-home --shell /bin/false fcc

# Copy venv + source with correct ownership
COPY --from=builder --chown=fcc /app /app

USER fcc

# Vercel / Railway / Render inject $PORT; fall back to 8082 locally
EXPOSE 8082

# Shell form so ${PORT:-8082} expands at runtime
CMD uv run uvicorn server:app \
        --host 0.0.0.0 \
        --port ${PORT:-8082} \
        --timeout-graceful-shutdown 5 \
        --no-access-log
