# Single image, many roles: the producer API and all three consumers share the
# same codebase and are differentiated by the `command:` in docker-compose.
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install -r requirements.txt

# Application code.
COPY app ./app

# Run as a non-root user.
RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000

# Default command runs the producer API; consumers override this in compose.
CMD ["uvicorn", "app.producer.main:app", "--host", "0.0.0.0", "--port", "8000"]
