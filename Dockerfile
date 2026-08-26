FROM python:3.12-slim AS base

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY scripts/ ./scripts/

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Test/dev image: same deps plus the test toolchain. Not used by `docker compose up`.
FROM base AS dev
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY tests/ ./tests/
COPY pytest.ini .
CMD ["pytest", "-q"]
