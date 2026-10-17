FROM python:3.12-slim AS base

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
# Hash-checked: requirements.txt is a lockfile (see requirements.in).
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

COPY src/ ./src/
COPY scripts/ ./scripts/

# Services run unprivileged. The files stay root-owned and world-readable: nothing
# here needs to write to its own code, and a compromised process cannot rewrite it.
RUN useradd --system --uid 10001 --no-create-home contrail
USER contrail

CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Test/dev image: same deps plus the test toolchain. Not used by `docker compose up`.
FROM base AS dev
# The test image is not an application: it root-installs the toolchain and writes
# reports into bind-mounted host directories, whose ownership will not match a fixed uid.
USER root
COPY requirements-dev.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements-dev.txt
COPY tests/ ./tests/
COPY load/ ./load/
COPY pytest.ini .
CMD ["pytest", "-q"]
