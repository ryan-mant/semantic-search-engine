# ---- Build Stage ----
FROM python:3.14-slim as builder

# 1. Install build dependencies and poetry
RUN apt-get update && apt-get install -y --no-install-recommends build-essential librdkafka-dev
RUN pip install poetry>=2.0.0 poetry-plugin-export

WORKDIR /app

# 2. Copy poetry files
COPY API/pyproject.toml API/poetry.lock ./

# 3. Export production dependencies and dev dependencies
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes
RUN poetry export --with dev -f requirements.txt --output requirements-dev.txt --without-hashes

# 4. Create a virtual environment
RUN python -m venv /opt/venv

# 5. Install production Python packages
RUN /opt/venv/bin/pip install --no-cache-dir -r requirements.txt


# ---- Test/Development Stage ----
FROM python:3.14-slim as development

WORKDIR /app/API

# 1. Install runtime and test/build dependency
RUN apt-get update && apt-get install -y --no-install-recommends build-essential librdkafka-dev librdkafka1 && rm -rf /var/lib/apt/lists/*

# 2. Copy virtual env
COPY --from=builder /opt/venv /opt/venv

# 3. Install dev packages into the virtual env
COPY --from=builder /app/requirements-dev.txt /app/requirements-dev.txt
RUN /opt/venv/bin/pip install --no-cache-dir -r /app/requirements-dev.txt

# 4. Copy the code
COPY API/src /app/src
COPY API /app/API

# 5. Set the python path and activate virtual environment
ENV PYTHONPATH /app
ENV PATH="/opt/venv/bin:$PATH"

# 6. Default command is pytest
CMD ["pytest"]


# ---- Final Stage (Production) ----
FROM python:3.14-slim as production

WORKDIR /app

# 1. Install runtime dependency
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka1 && rm -rf /var/lib/apt/lists/*

# 2. Copy virtual env
COPY --from=builder /opt/venv /opt/venv

# 3. Copy the code, PRESERVING the directory structure
COPY API/src /app/src
COPY API /app/API

# 4. Set the python path to the project root
ENV PYTHONPATH /app
# 5. Activate the virtual environment
ENV PATH="/opt/venv/bin:$PATH"

# 6. Run the API from its directory
CMD ["uvicorn", "API.src.main:app", "--host", "0.0.0.0", "--port", "8000"]
