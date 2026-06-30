# ---- Build Stage ----
FROM python:3.14-slim as builder

# 1. Install build dependencies and a SPECIFIC, MODERN version of poetry
RUN apt-get update && apt-get install -y --no-install-recommends build-essential librdkafka-dev
RUN pip install poetry>=2.0.0 poetry-plugin-export

# 2. Copy poetry files and export requirements.txt
WORKDIR /app
COPY API/pyproject.toml API/poetry.lock ./
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

# 3. Create a venv and install dependencies with pip
RUN python -m venv /opt/venv
RUN /opt/venv/bin/pip install --no-cache-dir -r requirements.txt


# ---- Final Stage ----
FROM python:3.14-slim

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
