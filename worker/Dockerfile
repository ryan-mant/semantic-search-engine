# ---- Build Stage ----
FROM python:3.14-slim as builder

# 1. Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends build-essential librdkafka-dev

# 2. Create a virtual environment
RUN python -m venv /opt/venv

# 3. Install Python packages
COPY worker/requirements.txt /app/requirements.txt
RUN /opt/venv/bin/pip install --no-cache-dir -r /app/requirements.txt


# ---- Final Stage ----
FROM python:3.14-slim

WORKDIR /app

# 1. Install runtime dependency
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka1 && rm -rf /var/lib/apt/lists/*

# 2. Copy virtual env
COPY --from=builder /opt/venv /opt/venv

# 3. Copy the code, PRESERVING the directory structure
COPY API/src /app/src
COPY worker /app/worker

# 4. Set the python path to the project root
ENV PYTHONPATH /app
# 5. Activate the virtual environment
ENV PATH="/opt/venv/bin:$PATH"

# 6. Run the worker from its directory
CMD ["python", "-u", "worker/main.py"]
