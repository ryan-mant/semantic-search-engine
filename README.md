# Semantic Search & Ingestion Engine

[![Python Version](https://img.shields.io/badge/python-3.12-blue.svg)](#)
[![Test Coverage](https://img.shields.io/badge/coverage-92%25-brightgreen.svg)](#)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](#)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](#)

An enterprise-grade, asynchronous document ingestion and semantic search engine built using Python, FastAPI, Apache Kafka, MongoDB, and AWS services, following the **Hexagonal Architecture (Ports & Adapters)** design pattern.

---

## 🎯 Project Objective

The primary objective of this project is to demonstrate a production-ready, highly resilient, and decoupled distributed system capable of handling high-throughput document ingestion. By using an event-driven flow with Apache Kafka, the ingestion API remains responsive and fast, offloading CPU-bound tasks (such as document processing, dense vector representation, and vector indexing) to background worker instances.

---

## 🏗️ Architecture & Best Practices

This project strictly adheres to **Clean Architecture** and **Hexagonal Architecture** principles.

### 📐 System Architecture Diagram

The end-to-end flow of document ingestion, asynchronous background processing, and semantic search queries is visualized below:

```mermaid
graph TD
    subgraph Client_User ["Client / User"]
        User([User / Client])
    end

    subgraph API_Container ["API Container"]
        FastAPI[FastAPI Router]
        IngestUC[IngestDocumentUseCase]
        SearchUC[SearchDocumentsUseCase]
        
        S3Adapter[S3StorageAdapter]
        MongoRep_API[MongoDB Repository]
        KafkaPublisher[KafkaEventPublisher]
        ST_API[SentenceTransformer Adapter]
        ChromaStore_API[ChromaDB Adapter]
    end

    subgraph Messaging ["Apache Kafka"]
        Kafka[Kafka Broker]
    end

    subgraph Worker_Container ["Worker Container"]
        Consumer[Kafka Consumer]
        MongoRep_Worker[MongoDB Repository]
        ST_Worker[SentenceTransformer Adapter]
        ChromaStore_Worker[ChromaDB Adapter]
    end

    subgraph Databases_Storage ["External Infrastructure (Databases & Storage)"]
        S3[(AWS S3 Storage)]
        Mongo[(MongoDB)]
        Chroma[(ChromaDB Vector DB)]
    end

    %% Flows
    User -->|1. POST /documents/ingest| FastAPI
    FastAPI -->|Executes| IngestUC
    IngestUC -->|2. Upload Stream| S3Adapter
    S3Adapter -->|Upload| S3
    IngestUC -->|3. Save Document (status: PENDING)| MongoRep_API
    MongoRep_API -->|Save| Mongo
    IngestUC -->|4. Publish Event| KafkaPublisher
    KafkaPublisher -->|Publish| Kafka
    FastAPI -->|5. HTTP 201 Created| User

    Kafka -->|6. Batch Consume Events| Consumer
    Consumer -->|7. Batch Generate Vectors| ST_Worker
    Consumer -->|8. Batch Index Vectors| ChromaStore_Worker
    ChromaStore_Worker -->|Upsert Batch| Chroma
    Consumer -->|9. Update status: INDEXED| MongoRep_Worker
    MongoRep_Worker -->|Update Status| Mongo

    User -->|"10. GET /documents/search?q=...&limit=5"| FastAPI
    FastAPI -->|Executes| SearchUC
    SearchUC -->|11. Generate Vector / Check LRU Cache| ST_API
    SearchUC -->|12. Query Similar Vectors| ChromaStore_API
    ChromaStore_API -->|Search (Cosine Space)| Chroma
    FastAPI -->|"13. Return Matches (score & distance)"| User

    User -->|14. GET /documents/{id}| FastAPI
    FastAPI -->|15. Fetch Metadata & Status| MongoRep_API
    FastAPI -->|16. Return Document Details| User
```

### 📦 Monorepo & Hexagonal Layer Structure

This repository is organized as a monorepo containing two **completely decoupled and self-contained microservices** that share no build-time code or runtime dependencies, ensuring high autonomy:
1. **API Service ([API/](./API))**: Exposes REST ingestion, search, and status tracking routes.
2. **Worker Service ([worker/](./worker))**: Consumes batches of document events from Kafka, generates vector representations in batch, indexes them in ChromaDB, and updates document status in MongoDB.

Both microservices implement their own isolated **Hexagonal Architecture (Ports & Adapters)** layer structure:

* **Domain Layer (Core)**: Completely isolated from frameworks, containing core entities (e.g., [Document](./API/src/domain/entities/document.py) in the API, and [Document](./worker/src/domain/entities/document.py) in the Worker), exceptions, and abstract ports:
  - API ports: [StoragePort](./API/src/domain/ports/storage.py), [EventPublisher](./API/src/domain/ports/event_publisher.py), [DocumentRepository](./API/src/domain/ports/document_repository.py), [EmbeddingPort](./API/src/domain/ports/embedding.py), [VectorStorePort](./API/src/domain/ports/vector_store.py).
  - Worker ports: [DocumentRepository](./worker/src/domain/ports/document_repository.py), [EmbeddingPort](./worker/src/domain/ports/embedding.py), [VectorStorePort](./worker/src/domain/ports/vector_store.py).
* **Application Layer**: Contains use cases coordinating the domain model (e.g. [IngestDocumentUseCase](./API/src/application/use_cases/ingest_document.py), [GetDocumentUseCase](./API/src/application/use_cases/get_document.py), and [SearchDocumentsUseCase](./API/src/application/use_cases/search_documents.py)).
* **Infrastructure Layer**: Framework-specific adapters implementing the domain ports (MongoDB, confluent-kafka, AWS S3, ChromaDB).
* **Resilient Worker Loop & Fault Tolerance (At-Least-Once Processing)**:
  * **Batch Processing & SIMD CPU Acceleration**: Consumes messages in configurable batches (16 messages / 0.5s), computing embeddings via SIMD vectorization and bulk upserting to ChromaDB for high indexing throughput.
  * **Manual Offset Commit (`enable.auto.commit = False`)**: The worker explicitly disables Kafka's automatic offset committing. Offsets are committed manually only *after* a batch has been successfully processed, indexed into ChromaDB, and updated to `INDEXED` in MongoDB.
  * **Poison Pill vs. Transient Error Handling with Exponential Backoff**: Corrupted or malformed payloads (e.g., `json.JSONDecodeError` or validation errors) are logged as poison pills and committed individually to avoid blocking the queue, while transient infrastructure failures (ChromaDB/MongoDB connection loss) trigger backoff and leave offsets uncommitted for automatic retry upon recovery.

---

## 🛠️ Technology Stack

- **FastAPI**: Asynchronous API framework.
- **Apache Kafka**: Decoupled event broker for message passing.
- **MongoDB (Motor)**: Document database for persisting metadata and document indexing status.
- **ChromaDB**: High-performance vector database configured with cosine distance space (`hnsw:space: cosine`).
- **Sentence-Transformers (`all-MiniLM-L6-v2`)**: Generates 384-dimensional dense vector representations of documents with in-memory LRU caching and batch inference.
- **AWS S3 / LocalStack**: Binary/raw storage for ingested documents.
- **AWS CloudWatch**: Standard log group registry.
- **Poetry**: Package dependency management.
- **Docker & Docker Compose**: Container orchestration with comprehensive service healthchecks.

---

## 🚀 Getting Started

### 📋 Prerequisites

- **Docker** and **Docker Compose** installed.

### ⚙️ Setup & Configuration

1. Clone this repository.
2. Initialize your local configuration file:
   ```bash
   cp .env.example .env
   ```
   *(The `.env` file is already listed in `.gitignore` and won't be pushed to git)*

### 🐳 Run with Docker Compose

Build and launch all services in the background (Kafka, MongoDB, ChromaDB, LocalStack, Ingestion API, and the processing Worker):

```bash
docker compose up --build -d
```

All services use Docker healthchecks, ensuring the API and Worker containers start only when MongoDB, Kafka, ChromaDB, and LocalStack are fully initialized and healthy.

Check service health logs:
```bash
docker compose logs api
docker compose logs worker
```

---

## 📡 API Usage Guide

### 1. Health Check
Verify the API is running correctly.

* **URL**: `/`
* **Method**: `GET`
* **Curl Command**:
  ```bash
  curl -X GET http://localhost:8000/
  ```
* **Response (200 OK)**:
  ```json
  {
    "status": "ok"
  }
  ```

### 2. Ingest Document
Submits a document to be uploaded raw to S3 and queued in Kafka for async batch vectorization, metadata storage, and vector indexing.

* **URL**: `/documents/ingest`
* **Method**: `POST`
* **Request Header**: `Content-Type: application/json`
* **Request Body**:
  ```json
  {
    "content": "The company policy requires all employees to submit their monthly expense reports by the last Friday of each month.",
    "metadata": {
      "user_id": "123",
      "category": "finance",
      "author": "John Doe"
    }
  }
  ```
* **Curl Command**:
  ```bash
  curl -X POST http://localhost:8000/documents/ingest \
    -H "Content-Type: application/json" \
    -d '{"content": "The company policy requires all employees to submit their monthly expense reports by the last Friday of each month.", "metadata": {"user_id": "123", "category": "finance", "author": "John Doe"}}'
  ```
* **Response (201 Created)**:
  ```json
  {
    "id": "e0bfa934-8b64-4bf8-b99b-3ee9c27ee98d",
    "content": "The company policy requires all employees to submit their monthly expense reports by the last Friday of each month.",
    "metadata": {
      "user_id": "123",
      "category": "finance",
      "author": "John Doe",
      "storage_url": "s3://raw-documents/raw/e0bfa934-8b64-4bf8-b99b-3ee9c27ee98d.txt"
    },
    "status": "PENDING",
    "created_at": "2026-07-02T19:35:10.512000Z"
  }
  ```

### 3. Get Document by ID
Check the processing status (`PENDING` or `INDEXED`) and metadata of an ingested document.

* **URL**: `/documents/{id}`
* **Method**: `GET`
* **Curl Command**:
  ```bash
  curl -X GET http://localhost:8000/documents/e0bfa934-8b64-4bf8-b99b-3ee9c27ee98d
  ```
* **Response (200 OK)**:
  ```json
  {
    "id": "e0bfa934-8b64-4bf8-b99b-3ee9c27ee98d",
    "content": "The company policy requires all employees to submit their monthly expense reports by the last Friday of each month.",
    "metadata": {
      "user_id": "123",
      "category": "finance",
      "author": "John Doe",
      "storage_url": "s3://raw-documents/raw/e0bfa934-8b64-4bf8-b99b-3ee9c27ee98d.txt"
    },
    "status": "INDEXED",
    "created_at": "2026-07-02T19:35:10.512000Z"
  }
  ```

### 4. Semantic Search
Search for similar documents using natural language. The API checks an in-memory LRU cache or generates an embedding for the query, and searches ChromaDB using cosine distance.

* **URL**: `/documents/search`
* **Method**: `GET`
* **Query Parameters**:
  * `q` *(string, required)*: The search query text.
  * `limit` *(integer, optional, default: 5, min: 1, max: 100)*: Number of top results to return.
* **Curl Command**:
  ```bash
  curl -X GET "http://localhost:8000/documents/search?q=expense%20deadline&limit=5"
  ```
* **Response (200 OK)**:
  ```json
  [
    {
      "id": "e0bfa934-8b64-4bf8-b99b-3ee9c27ee98d",
      "content": "The company policy requires all employees to submit their monthly expense reports by the last Friday of each month.",
      "metadata": {
        "user_id": "123",
        "category": "finance",
        "author": "John Doe",
        "storage_url": "s3://raw-documents/raw/e0bfa934-8b64-4bf8-b99b-3ee9c27ee98d.txt"
      },
      "score": 0.894310,
      "distance": 0.105690
    }
  ]
  ```

> **Understanding Metrics:**
> - `score` (`0.0` - `1.0`): Normalized similarity (`1.0 - distance`). Values closer to **1.0 (100%)** indicate high relevance.
> - `distance`: Raw cosine distance computed by the vector database. Values closer to **0.0** indicate exact vector proximity.

---

## 🧪 Running Tests

You can run unit and integration tests inside the dedicated docker test containers.

### Run API Tests
```bash
docker compose run --rm api-test
```

### Run Worker Tests
```bash
docker compose run --rm worker-test
```

Alternatively, if you want to run tests locally with Poetry:
```bash
# Inside API/ directory
cd API && poetry install && poetry run pytest

# Inside worker/ directory
cd worker && poetry install && poetry run pytest
```

---

## ⚡ Load Testing & Performance Optimization

To validate the high-throughput capabilities and check the decoupling under stress, we ran load tests using **k6** simulating **100 concurrent virtual users (VUs)** for 30 seconds.

### Mixed Workload Benchmark (60% Ingestion / 40% Semantic Search)

| Metric | Before Optimization | After Optimization | Improvement |
| :--- | :--- | :--- | :--- |
| **Total Completed Requests** | 1,950 | **4,392** | **+125%** (More than double throughput) |
| **Throughput (req/s)** | 64.93 req/s | **146.36 req/s** | **+125%** |
| **Average Ingest Latency** | 1,167 ms | **398.90 ms** | **~3x Faster** |
| **Median Ingest Latency** | 1,234 ms | **417.57 ms** | **~3x Faster** |
| **Minimum Ingest Latency** | 16.49 ms | **5.82 ms** | **~3x Faster** |
| **p(95) Ingest Latency** | 1,821 ms | **685.51 ms** | **~2.7x Faster** |
| **Average Search Latency** | 674.77 ms | **450.95 ms** | **~1.5x Faster** |

### 🛠️ Key Architectural Optimizations Implemented

1. **Non-Blocking S3 Upload (Thread Pool Offloading):** 
   Converted the `S3StorageAdapter.upload_stream` method into an async coroutine. Wrapping boto3's synchronous `upload_fileobj` inside `asyncio.to_thread` offloads I/O-bound disk and network operations to Python's internal thread pool, freeing the main ASGI event loop.
2. **Re-use of S3 Adapter Singleton:** 
   Restructured the FastAPI dependency tree so the `S3StorageAdapter` is instantiated once during application startup lifespan (`app.state.storage_adapter`) and re-used as a singleton. This eliminates redundant boto3 client creations and `create_bucket` S3 network calls on every HTTP request.
3. **Asynchronous Kafka Event Production (Removing Blocking Flush):** 
   Removed the per-request synchronous `flush()` call inside `KafkaEventPublisher.publish_document_created`. The publisher now enqueues events asynchronously using confluent-kafka's C-backed memory buffer and triggers callbacks via non-blocking `.poll(0)`. A final `flush(timeout=5)` is executed once when the application shuts down.
4. **Worker Batch Processing & SIMD Acceleration:**
   The background worker consumes messages in configurable batches (`batch_size=16`, `batch_timeout=0.5s`). Running batch vector inference through `SentenceTransformer.encode(texts)` leverages AVX2/SIMD instructions, delivering up to 4x higher CPU throughput than processing messages one-by-one.
5. **In-Memory LRU Cache for Search Queries:**
   Integrated an LRU cache in `SentenceTransformerEmbeddingAdapter` to eliminate redundant CPU computation for repeated search queries, returning search vectors instantly at 0ms CPU overhead.
6. **Container Healthchecks & Coordinated Startup:**
   Integrated Docker healthchecks for MongoDB, Kafka, ChromaDB, and LocalStack with `condition: service_healthy`, eliminating connection race conditions on startup.

### 🏃 Running Load Tests

To run the load test locally, make sure the services are running (`docker compose up -d`) and k6 is installed:

```bash
k6 run teste-carga.js
```

### 📉 Resource Limits & Scaling Analysis (Realistic Production Simulation)

To simulate different cloud deployment sizes (e.g., AWS ECS or Kubernetes container sizes), we benchmarked the optimized API under different resource allocations in `docker-compose.yml` with the same load test (100 VUs, 30s):

| Metric | Unconstrained (Host Machine) | Standard Container size (2.0 CPUs / 1GB-2GB RAM) | Constrained Container size (0.5 CPU / 1GB RAM) |
| :--- | :--- | :--- | :--- |
| **Total Completed Requests** | **4,392** | **3,696** | **775** |
| **Throughput (req/s)** | **146.36 req/s** | **123.00 req/s** | **25.75 req/s** |
| **Average Ingest Latency** | 398.90 ms | **485.64 ms** | 2,398.34 ms |
| **Median Ingest Latency** | 417.57 ms | **537.25 ms** | 2,392.89 ms |
| **Minimum Ingest Latency** | 5.82 ms | **5.92 ms** | 5.62 ms |
| **Average Search Latency** | 450.95 ms | **525.34 ms** | 2,756.77 ms |
| **Error Rate (%)** | 0.00% | 0.00% | 0.00% |

> [!IMPORTANT]
> **Understanding the Bottleneck (Local Embedding Model):** The performance degradation observed under strict resource constraints (e.g., 0.5 CPU) is **exclusively** caused by running the PyTorch-based embedding model (`SentenceTransformers`) locally in-process. Machine learning vector generation is a heavy, CPU-bound calculation. In a pure I/O-bound async ingestion path (without in-process inference), the API maintains high throughput and low latencies even under strict resource limits.

#### 💡 Key Architectural Takeaways

1. **Efficient Scaling with Standard Container Sizes:**
   * Giving the API container **2.0 CPU cores** allows it to perform at **~84% of the speed of the unconstrained host machine**, delivering a solid **123 req/s** with sub-500ms average latencies.
2. **The In-Process Embedding Bottleneck:**
   * Because the search endpoint generates embeddings in-process via `SentenceTransformer` on a CPU-bound thread, strict CPU constraints (like 0.5 CPU) create extreme CPU starvation, causing requests to queue up at the container gateway and pushing latencies to >2s.
3. **Production Design Recommendations:**
   * **Offload Embeddings:** Outsource query embedding generation to a dedicated model serving tier (like AWS SageMaker, Triton, or Hugging Face TEI) running on optimized or GPU-enabled instances.
   * **Lightweight / External Models:** Use lightweight embedding models or standard third-party APIs (like OpenAI embeddings) to convert search CPU-bound tasks into simple, fast, non-blocking HTTP requests.


