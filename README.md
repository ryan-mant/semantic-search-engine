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
    subgraph Client / User
        User([User / Client])
    end

    subgraph API [Ingestion & Search API]
        FastAPI[FastAPI Controller]
        IngestUC[IngestDocumentUseCase]
        SearchAdapter[Search Controller]
    end

    subgraph Messaging [Event Broker]
        Kafka[Apache Kafka Broker]
    end

    subgraph Worker [Asynchronous Processing Worker]
        Consumer[Kafka Consumer]
        ST[SentenceTransformer Adapter]
        MongoRep[MongoDB Repository]
        ChromaStore[ChromaDB Vector Store Adapter]
    end

    subgraph Storage [Storage & Infrastructure]
        S3[(AWS S3 Storage)]
        Mongo[(MongoDB)]
        Chroma[(ChromaDB Vector DB)]
        CW[(AWS CloudWatch Logs)]
    end

    %% Flows
    User -->|1. POST /documents/ingest| FastAPI
    FastAPI -->|Executes| IngestUC
    IngestUC -->|2. Save Raw Document| S3
    IngestUC -->|3. Publish Ingestion Event| Kafka
    IngestUC -->|4. Log Event| CW
    FastAPI -->|5. HTTP 201 Created| User

    Kafka -->|Consume Ingestion Event| Consumer
    Consumer -->|6. Save Document Metadata| MongoRep
    MongoRep -->|Persist| Mongo
    Consumer -->|7. Generate Dense Vector| ST
    Consumer -->|8. Index Vector & Metadata| ChromaStore
    ChromaStore -->|Upsert| Chroma

    User -->|9. GET /documents/search?q=...| FastAPI
    FastAPI -->|10. Generate Query Vector| ST
    FastAPI -->|11. Query Similar Vectors| ChromaStore
    ChromaStore -->|Search| Chroma
    FastAPI -->|12. Return Matches| User
```

### 📦 Hexagonal Layer Structure

* **Domain Layer (Core)**: Completely isolated from frameworks, containing core entities (e.g. [Document](file:///home/ryan-dev/Documentos/projetos/motor-ingestao-busca/API/src/domain/entities/document.py)), domain exceptions, and abstract ports:
  - [LoggerPort](file:///home/ryan-dev/Documentos/projetos/motor-ingestao-busca/API/src/domain/ports/logger.py)
  - [StoragePort](file:///home/ryan-dev/Documentos/projetos/motor-ingestao-busca/API/src/domain/ports/storage.py)
  - [EventPublisher](file:///home/ryan-dev/Documentos/projetos/motor-ingestao-busca/API/src/domain/ports/event_publisher.py)
  - [DocumentRepository](file:///home/ryan-dev/Documentos/projetos/motor-ingestao-busca/API/src/domain/ports/document_repository.py)
  - [EmbeddingPort](file:///home/ryan-dev/Documentos/projetos/motor-ingestao-busca/API/src/domain/ports/embedding.py)
  - [VectorStorePort](file:///home/ryan-dev/Documentos/projetos/motor-ingestao-busca/API/src/domain/ports/vector_store.py)
* **Application Layer**: Contains use cases coordinating the domain model (e.g. [IngestDocumentUseCase](file:///home/ryan-dev/Documentos/projetos/motor-ingestao-busca/API/src/application/use_cases/ingest_document.py)).
* **Infrastructure Layer**: Framework-specific adapters implementing the domain ports (MongoDB, confluent-kafka, AWS S3, AWS CloudWatch, ChromaDB).
* **Structured Logging**: Outputs JSON-formatted logs to stdout and AWS CloudWatch for modern observability.
* **Resilient Worker Loop**: The Kafka consumer includes fallback handling to guarantee that processing errors on a single message do not block or crash the consumer daemon.

---

## 🛠️ Technology Stack

- **FastAPI**: Asynchronous API framework.
- **Apache Kafka**: Decoupled event broker for message passing.
- **MongoDB (Motor)**: Document database for persisting metadata.
- **ChromaDB**: High-performance vector database for semantic indexing.
- **Sentence-Transformers (`all-MiniLM-L6-v2`)**: Generates 384-dimensional dense vector representations of documents.
- **AWS S3 / LocalStack**: Binary/raw storage for ingested documents.
- **AWS CloudWatch**: Standard log group registry.
- **Poetry**: Package dependency management.
- **Docker & Docker Compose**: Container orchestration.

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
Submits a document to be uploaded raw to S3 and queued in Kafka for async extraction, processing, metadata storage, and vector indexing.

* **URL**: `/documents/ingest`
* **Method**: `POST`
* **Request Header**: `Content-Type: application/json`
* **Request Body**:
  ```json
  {
    "content": "Artificial intelligence is transforming software engineering by automating repetitive tasks and proposing clean code structures.",
    "metadata": {
      "user_id": "999",
      "category": "technology",
      "author": "Antigravity"
    }
  }
  ```
* **Curl Command**:
  ```bash
  curl -X POST http://localhost:8000/documents/ingest \
    -H "Content-Type: application/json" \
    -d '{"content": "Artificial intelligence is transforming software engineering by automating repetitive tasks and proposing clean code structures.", "metadata": {"user_id": "999", "category": "technology", "author": "Antigravity"}}'
  ```
* **Response (201 Created)**:
  ```json
  {
    "id": "e0bfa934-8b64-4bf8-b99b-3ee9c27ee98d",
    "content": "Artificial intelligence is transforming software engineering by automating repetitive tasks and proposing clean code structures.",
    "metadata": {
      "user_id": "999",
      "category": "technology",
      "author": "Antigravity"
    },
    "created_at": "2026-07-02T19:35:10.512Z"
  }
  ```

### 3. Semantic Search
Search for similar documents using natural language. The API generates an embedding for the query and searches the vector store using cosine similarity.

* **URL**: `/documents/search`
* **Method**: `GET`
* **Query Parameters**: `q` (The search query)
* **Curl Command**:
  ```bash
  curl -X GET "http://localhost:8000/documents/search?q=AI%20in%20coding"
  ```
* **Response (200 OK)**:
  ```json
  [
    {
      "id": "e0bfa934-8b64-4bf8-b99b-3ee9c27ee98d",
      "content": "Artificial intelligence is transforming software engineering by automating repetitive tasks and proposing clean code structures.",
      "metadata": {
        "user_id": "999",
        "category": "technology",
        "author": "Antigravity"
      },
      "score": 0.824719
    }
  ]
  ```

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
