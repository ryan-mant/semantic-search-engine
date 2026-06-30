# Semantic Search & Ingestion Engine

An enterprise-grade, asynchronous document ingestion and semantic search engine built using Python, FastAPI, Apache Kafka, and AWS services, following the **Hexagonal Architecture (Ports & Adapters)** design pattern.

---

## 🎯 Project Objective

The primary objective of this project is to demonstrate a production-ready, highly resilient, and decoupled distributed system capable of handling high-throughput document ingestion. By using an event-driven flow with Apache Kafka, the ingestion API remains responsive and fast, offloading CPU-bound tasks (such as document processing and vector representation) to background worker instances.

---

## 🏗️ Architecture & Best Practices

This project strictly adheres to **Clean Architecture** and **Hexagonal Architecture** principles:

- **Domain Layer (Core)**: Completely isolated from frameworks, containing core entities ([Document](file:///app/src/domain/entities/document.py)), domain exceptions, and abstract ports (`LoggerPort`, `StoragePort`, `EventPublisher`, `DocumentRepository`).
- **Application Layer**: Contains use cases coordinating the domain model ([IngestDocumentUseCase](file:///app/src/application/use_cases/ingest_document.py)).
- **Infrastructure Layer**: Framework-specific adapters implementing the domain ports (MongoDB, confluent-kafka, AWS S3, AWS CloudWatch).
- **Structured Logging**: Outputs JSON formatted logs to stdout and AWS CloudWatch for modern observability.
- **Resilient Worker Loop**: The Kafka consumer includes fallback handling to guarantee that processing errors on a single message do not block or crash the consumer daemon.

---

## 🛠️ Technology Stack

- **FastAPI**: Asynchronous API framework.
- **Apache Kafka**: Decoupled event broker for message passing.
- **MongoDB (Motor)**: Document database for persisting metadata.
- **AWS S3**: Binary/raw storage for ingested documents.
- **AWS CloudWatch**: Standard log group registry.
- **Poetry**: Package dependency management.
- **Docker & Docker Compose**: Container orchestration.

---

## 🚀 Getting Started

### Prerequisites

- Docker and Docker Compose installed.

### Run with Docker Compose

Build and launch all services (Zookeeper, Kafka, API, and Worker):

```bash
docker compose up --build -d
```

### Ingesting a Document (Verification)

Send a POST request to the API to ingest a document:

```bash
curl -X POST http://localhost:8000/documents \
  -H "Content-Type: application/json" \
  -d '{"content": "Enterprise invoice document content...", "metadata": {"user_id": "123", "type": "pdf"}}'
```

Check the worker logs to verify successful processing:

```bash
docker compose logs worker
```
