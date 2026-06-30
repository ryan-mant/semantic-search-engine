import json
import os
from typing import Any, Dict

from confluent_kafka import Producer
from fastapi import FastAPI

app = FastAPI(
    title="Motor de Ingestão e Busca API",
    description="API para ingestão de documentos.",
    version="0.1.0",
)

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
topic = os.environ.get("KAFKA_TOPIC", "documents")

producer = Producer({"bootstrap.servers": bootstrap_servers})


@app.get("/", tags=["Health Check"])
async def health_check():
    """
    A simple health check endpoint.
    """
    return {"status": "ok"}


@app.post("/documents", status_code=202, tags=["Documents"])
async def create_document(document: Dict[str, Any]):
    """
    Receives a document and sends it to the ingestion pipeline.
    """
    payload = json.dumps(document)
    producer.produce(topic=topic, value=payload)
    producer.flush()
    return {"message": "Document received for processing."}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
