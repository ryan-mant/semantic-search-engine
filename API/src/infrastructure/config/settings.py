from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MongoSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='MONGO_')

    uri: str = "mongodb://localhost:27017"
    database: str = "ingestion_engine"
    username: str | None = None
    password: str | None = None


class AWSSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='AWS_')

    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    region: str = "us-east-1"
    s3_bucket: str | None = None
    cloudwatch_log_group: str | None = None
    cloudwatch_log_stream: str | None = None
    endpoint_url: str | None = None


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='KAFKA_')

    bootstrap_servers: str = "localhost:9092"
    topic: str = "document-events"


class ChromaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='CHROMA_')

    host: str = "localhost"
    port: int = 8000
    collection_name: str = "documents"


class Settings(BaseSettings):
    mongo: MongoSettings = Field(default_factory=MongoSettings)
    aws: AWSSettings = Field(default_factory=AWSSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    chroma: ChromaSettings = Field(default_factory=ChromaSettings)