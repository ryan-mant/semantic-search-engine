from pydantic_settings import BaseSettings, SettingsConfigDict


class AWSSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='AWS_')

    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    region: str = "us-east-1"
    s3_bucket: str | None = None
    cloudwatch_log_group: str | None = None
    cloudwatch_log_stream: str | None = None


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='KAFKA_')

    bootstrap_servers: str = "localhost:9092"
    topic: str = "document-events"


class Settings(BaseSettings):
    aws: AWSSettings = AWSSettings()
    kafka: KafkaSettings = KafkaSettings()