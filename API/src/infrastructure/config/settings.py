from pydantic_settings import BaseSettings, SettingsConfigDict


class AWSSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='AWS_')

    access_key_id: str
    secret_access_key: str
    session_token: str | None = None
    region: str = "us-east-1"
    s3_bucket: str
    cloudwatch_log_group: str
    cloudwatch_log_stream: str


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='KAFKA_')

    bootstrap_servers: str = "localhost:9092"
    topic: str = "document-events"


class Settings(BaseSettings):
    aws: AWSSettings = AWSSettings()
    kafka: KafkaSettings = KafkaSettings()