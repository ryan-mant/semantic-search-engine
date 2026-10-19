import os
from unittest import mock
import pytest

from src.infrastructure.config.settings import Settings, AWSSettings, KafkaSettings


def test_settings_default() -> None:
    with mock.patch.dict(os.environ, {}, clear=True):
        settings = Settings()
        assert settings.aws.region == "us-east-1"
        assert settings.kafka.bootstrap_servers == "localhost:9092"
        assert settings.kafka.topic == "document-events"
        assert settings.kafka.acks == "all"
        assert settings.kafka.enable_idempotence is True
        assert settings.kafka.compression_type == "lz4"
        assert settings.kafka.linger_ms == 10
        assert settings.kafka.batch_size == 32768
        assert settings.kafka.delivery_timeout_ms == 120000
        assert settings.kafka.max_in_flight_requests_per_connection == 5


def test_settings_env_override() -> None:
    env_vars = {
        "AWS_REGION": "us-west-2",
        "AWS_S3_BUCKET": "my-test-bucket",
        "AWS_ACCESS_KEY_ID": "test-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "KAFKA_TOPIC": "my-topic",
    }
    with mock.patch.dict(os.environ, env_vars):
        settings = Settings(aws=AWSSettings(), kafka=KafkaSettings())
        assert settings.aws.region == "us-west-2"
        assert settings.aws.s3_bucket == "my-test-bucket"
        assert settings.aws.access_key_id == "test-key"
        assert settings.aws.secret_access_key == "test-secret"
        assert settings.kafka.bootstrap_servers == "kafka:9092"
        assert settings.kafka.topic == "my-topic"
