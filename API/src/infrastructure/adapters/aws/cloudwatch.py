import time
import json
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.domain.ports.logger import LoggerPort
from src.domain.exceptions import LoggingError
from src.infrastructure.config.settings import Settings


class CloudWatchLoggerAdapter(LoggerPort):
    def __init__(self, settings: Settings) -> None:
        self._log_group = settings.aws.cloudwatch_log_group
        self._log_stream = settings.aws.cloudwatch_log_stream
        if not self._log_group or not self._log_stream:
            raise LoggingError("CloudWatch log group and stream must be configured. Please set the AWS_CLOUDWATCH_LOG_GROUP and AWS_CLOUDWATCH_LOG_STREAM environment variables.")
        self._lock = threading.Lock()
        self._sequence_token: Optional[str] = None

        client_kwargs = {
            "region_name": settings.aws.region
        }
        if settings.aws.access_key_id:
            client_kwargs["aws_access_key_id"] = settings.aws.access_key_id
        if settings.aws.secret_access_key:
            client_kwargs["aws_secret_access_key"] = settings.aws.secret_access_key
        if settings.aws.session_token:
            client_kwargs["aws_session_token"] = settings.aws.session_token
        if settings.aws.endpoint_url:
            client_kwargs["endpoint_url"] = settings.aws.endpoint_url

        try:
            self._client = boto3.client("logs", **client_kwargs)
            
            try:
                self._client.create_log_group(logGroupName=self._log_group)
            except self._client.exceptions.ResourceAlreadyExistsException:
                pass

            try:
                self._client.create_log_stream(
                    logGroupName=self._log_group, logStreamName=self._log_stream
                )
            except self._client.exceptions.ResourceAlreadyExistsException:
                pass
                
            self._fetch_sequence_token()
        except (BotoCoreError, ClientError) as e:
            raise LoggingError(f"Failed to initialize CloudWatch Logs: {e}") from e

    def _fetch_sequence_token(self) -> None:
        try:
            response = self._client.describe_log_streams(
                logGroupName=self._log_group,
                logStreamNamePrefix=self._log_stream
            )
            for stream in response.get("logStreams", []):
                if stream.get("logStreamName") == self._log_stream:
                    self._sequence_token = stream.get("uploadSequenceToken")
                    break
        except Exception:
            pass

    def _log(self, message: str, level: str, extra: Optional[Dict[str, Any]] = None) -> None:
        log_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": level,
            "message": message,
            "extra": extra or {},
        }
        log_message = json.dumps(log_payload)
        timestamp_ms = int(time.time() * 1000)

        event = {
            "timestamp": timestamp_ms,
            "message": log_message
        }

        with self._lock:
            kwargs = {
                "logGroupName": self._log_group,
                "logStreamName": self._log_stream,
                "logEvents": [event],
            }
            if self._sequence_token:
                kwargs["sequenceToken"] = self._sequence_token

            try:
                response = self._client.put_log_events(**kwargs)
                self._sequence_token = response.get("nextSequenceToken")
            except self._client.exceptions.InvalidSequenceTokenException as e:
                self._sequence_token = e.response.get("expectedSequenceToken")
                kwargs["sequenceToken"] = self._sequence_token
                response = self._client.put_log_events(**kwargs)
                self._sequence_token = response.get("nextSequenceToken")
            except (BotoCoreError, ClientError) as e:
                raise LoggingError(f"CloudWatch logging failed: {e}") from e

    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._log(message, "INFO", extra)

    def error(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._log(message, "ERROR", extra)

    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._log(message, "WARNING", extra)

    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self._log(message, "DEBUG", extra)
