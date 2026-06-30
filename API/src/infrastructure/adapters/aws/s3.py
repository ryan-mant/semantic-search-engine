import boto3
from botocore.exceptions import BotoCoreError, ClientError
from typing import BinaryIO

from src.domain.ports.storage import StoragePort
from src.domain.exceptions import StorageError
from src.infrastructure.config.settings import Settings


class S3StorageAdapter(StoragePort):
    def __init__(self, settings: Settings) -> None:
        self._bucket_name = settings.aws.s3_bucket
        if not self._bucket_name:
            raise StorageError("S3 bucket name is not configured. Please set the AWS_S3_BUCKET environment variable.")
        
        client_kwargs = {
            "region_name": settings.aws.region
        }
        if settings.aws.access_key_id:
            client_kwargs["aws_access_key_id"] = settings.aws.access_key_id
        if settings.aws.secret_access_key:
            client_kwargs["aws_secret_access_key"] = settings.aws.secret_access_key
        if settings.aws.session_token:
            client_kwargs["aws_session_token"] = settings.aws.session_token

        try:
            self._s3_client = boto3.client("s3", **client_kwargs)
        except (BotoCoreError, ClientError) as e:
            raise StorageError(f"Failed to initialize S3 client: {e}") from e

    def upload_stream(self, file_obj: BinaryIO, key: str) -> str:
        try:
            self._s3_client.upload_fileobj(file_obj, self._bucket_name, key)
            return f"s3://{self._bucket_name}/{key}"
        except (BotoCoreError, ClientError) as e:
            raise StorageError(f"S3 upload failed for key '{key}': {e}") from e