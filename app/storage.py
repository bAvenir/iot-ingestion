"""M1/M5 — S3 (RustFS) access.

Only two uses: oversized payloads to landing/{YYYY}/{MM}/{DD}/{job_id}.json
(written BEFORE the enqueue), and published/ RDF + DCAT output in M5.
warehouse/ is owned by pyiceberg, not touched here.
"""

import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()


class S3Storage:
    def __init__(self):
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.S3_ACCESS_KEY,
            aws_secret_access_key=settings.S3_SECRET_KEY,
            # RustFS ignores the region, but botocore refuses to sign without one.
            region_name=settings.S3_REGION,
            # Path-style: http://localhost:9000/landing/<key>. The default
            # virtual-host style would resolve http://landing.localhost:9000 and fail.
            config=Config(s3={"addressing_style": "path"}),
        )
        self.bucket = settings.S3_BUCKET_LANDING

    def upload_file(self, bucket, body, key):
        self.client.put_object(Bucket=bucket, Key=key, Body=body)

    def model_exists(self, key) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise
