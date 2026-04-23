from __future__ import annotations

import os

import boto3


endpoint = os.getenv("S3_ENDPOINT", "http://minio.streaming.svc.cluster.local:9000")
access_key = os.getenv("S3_ACCESS_KEY", "minio")
secret_key = os.getenv("S3_SECRET_KEY", "minio123")

client = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)

for bucket_name in ("raw-clickstream", "curated-clickstream"):
    buckets = [bucket["Name"] for bucket in client.list_buckets()["Buckets"]]
    if bucket_name not in buckets:
        client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket {bucket_name}")
    else:
        print(f"Bucket {bucket_name} already exists")
