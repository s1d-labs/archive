"""Content-addressed blob store on B2 (S3-compatible). A blob is keyed by the sha256 of its bytes, so identical content
is stored once no matter how many captures reference it. Keys: raw/<sha[:2]>/<sha>. Text extracted from a blob is stored
beside it as txt/<sha[:2]>/<sha> (so full-text and re-parsing never re-fetch)."""
import hashlib

import boto3
from botocore.config import Config

from . import config

_client = None
_bucket = None


def _s3(write=False):
    global _client, _bucket
    if _client is None:
        e = config.store_env(write=write)
        _bucket = e["B2_BUCKET"]
        _client = boto3.client("s3", endpoint_url=e["AWS_ENDPOINT_URL"], region_name=e.get("AWS_DEFAULT_REGION"),
                               aws_access_key_id=e["AWS_ACCESS_KEY_ID"], aws_secret_access_key=e["AWS_SECRET_ACCESS_KEY"],
                               config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, max_pool_connections=32))
    return _client, _bucket


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def blob_key(sha: str) -> str:
    return f"raw/{sha[:2]}/{sha}"


def text_key(sha: str) -> str:
    return f"txt/{sha[:2]}/{sha}"


def exists(key: str) -> bool:
    s3, bucket = _s3()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def put_blob(data: bytes, content_type: str = None) -> tuple[str, str, bool]:
    """Store raw bytes if not already present. Returns (sha256, key, newly_stored)."""
    sha = sha256(data)
    key = blob_key(sha)
    s3, bucket = _s3(write=True)
    if exists(key):
        return sha, key, False
    s3.put_object(Bucket=bucket, Key=key, Body=data, **({"ContentType": content_type} if content_type else {}))
    return sha, key, True


def put_text(sha: str, text: str) -> str:
    key = text_key(sha)
    s3, bucket = _s3(write=True)
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8", "replace"), ContentType="text/plain; charset=utf-8")
    return key


def get_blob(sha: str) -> bytes:
    s3, bucket = _s3()
    return s3.get_object(Bucket=bucket, Key=blob_key(sha))["Body"].read()


def get_text(sha: str) -> str:
    s3, bucket = _s3()
    return s3.get_object(Bucket=bucket, Key=text_key(sha))["Body"].read().decode("utf-8", "replace")
