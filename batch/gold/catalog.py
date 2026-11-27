"""Location and publication contract shared by the worker and the HTTP reader."""
from __future__ import annotations

import io
import json
import os
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

from minio import Minio
from minio.error import S3Error


TABLES = (
    "hour_distribution", "weekday_distribution", "weekday_hour_distribution", "day_type_distribution", "day_comparison",
)
LEGACY_TABLES = (
    "user_coverage", "hour_distribution", "weekday_distribution", "month_distribution",
    "day_type_distribution", "day_comparison", "listening_sessions", "weekly_trend", "hour_tags",
)
RUN_ID = re.compile(r"[0-9]{8}T[0-9]{12}Z_[0-9a-f]{8}")
MISSING = {"NoSuchBucket", "NoSuchKey", "NoSuchObject"}


@dataclass(repr=False)
class Settings:
    endpoint: str
    secure: bool
    access: str
    secret: str
    bucket: str
    prefix: str
    silver: str

    @classmethod
    def from_env(cls):
        value = (os.getenv("GOLD_MINIO_ENDPOINT") or os.getenv("ANALYSIS_MINIO_ENDPOINT")
                 or os.getenv("MINIO_ENDPOINT") or "localhost:9000").rstrip("/")
        scheme = "https" if os.getenv("MINIO_SECURE", "").lower() in ("true", "1", "yes") else "http"
        url = urlsplit(value if "://" in value else f"{scheme}://{value}")
        if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.path or url.query or url.fragment:
            raise ValueError("Invalid Gold MinIO endpoint (expected host:port or HTTP(S) URL)")
        access = os.getenv("MINIO_ACCESS_KEY") or os.getenv("MINIO_ROOT_USER")
        secret = os.getenv("MINIO_SECRET_KEY") or os.getenv("MINIO_ROOT_PASSWORD")
        if not access or not secret:
            raise ValueError("Gold requires MinIO credentials")
        source = "/".join(x for x in ("s3a://" + (os.getenv("MINIO_SILVER_BUCKET") or "silver"),
                          os.getenv("MINIO_SILVER_PREFIX", "").strip("/"), "fact_listen") if x)
        return cls(url.netloc, url.scheme == "https", access, secret,
                   os.getenv("MINIO_GOLD_BUCKET") or "gold",
                   os.getenv("MINIO_GOLD_PREFIX", "batch_analysis").strip("/"),
                   os.getenv("GOLD_SILVER_PATH") or source)

    def client(self):
        return Minio(self.endpoint, access_key=self.access, secret_key=self.secret, secure=self.secure)

    def delta_options(self):
        return {"AWS_ACCESS_KEY_ID": self.access, "AWS_SECRET_ACCESS_KEY": self.secret,
                "AWS_ENDPOINT_URL": ("https://" if self.secure else "http://") + self.endpoint,
                "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": str(not self.secure).lower(),
                "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false"}


class GoldCatalog:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = settings.client()
        self.bucket = settings.bucket
        self.prefix = settings.prefix + "/" if settings.prefix else ""

    def key(self, run, suffix):
        return f"{self.prefix}{run}/{suffix}"

    def manifest(self, run):
        if not RUN_ID.fullmatch(run):
            return None
        try:
            response = self.client.get_object(self.bucket, self.key(run, "_manifest.json"))
        except S3Error as exc:
            if exc.code in MISSING:
                return None
            raise
        try:
            data = json.loads(response.read())
        finally:
            response.close()
            response.release_conn()
        if (data.get("status") != "succeeded" or data.get("run_id") != run
                or set(data.get("tables", {})) not in (set(TABLES), set(LEGACY_TABLES))):
            return None
        return data

    def latest(self):
        # Nonrecursive listing returns one prefix per run, including notebook exports.
        try:
            runs = [o.object_name[len(self.prefix):].rstrip("/") for o in
                    self.client.list_objects(self.bucket, prefix=self.prefix) if o.is_dir]
        except S3Error as exc:
            if exc.code in MISSING:
                return None
            raise
        for run in sorted(runs, reverse=True):
            manifest = self.manifest(run)
            if manifest and set(manifest["tables"]) == set(TABLES):
                return manifest
        return None

    def ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            try:
                self.client.make_bucket(self.bucket)
            except S3Error as exc:
                if exc.code != "BucketAlreadyOwnedByYou":
                    raise

    def commit(self, manifest):
        body = json.dumps(manifest, ensure_ascii=False).encode("utf-8")
        self.client.put_object(self.bucket, self.key(manifest["run_id"], "_manifest.json"),
                               io.BytesIO(body), len(body), content_type="application/json")
