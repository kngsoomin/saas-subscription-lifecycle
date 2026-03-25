# keep local adapter for local validation
import io
import json
from pathlib import Path
from sys import base_prefix
from typing import List

import pandas as pd
from abc import ABC, abstractmethod

import boto3
from botocore.exceptions import ClientError

from src.common.constants import EC2_DATA_ROOT
from src.common.config import S3_BUCKET, S3_BASE_PREFIX



class Storage(ABC):

    @abstractmethod
    def join(self, *parts: str) -> str:
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass

    @abstractmethod
    def list_paths(self, base_dir: str, pattern: str) -> List[str]:
        pass

    @abstractmethod
    def read_jsonl(self, path: str) -> List[dict]:
        pass

    @abstractmethod
    def write_jsonl(self, path: str, records: List[dict]) -> None:
        pass

    @abstractmethod
    def read_parquet(self, path: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_parquet(self, path: str, df: pd.DataFrame) -> None:
        pass


class LocalStorage(Storage):
    """
    Local filesystem-backed storage helper
    """
    def __init__(self, base_dir: str = EC2_DATA_ROOT):
        self.base_dir = base_dir.rstrip("/")

    # logical path helpers
    def basename(self, path: str) -> str:
        return Path(path).name

    def parent(self, path: str) -> str:
        parent = Path(path).parent
        return "" if str(parent) == "." else str(parent)

    def join(self, *parts: str) -> str:
        cleaned = [p.strip("/") for p in parts if p]
        return str(Path(*cleaned)) if cleaned else ""

    # internal path conversion
    def _to_physical_path(self, path: str) -> Path:
        p = Path(path)

        if p.is_absolute():
            resolved = p.resolve()
            try:
                resolved.relative_to(self.base_dir)
            except ValueError:
                raise ValueError(f"Path escapes local storage root: {path}")
            return resolved

        return (self.base_dir / p).resolve()

    def _to_logical_path(self, path: Path) -> str:
        try:
            return str(path.resolve().relative_to(self.base_dir))
        except ValueError:
            raise ValueError(f"Path is outside local storage root: {path}")

    # high-level storage contract
    def exists(self, path: str) -> bool:
        return self._to_physical_path(path).exists()

    def list_paths(self, base_dir: str, pattern: str) -> List[str]:
        physical_base = self._to_physical_path(base_dir)
        return sorted(
            self._to_logical_path(p)
            for p in physical_base.glob(pattern)
        )

    def open_text_read(self, path: str):
        physical_path = self._to_physical_path(path)
        return physical_path.open("r", encoding="utf-8")

    def open_text_write(self, path: str):
        physical_path = self._to_physical_path(path)
        physical_path.parent.mkdir(parents=True, exist_ok=True)
        return physical_path.open("w", encoding="utf-8")

    def read_jsonl(self, path: str) -> List[dict]:
        physical_path = self._to_physical_path(path)
        records: List[dict] = []

        with physical_path.open("r", encoding="utf-8") as f:
            for line in f:
                records.append(json.loads(line))

        return records

    def write_jsonl(self, path: str, records: List[dict]) -> None:
        physical_path = self._to_physical_path(path)
        physical_path.parent.mkdir(parents=True, exist_ok=True)

        with physical_path.open("w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

    def read_parquet(self, path: str) -> pd.DataFrame:
        physical_path = self._to_physical_path(path)
        return pd.read_parquet(physical_path)

    def write_parquet(self, path: str, df: pd.DataFrame) -> None:
        physical_path = self._to_physical_path(path)
        physical_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(physical_path, index=False)



class S3Storage(Storage):
    def __init__(self,
                 bucket: str = S3_BUCKET,
                 base_prefix: str = S3_BASE_PREFIX):

        self.bucket = bucket
        self.base_prefix = base_prefix.strip("/")
        self.s3_client = boto3.client("s3")

    # logical path helpers
    def basename(self, path: str) -> str:
        return Path(path).name

    def parent(self, path: str) -> str:
        parent = Path(path).parent
        return "" if str(parent) == "." else str(parent)

    def join(self, *parts: str) -> str:
        cleaned = [p.strip("/") for p in parts if p]
        return str(Path(*cleaned)) if cleaned else ""

    # internal conversions
    def _to_key(self, path: str) -> str:
        logical_path = path.strip("/")
        if self.base_prefix:
            return f"{self.base_prefix}/{logical_path}" if logical_path else self.base_prefix
        return logical_path

    def _to_s3_uri(self, path: str) -> str:
        key = self._to_key(path)
        return f"s3://{self.bucket}/{key}"

    def _key_to_logical_path(self, key: str) -> str:
        if self.base_prefix:
            prefix = f"/{base_prefix}"
            if key.startswith(prefix):
                return key[len(prefix):]
            if key == prefix:
                return ""
        else:
            return key

    # high-level storage contract
    def exists(self, path: str) -> bool:
        key = self._to_key(path)
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            code = e.response['Error']['Code']
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def list_paths(self, base_dir: str, pattern: str) -> List[str]:
        prefix = self._to_key(base_dir).rstrip("/") + "/"

        paginator = self.s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

        keys: List[str] = []
        for page in pages:
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        if pattern == "dt=*":
            dt_set = {
                key[len(prefix):].split("/")[0]
                for key in keys
                if key[len(prefix):].startswith("dt=")
            }
            return sorted(self.join(base_dir, dt) for dt in dt_set)

        if pattern == "*.jsonl":
            return sorted(
                self._key_to_logical_path(key)
                for key in keys
                if key.endswith(".jsonl")
            )

        if pattern == "dt=*/part-*.parquet":
            return sorted(
                self._key_to_logical_path(key)
                for key in keys
                if "/dt=" in f"/{key}"
                and key.split("/")[-1].startswith("part-")
                and key.endswith(".parquet")
            )

        raise ValueError(f"Unsupported S3 list_paths pattern: {pattern}")

    def read_jsonl(self, path: str) -> List[dict]:
        key = self._to_key(path)
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        lines = obj["Body"].read().decode("utf-8").splitlines()
        return [json.loads(line) for line in lines]

    def write_jsonl(self, path: str, records: List[dict]) -> None:
        key = self._to_key(path)
        body = "\n".join(json.dumps(r) for r in records)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode("utf-8"),
        )

    def read_parquet(self, path: str) -> pd.DataFrame:
        key = self._to_key(path)
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        return pd.read_parquet(buf)

    def write_parquet(self, path: str, df: pd.DataFrame) -> None:
        key = self._to_key(path)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        buf.seek(0)

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buf.getvalue(),
        )



