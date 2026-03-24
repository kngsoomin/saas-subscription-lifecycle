# keep local adapter for local validation
import json
from pathlib import Path
from typing import List
import pandas as pd
from abc import ABC, abstractmethod



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
    def basename(self, path: str) -> str:
        """Returns the basename of the path"""
        return Path(path).name

    def parent(self, path: str) -> str:
        """Returns the parent of the path"""
        return str(Path(path).parent)

    def join(self, *parts: str) -> str:
        """Join path parts into a single path string."""
        return str(Path(*parts))

    def list_paths(self, base_dir: str, pattern: str) -> List[str]:
        """Returns a list of paths matching a glob pattern"""
        return sorted(str(p) for p in Path(base_dir).glob(pattern))

    def exists(self, path: str) -> bool:
        """Returns True if the path exists"""
        return Path(path).exists()

    def mkdir(self, path: str) -> None:
        """Creates a directory if it doesn't exist"""
        Path(path).mkdir(parents=True, exist_ok=True)

    def open_text_read(self, path: str):
        """Open a text file and return its content"""
        return Path(path).open("r", encoding="utf-8")

    def open_text_write(self, path: str):
        """Write contents to a text file"""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open("w", encoding="utf-8")

    def read_jsonl(self, path: str) -> List[dict]:
        """Read a jsonl file into a list"""
        records = []
        with self.open_text_read(path) as f:
            for line in f:
                records.append(json.loads(line))
            return records

    def write_jsonl(self, path: str, records: List[dict]) -> None:
        """Write records to a jsonl file"""
        with self.open_text_write(path) as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

    def read_parquet(self, path: str) -> pd.DataFrame:
        """Read a parquet file into a dataframe"""
        return pd.read_parquet(path)

    def write_parquet(self, path: str, df: pd.DataFrame) -> None:
        """Write a dataframe to a parquet file"""
        self.mkdir(self.parent(path))
        df.to_parquet(path, index=False)