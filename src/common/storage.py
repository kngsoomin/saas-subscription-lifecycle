# keep local adapter for local validation
from pathlib import Path
from typing import List


class LocalStorage:
    """
    Local filesystem-backed storage helper

    for now -
    - list_paths - replacing .glob
    - exists
    - mkdir
    """

    def list_paths(self, base_dir: str, pattern: str) -> List[Path]:
        """Returns a list of paths matching a glob pattern"""
        return sorted(Path(base_dir).glob(pattern))

    def exists(self, path: str) -> bool:
        """Returns True if the path exists"""
        return Path(path).exists()

    def mkdir(self, path: str) -> None:
        """Creates a directory if it doesn't exist"""
        Path(path).mkdir(parents=True, exist_ok=True)