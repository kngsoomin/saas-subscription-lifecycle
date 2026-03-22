# keep local adapter for local validation
from pathlib import Path
from typing import List, Union


PathLike = Union[str, Path]


class LocalStorage:
    """
    Local filesystem-backed storage helper
    """
    def basename(self, path: PathLike) -> str:
        """Returns the basename of the path"""
        return Path(path).name

    def parent(self, path: PathLike) -> str:
        """Returns the parent of the path"""
        return str(Path(path).parent)

    def join(self, *parts: str) -> str:
        """Join path parts into a single path string."""
        return str(Path(*parts))

    def list_paths(self, base_dir: PathLike, pattern: str) -> List[str]:
        """Returns a list of paths matching a glob pattern"""
        return sorted(str(p) for p in Path(base_dir).glob(pattern))

    def exists(self, path: PathLike) -> bool:
        """Returns True if the path exists"""
        return Path(path).exists()

    def mkdir(self, path: PathLike) -> None:
        """Creates a directory if it doesn't exist"""
        Path(path).mkdir(parents=True, exist_ok=True)

    def open_text_read(self, path: PathLike, encoding: str = "utf-8"):
        """Open a text file and return its content"""
        return Path(path).open("r", encoding=encoding)

    def open_text_write(self, path: PathLike, encoding: str = "utf-8"):
        """Write contents to a text file"""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open("w", encoding=encoding)