"""
General-purpose file utility: resolve, read, write, move, and delete files
relative to a configurable base path.
"""

from pathlib import Path


_DEFAULT_BASE_PATH = Path(__file__).parent.parent.parent / "out"


class FileUtil:
    def __init__(self, base_path: str = None, encoding: str = "utf-8"):
        self.base_path: Path = Path(base_path) if base_path is not None else _DEFAULT_BASE_PATH
        self.encoding:  str  = encoding

    def resolve_path(self, relative_path: str) -> Path:
        p = Path(relative_path)
        return p if p.is_absolute() else self.base_path / p

    def delete_file(self, relative_path: str) -> None:
        file_path = self.resolve_path(relative_path)
        try:
            file_path.unlink()
        except FileNotFoundError:
            print(f"File not found, skipping delete: {file_path}")

    def move_file(self, src: str, dst: str, overwrite: bool = False) -> None:
        src_path = self.resolve_path(src)
        dst_path = self.resolve_path(dst)
        if dst_path.exists() and not overwrite:
            raise FileExistsError(f"Destination already exists: {dst_path}")
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        src_path.rename(dst_path)

    def write_file(self, relative_path: str, content: str, overwrite: bool = True) -> Path:
        file_path = self.resolve_path(relative_path)
        if file_path.exists() and not overwrite:
            raise FileExistsError(f"File already exists: {file_path}")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(content, encoding=self.encoding)
        return file_path

    def read_file(self, relative_path: str) -> str:
        file_path = self.resolve_path(relative_path)
        return file_path.read_text(encoding=self.encoding)
