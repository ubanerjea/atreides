import tomllib
from pathlib import Path

from sqlalchemy import create_engine, text


def _load_config(config_path: Path | None = None) -> dict:
    if config_path is None:
        config_path = Path(__file__).parent.parent / "db.toml"
    with open(config_path, "rb") as f:
        return tomllib.load(f)["database"]


class DBConnection:
    def __init__(self, config_path: Path | None = None):
        cfg = _load_config(config_path)
        url = (
            f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg['port']}/{cfg['name']}"
        )
        self._engine = create_engine(
            url,
            connect_args={"options": f"-csearch_path={cfg['schema']}"},
        )

    def query(self, sql: str) -> list[tuple]:
        with self._engine.connect() as conn:
            result = conn.execute(text(sql))
            return result.fetchall()
