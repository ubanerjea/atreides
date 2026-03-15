# Run with: python src/scripts/db/mask_db_config.py

import tomllib
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent.parent.parent / "db.toml"


def mask_password(config_path: Path) -> None:
    with open(config_path, "rb") as f:
        content = f.read().decode()
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
    password = config["database"]["password"]
    masked = content.replace(f'"{password}"', f'"{"#" * len(password)}"')
    config_path.write_text(masked)


def main():
    mask_password(CONFIG_PATH)
    print("Password masked in db.toml")


if __name__ == "__main__":
    main()
