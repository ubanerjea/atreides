# Run with: python src/mask_db_config.py

import tomllib
from pathlib import Path

config_path = Path(__file__).parent.parent / "db.toml"

with open(config_path, "rb") as f:
    content = f.read().decode()

with open(config_path, "rb") as f:
    config = tomllib.load(f)

password = config["database"]["password"]
masked = content.replace(f'"{password}"', f'"{"#" * len(password)}"')

config_path.write_text(masked)
print("Password masked in db.toml")
