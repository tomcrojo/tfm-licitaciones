"""Configuration loading and project-root path resolution."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def project_root() -> Path:
    """Return the repository directory containing ``pyproject.toml``."""

    return Path(__file__).resolve().parents[2]


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load the JSON configuration and resolve paths relative to the project."""

    config_path = Path(path) if path else project_root() / "config" / "pipeline.json"
    if not config_path.is_absolute():
        config_path = project_root() / config_path
    with config_path.open(encoding="utf-8") as handle:
        config = json.load(handle)
    config["_config_path"] = str(config_path)
    config["_project_root"] = str(project_root())
    return config


def configured_path(config: dict[str, Any], key: str) -> Path:
    """Resolve one storage path from the loaded configuration."""

    root = Path(config["_project_root"])
    value = Path(config["storage"][key])
    return value if value.is_absolute() else root / value
