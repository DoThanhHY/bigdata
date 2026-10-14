"""Đọc config/settings.yaml, cho phép ghi đè bằng biến môi trường.

Biến môi trường dạng BDI__<SECTION>__<KEY>, ví dụ:
    BDI__KAFKA__BOOTSTRAP=kafka:9092
    BDI__POSTGRES__HOST=postgres
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = Path(os.environ.get("BDI_CONFIG", ROOT / "config" / "settings.yaml"))


def _coerce(value: str, like: Any) -> Any:
    if isinstance(like, bool):
        return value.lower() in {"1", "true", "yes"}
    if isinstance(like, int):
        return int(value)
    if isinstance(like, float):
        return float(value)
    return value


@lru_cache(maxsize=1)
def settings() -> dict[str, Any]:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    for key, value in os.environ.items():
        if not key.startswith("BDI__"):
            continue
        parts = key[len("BDI__"):].lower().split("__")
        node = cfg
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        node[parts[-1]] = _coerce(value, node.get(parts[-1], ""))
    return cfg


def path(name: str) -> Path:
    """Đường dẫn tuyệt đối cho một mục trong `paths`."""
    p = Path(settings()["paths"][name])
    return p if p.is_absolute() else ROOT / p


def pg_dsn() -> str:
    pg = settings()["postgres"]
    return f"host={pg['host']} port={pg['port']} dbname={pg['db']} user={pg['user']} password={pg['password']}"
