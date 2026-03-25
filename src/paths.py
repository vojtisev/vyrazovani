"""Cesty relativni ke koreni projektu (slozka s app.py) – spolehlive i pri spusteni z jineho CWD."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from .config import DEFAULT_PARQUET_PATH


def project_root() -> Path:
    """Koren projektu (adresar, kde lezi app.py)."""
    return Path(__file__).resolve().parent.parent


def _is_http_url(s: str) -> bool:
    t = s.strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _is_s3_uri(s: str) -> bool:
    return s.strip().lower().startswith("s3://")


def find_default_parquet_in_project() -> Optional[Path]:
    """Najde v koreni projektu vhodny Parquet pro vychozi analyzu.

    Poradi:
    1. Presny nazev z configu (DEFAULT_PARQUET_PATH)
    2. Stejny nazev bez ohledu na velikost pismen (napr. .Parquet)
    3. Jeden soubor, v jehoz nazvu je „vypujcky“ i „enriched“ (case-insensitive)
    4. Pokud je v koreni projektu prave jeden *.parquet, pouzije se
    """
    root = project_root()
    expected = (root / DEFAULT_PARQUET_PATH).resolve()
    if expected.is_file():
        return expected

    target_lower = DEFAULT_PARQUET_PATH.lower()
    for path in root.iterdir():
        if path.is_file() and path.name.lower() == target_lower:
            return path.resolve()

    fuzzy: list[Path] = []
    for path in root.iterdir():
        if not path.is_file() or path.suffix.lower() != ".parquet":
            continue
        n = path.name.lower()
        if "vypujcky" in n and "enriched" in n:
            fuzzy.append(path.resolve())
    if len(fuzzy) == 1:
        return fuzzy[0]
    if len(fuzzy) > 1:
        return None

    all_pq = [p.resolve() for p in root.iterdir() if p.is_file() and p.suffix.lower() == ".parquet"]
    if len(all_pq) == 1:
        return all_pq[0]
    return None


def resolve_parquet_path_for_load(parquet_path: str) -> str:
    """Pro nacteni: relativni cesty jsou vzdy ke koreni projektu, ne k aktualnimu CWD."""
    raw = parquet_path.strip()
    if not raw:
        return raw
    if _is_http_url(raw) or _is_s3_uri(raw):
        return raw
    p = Path(raw).expanduser()
    if p.is_absolute():
        return str(p.resolve())
    return str((project_root() / raw).resolve())
