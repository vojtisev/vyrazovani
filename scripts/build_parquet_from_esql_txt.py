"""Builder: parse eSQL table dumps (.txt) and write Parquet.

These .txt files have a common structure:
  eSQL>
  select ...
  PLAN (...)

  COL_A   COL_B   COL_C
  =====   =====   =====
  <rows...>

This script:
- robustly finds the header + separator lines,
- parses fixed-width columns (works even when values contain spaces),
- concatenates monthly loan files into one Parquet,
- merges svazky-1 + svazky-2 into one Parquet,
- prefers UTF-8 when valid, otherwise encoding fallbacks (cp1250 / latin-1).
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import duckdb


ENCODINGS: Tuple[str, ...] = ("utf-8", "cp1250", "latin-1")


def _decode_bytes(b: bytes, *, path: Path) -> str:
    """Decode raw dump bytes.

    If the file is valid UTF-8 (e.g. already converted from cp1250), always use UTF-8.
    Otherwise cp1250 and latin-1 can decode UTF-8 bytes into mojibake and wrongly win
    a heuristic score — which corrupts Czech diacritics in titles.
    """
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        pass

    def _score(txt: str) -> Tuple[int, int, int]:
        # Lower is better.
        replacement = txt.count("\ufffd")
        ctrl = sum(1 for ch in txt if ord(ch) < 32 and ch not in ("\n", "\r", "\t"))
        mojibake = sum(txt.count(ch) for ch in ["\x8a", "\x9a", "\x9e", "�", "", "", ""])
        return (replacement, ctrl + mojibake, len(txt))

    best_txt: Optional[str] = None
    best_score: Optional[Tuple[int, int, int]] = None
    for enc in ENCODINGS:
        try:
            cand = b.decode(enc)
            sc = _score(cand)
            if best_score is None or sc < best_score:
                best_txt = cand
                best_score = sc
        except Exception:
            continue
    if best_txt is not None:
        return best_txt
    # As last resort, replace errors to keep pipeline moving.
    return b.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class ParsedTable:
    columns: List[str]
    spans: List[Tuple[int, int]]  # (start, end) per column in the line
    data_start_line: int  # 0-based index of first data row


def _normalize_col_name(name: str) -> str:
    n = name.strip()
    # keep $, but downstream often prefers underscores; normalize gently
    n = n.replace("$", "_")
    n = re.sub(r"__+", "_", n)
    return n.upper()


def _find_table(lines: Sequence[str]) -> ParsedTable:
    # Find the separator line made of '=' and whitespace (spaces/tabs).
    # We use it to locate the header line right above it.
    sep_idx = -1
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        raw = line.rstrip("\n\r")
        # Some exports use tabs; accept any whitespace. Consider it a separator
        # if the non-whitespace characters are only '=' and there are enough of them.
        non_ws = re.sub(r"\s+", "", raw)
        if non_ws and set(non_ws) == {"="} and len(non_ws) >= 5:
            sep_idx = i
            break
    if sep_idx <= 0:
        raise ValueError("Could not find table separator line of '=' characters.")

    header = lines[sep_idx - 1].rstrip("\n\r")
    sep = lines[sep_idx].rstrip("\n\r")

    # Determine column boundaries from the separator (runs of '='). This is more robust
    # than using header token positions and prevents truncation like '2023-..' -> '2-..'.
    starts: List[int] = []
    for m in re.finditer(r"=+", sep):
        starts.append(m.start())
    if not starts:
        raise ValueError("Could not infer column starts from separator line.")

    # Build (start,end) spans based on next start, otherwise line length.
    line_len = max(len(header), len(sep))
    spans: List[Tuple[int, int]] = []
    for idx, s in enumerate(starts):
        e = starts[idx + 1] if idx + 1 < len(starts) else line_len
        spans.append((s, e))

    # Extract column names using spans (keeps multi-token names aligned).
    cols: List[str] = []
    for (s, e) in spans:
        cols.append(_normalize_col_name(header[s:e]))

    # Data starts right after separator line, skipping blank lines.
    j = sep_idx + 1
    while j < len(lines) and not lines[j].strip():
        j += 1
    return ParsedTable(columns=cols, spans=spans, data_start_line=j)


def iter_rows_from_esql_txt(path: Path) -> Iterator[Dict[str, str]]:
    raw = path.read_bytes()
    text = _decode_bytes(raw, path=path)
    lines = text.splitlines()

    table = _find_table(lines)
    cols = table.columns
    spans = table.spans

    for line in lines[table.data_start_line :]:
        if not line.strip():
            continue
        # Stop at common footer / prompt lines (otherwise we may ingest them as data).
        s = line.strip()
        if s.startswith(("Records", "Affected", "Full:", "eSQL>", "out")):
            break
        if "lines not shown" in line:
            continue
        # Fixed-width slice
        values = [line[s:e].strip() for (s, e) in spans]
        if all(v == "" for v in values):
            continue
        # Some exports prefix the first column with spaces; it's fine.
        row = {c: v for c, v in zip(cols, values)}
        yield row


_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$")


def _infer_arrow_type(col: str) -> pa.DataType:
    c = col.upper()
    # Textové sloupce (i když obsahují "ID"/"OPID" v názvu) musí zůstat string,
    # jinak se při castu zahodí (např. OPIDY_POPIS).
    if any(k in c for k in ("POPIS", "TEXT", "NAZEV", "DESKRIPTOR")):
        return pa.string()
    # Keep *_TIME as STRING in staging.
    # Parsing to TIMESTAMP happens later in the enrichment step in DuckDB.
    # This avoids silent data loss if a particular python-side parser misses edge cases.
    if c.endswith("_TIME") or c.endswith("TIME"):
        return pa.string()
    if c.endswith("_DATE") or c == "DATE":
        return pa.timestamp("s")
    # default numeric-like keys (pozor: neplést s *_POPIS apod., viz výše)
    if any(k in c for k in ("_KEY", "_PTR_", "_OPID", "_LEG")) or c.endswith("_OPID") or c == "OPID":
        return pa.int64()
    return pa.string()


def _cast_value(v: str, t: pa.DataType) -> object:
    if v == "":
        return None
    if pa.types.is_int64(t):
        try:
            return int(v)
        except Exception:
            return None
    if pa.types.is_timestamp(t):
        # Common formats observed:
        # - 2025-12-01 07:18:24
        # - 2025-12-01 (rare)
        s = v.strip()
        try:
            if _TS_RE.match(s):
                return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            return datetime.fromisoformat(s)
        except Exception:
            return None
    return v


def write_parquet_from_rows(
    rows: Iterable[Dict[str, str]],
    *,
    output_path: Path,
    chunk_size: int = 200_000,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Avoid accidentally reusing an old schema by appending/partial writes.
    # We always rebuild from scratch.
    if output_path.exists():
        output_path.unlink()

    writer: Optional[pq.ParquetWriter] = None
    buf: List[Dict[str, str]] = []

    def flush() -> None:
        nonlocal writer, buf
        if not buf:
            return
        cols = list(buf[0].keys())
        fields = [pa.field(c, _infer_arrow_type(c)) for c in cols]
        schema = pa.schema(fields)

        arrays: List[pa.Array] = []
        for f in schema:
            t = f.type
            arrays.append(pa.array([_cast_value(r.get(f.name, ""), t) for r in buf], type=t))

        table = pa.Table.from_arrays(arrays, names=cols)
        if writer is None:
            writer = pq.ParquetWriter(str(output_path), table.schema, compression="zstd")
        writer.write_table(table)
        buf = []

    for r in rows:
        buf.append(r)
        if len(buf) >= chunk_size:
            flush()
    flush()
    if writer is not None:
        writer.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", required=True, help="Cesta ke slozce s raw txt soubory")
    ap.add_argument("--out-dir", required=True, help="Kam zapisovat vysledne Parquety")
    ap.add_argument("--chunk-size", type=int, default=200_000, help="Pocet radku na jednu parquet batch")
    args = ap.parse_args()

    raw_dir = Path(args.raw_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Svazky
    sv1 = raw_dir / "svazky-1.txt"
    sv2 = raw_dir / "svazky-2.txt"
    if sv1.exists() and sv2.exists():
        def sv_iter() -> Iterator[Dict[str, str]]:
            yield from iter_rows_from_esql_txt(sv1)
            yield from iter_rows_from_esql_txt(sv2)

        write_parquet_from_rows(
            sv_iter(),
            output_path=out_dir / "svazky.parquet",
            chunk_size=int(args.chunk_size),
        )

    # 2) Vypujcky (monthly)
    loan_files = sorted(raw_dir.glob("vypujcky????-??.txt"))
    if loan_files:
        def loan_iter() -> Iterator[Dict[str, str]]:
            for f in loan_files:
                yield from iter_rows_from_esql_txt(f)

        write_parquet_from_rows(
            loan_iter(),
            output_path=out_dir / "vypujcky.parquet",
            chunk_size=int(args.chunk_size),
        )

    # 2b) TXOCH (texty OCH) — sloučit txoch.txt + txoch2.txt do jednoho Parquetu
    txoch_parts: List[Path] = []
    p_tx1 = raw_dir / "txoch.txt"
    p_tx2 = raw_dir / "txoch2.txt"
    if p_tx1.exists():
        txoch_parts.append(p_tx1)
    if p_tx2.exists():
        txoch_parts.append(p_tx2)
    if txoch_parts:

        def txoch_iter() -> Iterator[Dict[str, str]]:
            for p in txoch_parts:
                yield from iter_rows_from_esql_txt(p)

        write_parquet_from_rows(
            txoch_iter(),
            output_path=out_dir / "txoch.parquet",
            chunk_size=int(args.chunk_size),
        )

    # 3) Tituly + dimenze (pro enrichment)
    simple_tables = {
        "tituly-signatura.txt": "tituly_signatura.parquet",
        "tituly-nazev.txt": "tituly_nazev.parquet",
        "tituly-zahlavi.txt": "tituly_zahlavi.parquet",
        "tituly-rokjazykdruh.txt": "tituly_meta.parquet",
        "opidy.txt": "opidy.parquet",
        "knihovny.txt": "knoddel.parquet",
        "druhdokumentu.txt": "druhdokumentu.parquet",
        "legitky.txt": "legitky.parquet",
        "och.txt": "och.parquet",
        "desk.txt": "desk.parquet",
        "deskt.txt": "deskt.parquet",
        "txdesk.txt": "txdesk.parquet",
    }
    for txt_name, pq_name in simple_tables.items():
        src = raw_dir / txt_name
        if not src.exists():
            continue
        write_parquet_from_rows(
            iter_rows_from_esql_txt(src),
            output_path=out_dir / pq_name,
            chunk_size=int(args.chunk_size),
        )

    # 4) Sloucene tituly do jedne tabulky (join na TITUL_KEY)
    ts = out_dir / "tituly_signatura.parquet"
    tn = out_dir / "tituly_nazev.parquet"
    tz = out_dir / "tituly_zahlavi.parquet"
    tm = out_dir / "tituly_meta.parquet"
    if ts.exists() and tn.exists() and tz.exists() and tm.exists():
        con = duckdb.connect(database=":memory:")
        try:
            query = """
SELECT
  TRY_CAST(s.TITUL_KEY AS BIGINT) AS TITUL_KEY,
  s.TITUL_SIGN_FULL AS TITUL_SIGN_FULL,
  n.TITUL_NAZEV AS TITUL_NAZEV,
  z.TITUL_ZAHLAVI AS TITUL_ZAHLAVI,
  TRY_CAST(m.TITUL_ROK_VYDANI AS BIGINT) AS TITUL_ROK_VYDANI,
  m.TITUL_JAZYK AS TITUL_JAZYK,
  m.TITUL_DRUH_DOKUMENTU AS TITUL_DRUH_DOKUMENTU
FROM read_parquet($ts) s
LEFT JOIN read_parquet($tn) n ON n.TITUL_KEY = s.TITUL_KEY
LEFT JOIN read_parquet($tz) z ON z.TITUL_KEY = s.TITUL_KEY
LEFT JOIN read_parquet($tm) m ON m.TITUL_KEY = s.TITUL_KEY
"""
            out_path = out_dir / "tituly.parquet"
            out_sql = str(out_path).replace("'", "''")
            con.execute(
                f"COPY ({query}) TO '{out_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)",
                {"ts": str(ts), "tn": str(tn), "tz": str(tz), "tm": str(tm)},
            )
        finally:
            con.close()


if __name__ == "__main__":
    main()

