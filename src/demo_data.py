from __future__ import annotations

import csv
import io
import re
import sqlite3
from pathlib import Path
from html import unescape
from html.parser import HTMLParser
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.error import URLError
from urllib.request import Request, urlopen


def ensure_demo_database(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS employers (
                employer_id TEXT PRIMARY KEY,
                fein TEXT NOT NULL,
                employer_name TEXT NOT NULL,
                state TEXT NOT NULL,
                tpa_name TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS liabilities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employer_id TEXT NOT NULL,
                fein TEXT NOT NULL,
                liability_incurred_date TEXT NOT NULL,
                amount_due REAL NOT NULL,
                FOREIGN KEY (employer_id) REFERENCES employers(employer_id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS wage_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employer_id TEXT NOT NULL,
                quarter INTEGER NOT NULL,
                year INTEGER NOT NULL,
                amount_due REAL NOT NULL,
                FOREIGN KEY (employer_id) REFERENCES employers(employer_id)
            )
            """
        )

        cur.execute("SELECT COUNT(*) FROM employers")
        employer_count = int((cur.fetchone() or [0])[0])
        if employer_count == 0:
            cur.executemany(
                """
                INSERT INTO employers (employer_id, fein, employer_name, state, tpa_name)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    ("EMP1001", "12-3456789", "Acme Manufacturing", "KY", "BlueRiver TPA"),
                    ("EMP1002", "98-7654321", "Riverfront Logistics", "KY", "Summit Admin"),
                    ("EMP1003", "11-2233445", "Pioneer Health Group", "TN", "BlueRiver TPA"),
                    ("EMP1004", "44-5566778", "Peak Retail Co", "OH", None),
                ],
            )

        cur.execute("SELECT COUNT(*) FROM liabilities")
        liability_count = int((cur.fetchone() or [0])[0])
        if liability_count == 0:
            cur.executemany(
                """
                INSERT INTO liabilities (employer_id, fein, liability_incurred_date, amount_due)
                VALUES (?, ?, ?, ?)
                """,
                [
                    ("EMP1001", "12-3456789", "2025-01-10", 2200.50),
                    ("EMP1001", "12-3456789", "2025-02-14", 3100.10),
                    ("EMP1002", "98-7654321", "2025-01-09", 900.00),
                    ("EMP1002", "98-7654321", "2025-02-20", 1850.25),
                    ("EMP1003", "11-2233445", "2025-03-02", 4050.00),
                    ("EMP1004", "44-5566778", "2025-03-15", 1200.75),
                ],
            )

        cur.execute("SELECT COUNT(*) FROM wage_reports")
        wage_count = int((cur.fetchone() or [0])[0])
        if wage_count == 0:
            cur.executemany(
                """
                INSERT INTO wage_reports (employer_id, quarter, year, amount_due)
                VALUES (?, ?, ?, ?)
                """,
                [
                    ("EMP1001", 1, 2025, 1980.00),
                    ("EMP1001", 2, 2025, 2405.00),
                    ("EMP1002", 1, 2025, 1100.00),
                    ("EMP1002", 2, 2025, 1400.00),
                    ("EMP1003", 1, 2025, 3899.00),
                    ("EMP1004", 1, 2025, 1000.00),
                ],
            )

        conn.commit()


def _sanitize_identifier(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", (value or "").strip()).strip("_").lower()
    if not cleaned:
        cleaned = fallback
    if cleaned[0].isdigit():
        cleaned = f"c_{cleaned}"
    return cleaned


def _dedupe_names(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in names:
        count = seen.get(name, 0)
        if count == 0:
            out.append(name)
        else:
            out.append(f"{name}_{count + 1}")
        seen[name] = count + 1
    return out


def _fetch_url_bytes(url: str, timeout: int = 20) -> bytes:
    request = Request(
        url.strip(),
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/csv,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://baseballsavant.mlb.com/",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def _looks_like_html(payload: bytes) -> bool:
    snippet = payload.lstrip()[:200].lower()
    return snippet.startswith(b"<!doctype html") or snippet.startswith(b"<html") or b"<body" in snippet


def _candidate_csv_urls(url: str) -> list[str]:
    parsed = urlparse(url.strip())
    if not parsed.scheme or not parsed.netloc:
        return [url.strip()]

    query_items = dict(parse_qsl(parsed.query, keep_blank_values=True))
    candidates: list[str] = []

    if "baseballsavant.mlb.com" in parsed.netloc.lower():
        for key, value in (("csv", "true"), ("format", "csv"), ("download", "1"), ("output", "csv")):
            if query_items.get(key) != value:
                updated = dict(query_items)
                updated[key] = value
                candidates.append(urlunparse(parsed._replace(query=urlencode(updated))))

    candidates.append(url.strip())

    if parsed.path.lower().endswith(".csv"):
        return candidates

    if not parsed.query:
        candidates.append(urlunparse(parsed._replace(query=urlencode({"csv": "true"}))))

    unique_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate not in seen:
            unique_candidates.append(candidate)
            seen.add(candidate)
    return unique_candidates


class _TableHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._current_table: list[list[str]] = []
        self._current_row: list[str] = []
        self._current_cell: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "table":
            self._in_table = True
            self._current_table = []
        elif self._in_table and tag.lower() == "tr":
            self._in_row = True
            self._current_row = []
        elif self._in_row and tag.lower() in {"td", "th"}:
            self._in_cell = True
            self._current_cell = []

    def handle_endtag(self, tag):
        lowered = tag.lower()
        if lowered in {"td", "th"} and self._in_cell:
            self._in_cell = False
            cell_text = unescape("".join(self._current_cell).strip())
            self._current_row.append(cell_text)
            self._current_cell = []
        elif lowered == "tr" and self._in_row:
            self._in_row = False
            if any(cell.strip() for cell in self._current_row):
                self._current_table.append(self._current_row)
            self._current_row = []
        elif lowered == "table" and self._in_table:
            self._in_table = False
            if self._current_table:
                self.tables.append(self._current_table)
            self._current_table = []

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell.append(data)


def _html_table_to_csv_rows(html_text: str) -> tuple[list[str], list[list[str]]]:
    parser = _TableHTMLParser()
    parser.feed(html_text)
    if not parser.tables:
        return [], []

    table = max(parser.tables, key=len)
    if not table:
        return [], []

    header = table[0]
    rows = table[1:] if len(table) > 1 else []

    if not header and rows:
        header = [f"col_{idx + 1}" for idx in range(len(rows[0]))]

    return header, rows


def import_csv_url_to_demo_sqlite(
    db_path: Path,
    csv_url: str,
    table_name: str,
    replace_table: bool = False,
    max_rows: int = 50000,
) -> dict:
    table = _sanitize_identifier(table_name, "public_dataset")
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", table):
        return {"ok": False, "error": "Invalid table name. Use letters, numbers, and underscores."}

    last_error = ""
    raw_bytes = b""
    source_url = csv_url.strip()
    for candidate_url in _candidate_csv_urls(source_url):
        try:
            raw_bytes = _fetch_url_bytes(candidate_url)
            source_url = candidate_url
            break
        except URLError as exc:
            last_error = str(exc)
        except Exception as exc:
            last_error = str(exc)

    if not raw_bytes:
        return {"ok": False, "error": f"Could not download URL: {last_error or 'unknown error'}"}

    decoded = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            decoded = raw_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if decoded is None:
        return {"ok": False, "error": "Unable to decode CSV file content."}

    if _looks_like_html(raw_bytes):
        header, html_rows = _html_table_to_csv_rows(decoded)
        if not header or not html_rows:
            return {
                "ok": False,
                "error": (
                    "That URL looks like a web page, not a direct CSV export. "
                    "For Baseball Savant, use the page's CSV/download link if available."
                ),
            }
        rows = [header] + html_rows
    else:
        reader = csv.reader(io.StringIO(decoded))
        rows = list(reader)
        if not rows:
            return {"ok": False, "error": "CSV file is empty."}

        header = rows[0]
        if not header:
            return {"ok": False, "error": "CSV header row is missing."}

    columns = _dedupe_names([
        _sanitize_identifier(col, f"col_{idx + 1}") for idx, col in enumerate(rows[0])
    ])

    data_rows = rows[1 : max_rows + 1]
    normalized_rows: list[tuple] = []
    col_count = len(columns)
    for row in data_rows:
        fixed = list(row[:col_count])
        if len(fixed) < col_count:
            fixed.extend([""] * (col_count - len(fixed)))
        normalized_rows.append(tuple(fixed))

    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        if replace_table:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')

        column_sql = ", ".join([f'"{c}" TEXT' for c in columns])
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({column_sql})')

        if replace_table:
            cur.execute(f'DELETE FROM "{table}"')

        if normalized_rows:
            placeholders = ", ".join(["?"] * len(columns))
            cur.executemany(
                f'INSERT INTO "{table}" ({", ".join([f"\"{c}\"" for c in columns])}) VALUES ({placeholders})',
                normalized_rows,
            )

        conn.commit()

    return {
        "ok": True,
        "table": table,
        "columns": columns,
        "row_count": len(normalized_rows),
        "max_rows": max_rows,
        "source_url": source_url,
    }


def import_csv_bytes_to_demo_sqlite(
    db_path: Path,
    csv_bytes: bytes,
    table_name: str,
    replace_table: bool = False,
    max_rows: int = 50000,
    source_name: str = "uploaded_file.csv",
) -> dict:
    table = _sanitize_identifier(table_name, "public_dataset")
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", table):
        return {"ok": False, "error": "Invalid table name. Use letters, numbers, and underscores."}

    if not csv_bytes:
        return {"ok": False, "error": "Uploaded file is empty."}

    decoded = None
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            decoded = csv_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if decoded is None:
        return {"ok": False, "error": "Unable to decode uploaded CSV file."}

    reader = csv.reader(io.StringIO(decoded))
    rows = list(reader)
    if not rows:
        return {"ok": False, "error": "CSV file is empty."}

    header = rows[0]
    if not header:
        return {"ok": False, "error": "CSV header row is missing."}

    columns = _dedupe_names([
        _sanitize_identifier(col, f"col_{idx + 1}") for idx, col in enumerate(rows[0])
    ])

    data_rows = rows[1 : max_rows + 1]
    normalized_rows: list[tuple] = []
    col_count = len(columns)
    for row in data_rows:
        fixed = list(row[:col_count])
        if len(fixed) < col_count:
            fixed.extend([""] * (col_count - len(fixed)))
        normalized_rows.append(tuple(fixed))

    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        if replace_table:
            cur.execute(f'DROP TABLE IF EXISTS "{table}"')

        column_sql = ", ".join([f'"{c}" TEXT' for c in columns])
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({column_sql})')

        if replace_table:
            cur.execute(f'DELETE FROM "{table}"')

        if normalized_rows:
            placeholders = ", ".join(["?"] * len(columns))
            cur.executemany(
                f'INSERT INTO "{table}" ({", ".join([f"\"{c}\"" for c in columns])}) VALUES ({placeholders})',
                normalized_rows,
            )

        conn.commit()

    return {
        "ok": True,
        "table": table,
        "columns": columns,
        "row_count": len(normalized_rows),
        "max_rows": max_rows,
        "source_name": source_name,
    }


def list_demo_tables(db_path: Path) -> list[str]:
    if not db_path.exists():
        return []

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [row[0] for row in cur.fetchall()]


def preview_demo_table(db_path: Path, table_name: str, limit: int = 20) -> dict:
    if not db_path.exists():
        return {"ok": False, "error": "Demo database not found."}

    safe_table = _sanitize_identifier(table_name, "")
    if not safe_table:
        return {"ok": False, "error": "Invalid table name."}

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM "{safe_table}" LIMIT ?', (int(limit),))
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description] if cur.description else []

    return {"ok": True, "columns": columns, "rows": rows, "row_count": len(rows), "table": safe_table}
