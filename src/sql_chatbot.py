import argparse
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

try:
    import psycopg
except ImportError:
    psycopg = None


@dataclass
class QuerySnippet:
    id: int
    title: str
    sql: str


@dataclass
class QueryIntent:
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    fein: Optional[str] = None
    employer_id: Optional[str] = None
    quarter: Optional[int] = None
    year: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "fein": self.fein,
            "employer_id": self.employer_id,
            "quarter": self.quarter,
            "year": self.year,
        }


@dataclass
class JoinDraft:
    reason: str
    sql: str
    left_table: str
    right_table: str
    join_key: str
    parameters: dict
    confidence: str
    verification: dict

    def to_dict(self) -> dict:
        return {
            "reason": self.reason,
            "sql": self.sql,
            "left_table": self.left_table,
            "right_table": self.right_table,
            "join_key": self.join_key,
            "parameters": self.parameters,
            "confidence": self.confidence,
            "verification": self.verification,
        }


class SQLBibleChatbot:
    def __init__(
        self,
        sql_file: Path,
        max_rows: int = 200,
        database_url: Optional[str] = None,
        allowed_tables: Optional[set[str]] = None,
    ):
        self.sql_file = sql_file
        self.max_rows = max_rows
        self.queries: List[QuerySnippet] = self._load_queries()
        self.client = self._build_openai_client()
        self.database_url = (
            database_url.strip() if isinstance(database_url, str) else os.getenv("DATABASE_URL", "").strip()
        )
        self.allowed_tables = allowed_tables if allowed_tables is not None else self._load_allowed_tables()

    @staticmethod
    def _is_sqlite_url(url: str) -> bool:
        return url.lower().startswith("sqlite:///")

    @staticmethod
    def _sqlite_path_from_url(url: str) -> str:
        return url[len("sqlite:///") :]

    def _connection_mode(self) -> str:
        if not self.database_url:
            return "none"
        if self._is_sqlite_url(self.database_url):
            return "sqlite"
        return "postgres"

    @staticmethod
    def _load_allowed_tables() -> set[str]:
        raw = os.getenv("ALLOWED_TABLES", "").strip()
        if not raw:
            return set()
        return {item.strip().lower() for item in raw.split(",") if item.strip()}

    def _build_openai_client(self):
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key or OpenAI is None:
            return None
        return OpenAI(api_key=api_key)

    def _load_queries(self) -> List[QuerySnippet]:
        text = self.sql_file.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()

        snippets: List[QuerySnippet] = []
        comment_buffer: List[str] = []
        statement_lines: List[str] = []
        in_statement = False

        def is_separator_line(value: str) -> bool:
            # Treat long dashed sections as hard boundaries between query blocks.
            normalized = value.replace(" ", "")
            return len(normalized) >= 8 and all(ch == "-" for ch in normalized)

        def flush_statement():
            nonlocal statement_lines, comment_buffer, snippets
            sql = "\n".join(statement_lines).strip()
            statement_lines = []
            if not sql:
                return
            if not self._starts_with_read_only_statement(sql):
                comment_buffer = []
                return

            title = self._title_from_comments(comment_buffer)
            if not title:
                title = self._infer_title_from_sql(sql)
            if not title:
                title = f"Query {len(snippets) + 1}"
            snippets.append(QuerySnippet(id=len(snippets) + 1, title=title, sql=sql))
            comment_buffer = []

        for raw_line in lines:
            line = raw_line.rstrip()
            stripped = line.strip()

            if is_separator_line(stripped):
                if in_statement and statement_lines:
                    flush_statement()
                    in_statement = False
                comment_buffer = []
                continue

            if not stripped:
                if in_statement and statement_lines:
                    # Blank line often signals the end of ad-hoc single SELECT snippets.
                    flush_statement()
                    in_statement = False
                continue

            if stripped.startswith("--"):
                if in_statement:
                    flush_statement()
                    in_statement = False
                    comment_buffer = [stripped]
                else:
                    comment_buffer.append(stripped)
                continue

            if stripped.startswith("/*") and not in_statement:
                comment_buffer.append(stripped)
                continue

            in_statement = True
            statement_lines.append(line)

            if stripped.endswith(";"):
                flush_statement()
                in_statement = False

        if statement_lines:
            flush_statement()

        return snippets

    @staticmethod
    def _starts_with_read_only_statement(sql: str) -> bool:
        for line in sql.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("--"):
                continue
            lowered = stripped.lower().lstrip("(")
            return bool(re.match(r"^(select|with)\b", lowered))
        return False

    @staticmethod
    def _infer_title_from_sql(sql: str) -> str:
        """Generate a business-focused title describing what the query will find."""
        sql_lower = sql.lower()
        
        # Check for specific business patterns
        if "not exists" in sql_lower and "wage" in sql_lower:
            return "Employers Without Wage Reports"
        
        if "inac" in sql_lower or "status" in sql_lower and "inac" in sql:
            return "Inactive Employers Analysis"
        
        if "delinquent" in sql_lower:
            return "Delinquent Account Summary"
        
        if "tpa" in sql_lower:
            return "TPA Provider Analysis"
        
        if re.search(r"order by.*desc", sql_lower):
            if "liability" in sql_lower or "amount" in sql_lower:
                return "Top Employers by Amount Due"
            if "employer" in sql_lower:
                return "High-Impact Employer Analysis"
            return "High-Value Results"
        
        if "group by" in sql_lower:
            if "quarter" in sql_lower or "year" in sql_lower:
                if "liability" in sql_lower or "wage" in sql_lower:
                    return "Liability & Wage Trends by Period"
                return "Temporal Trend Analysis"
            if "fein" in sql_lower or "employer" in sql_lower:
                return "Summary by Employer"
            return "Grouped Analysis"
        
        if "join" in sql_lower:
            if "liability" in sql_lower and "wage" in sql_lower:
                return "Employer Liability & Wage Report"
            return "Cross-Table Analysis"
        
        if "distinct" in sql_lower:
            return "Unique Records Query"
        
        # Fallback: extract first WHERE condition or main table
        where_match = re.search(r"where\s+([a-z_.\"]+)\s*[=><]", sql_lower)
        if where_match:
            condition = where_match.group(1).strip('\"').split('.')[-1]
            return f"Filtered by {condition.replace('_', ' ').title()}"
        
        match = re.search(r"from\s+([a-z_][a-z0-9_.]*)", sql_lower)
        if match:
            table = match.group(1).split(".")[-1].capitalize()
            return f"{table} Query"
        
        return "Data Query"
    
    @staticmethod
    def _title_from_comments(comments: List[str]) -> str:
        cleaned = []
        for c in comments:
            t = re.sub(r"^-+", "", c.replace("--", "")).strip(" -\t")
            if t:
                cleaned.append(t)
        if not cleaned:
            return ""
        title = " ".join(cleaned)
        return title[:120]

    @staticmethod
    def extract_intent(question: str) -> QueryIntent:
        intent = QueryIntent()
        lowered = question.lower()

        date_matches = re.findall(r"\b(20\d{2}-\d{2}-\d{2})\b", question)
        if len(date_matches) >= 2:
            intent.start_date, intent.end_date = date_matches[0], date_matches[1]
        elif len(date_matches) == 1:
            if "from" in lowered or "start" in lowered:
                intent.start_date = date_matches[0]
            else:
                intent.end_date = date_matches[0]

        fein_match = re.search(r"\b(\d{2}-?\d{7})\b", question)
        if fein_match:
            raw_fein = fein_match.group(1).replace("-", "")
            if len(raw_fein) == 9:
                intent.fein = f"{raw_fein[:2]}-{raw_fein[2:]}"

        employer_match = re.search(
            r"\b(?:employer|emp)\s*id\b[:#\s-]*([A-Za-z0-9_-]+)",
            question,
            re.IGNORECASE,
        )
        if employer_match:
            intent.employer_id = employer_match.group(1).strip()

        quarter_match = re.search(r"\b(?:q|quarter)\s*([1-4])\b", lowered)
        if quarter_match:
            intent.quarter = int(quarter_match.group(1))

        year_match = re.search(r"\b(20\d{2})\b", question)
        if year_match:
            intent.year = int(year_match.group(1))

        return intent

    @staticmethod
    def _apply_intent_override(base: QueryIntent, override: Optional[dict]) -> QueryIntent:
        if not override:
            return base

        for key in ("start_date", "end_date", "fein", "employer_id"):
            value = (override.get(key) or "").strip() if isinstance(override, dict) else ""
            if value:
                setattr(base, key, value)

        if isinstance(override, dict):
            quarter_value = override.get("quarter")
            year_value = override.get("year")
            if quarter_value not in (None, ""):
                try:
                    parsed_quarter = int(str(quarter_value).strip())
                    if 1 <= parsed_quarter <= 4:
                        base.quarter = parsed_quarter
                except ValueError:
                    pass
            if year_value not in (None, ""):
                try:
                    parsed_year = int(str(year_value).strip())
                    if 1900 <= parsed_year <= 2100:
                        base.year = parsed_year
                except ValueError:
                    pass
        return base

    @staticmethod
    def _intent_to_terms(intent: QueryIntent) -> List[str]:
        terms: List[str] = []
        if intent.start_date:
            terms.extend([intent.start_date, "start_date", "date"])
        if intent.end_date:
            terms.extend([intent.end_date, "end_date", "date"])
        if intent.fein:
            terms.extend([intent.fein, intent.fein.replace("-", ""), "fein"])
        if intent.employer_id:
            terms.extend([intent.employer_id, "employer", "employer_id"])
        if intent.quarter:
            terms.extend([f"q{intent.quarter}", "quarter"])
        if intent.year:
            terms.extend([str(intent.year), "year"])
        return terms

    @staticmethod
    def _has_any(text: str, words: List[str]) -> bool:
        lowered = text.lower()
        return any(w in lowered for w in words)

    def _question_needs_join_draft(self, question: str) -> bool:
        q = question.lower()
        asks_join = self._has_any(q, ["join", "both", "also", "together", "combined"])
        mentions_liability = self._has_any(q, ["liability", "liable", "liabilities"])
        mentions_wage = self._has_any(q, ["wage", "wages", "payroll", "wage report"])
        return asks_join or (mentions_liability and mentions_wage)

    def _infer_domain_tables(self, candidates: List[QuerySnippet]) -> dict:
        domain_map = {
            "liability": ["liability", "liable", "liabilities"],
            "wage": ["wage", "wages", "payroll", "wage_report"],
        }
        found: dict = {"liability": [], "wage": []}

        for c in candidates:
            text = f"{c.title}\n{c.sql}".lower()
            refs = self._extract_table_references(c.sql)
            for domain, words in domain_map.items():
                if self._has_any(text, words):
                    found[domain].extend(refs)

        for domain in found:
            unique = []
            for t in found[domain]:
                if t not in unique:
                    unique.append(t)
            found[domain] = unique

        return found

    @staticmethod
    def _guess_join_key(intent: QueryIntent) -> str:
        if intent.employer_id:
            return "employer_id"
        if intent.fein:
            return "fein"
        return "employer_id"

    def _build_join_sql(
        self,
        left_table: str,
        right_table: str,
        join_key: str,
        intent: QueryIntent,
    ) -> str:
        where_lines = ["WHERE 1=1"]
        if intent.start_date:
            where_lines.append("  AND l.liability_incurred_date >= %(start_date)s")
        if intent.end_date:
            where_lines.append("  AND l.liability_incurred_date <= %(end_date)s")
        if intent.employer_id:
            where_lines.append(f"  AND l.{join_key} = %(employer_id)s")
        if intent.fein:
            where_lines.append("  AND l.fein = %(fein)s")
        where_lines.append("  AND w.quarter = %(quarter)s")
        where_lines.append("  AND w.year = %(year)s")

        where_sql = "\n".join(where_lines)

        return (
            "-- Draft JOIN query generated from your question\n"
            "-- Verify table/column names in your database before running\n"
            f"SELECT\n"
            f"  l.{join_key} AS employer_key,\n"
            "  l.liability_incurred_date,\n"
            "  w.quarter,\n"
            "  w.year,\n"
            "  w.amount_due AS wage_amount_due\n"
            f"FROM {left_table} l\n"
            f"JOIN {right_table} w\n"
            f"  ON l.{join_key} = w.{join_key}\n"
            f"{where_sql}\n"
            f"LIMIT {self.max_rows};"
        )

    @staticmethod
    def _build_join_parameters(intent: QueryIntent) -> dict:
        return {
            "start_date": intent.start_date,
            "end_date": intent.end_date,
            "fein": intent.fein,
            "employer_id": intent.employer_id,
            "quarter": intent.quarter if intent.quarter else "<set-quarter-1-4>",
            "year": intent.year if intent.year else "<set-year-YYYY>",
        }

    @staticmethod
    def _join_confidence(left_table: str, right_table: str, intent: QueryIntent) -> str:
        score = 0
        if left_table and right_table and left_table != right_table:
            score += 1
        if intent.quarter:
            score += 1
        if intent.year:
            score += 1
        if intent.employer_id or intent.fein:
            score += 1
        if score >= 4:
            return "high"
        if score >= 2:
            return "medium"
        return "low"

    @staticmethod
    def _split_table_name(table_name: str) -> tuple[str, str]:
        parts = [p for p in table_name.split(".") if p]
        if len(parts) >= 2:
            return parts[0], parts[-1]
        return "public", table_name

    def _table_has_column(self, table_name: str, column_name: str) -> Optional[bool]:
        if not self.database_url:
            return None

        mode = self._connection_mode()
        schema_name, simple_table = self._split_table_name(table_name)

        if mode == "sqlite":
            try:
                sqlite_path = self._sqlite_path_from_url(self.database_url)
                with sqlite3.connect(sqlite_path) as conn:
                    cur = conn.cursor()
                    cur.execute(f"PRAGMA table_info({simple_table})")
                    cols = cur.fetchall()
                    known_cols = {str(row[1]).lower() for row in cols}
                    return column_name.lower() in known_cols
            except Exception:
                return None

        if psycopg is None:
            return None

        try:
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = %s
                          AND table_name = %s
                          AND column_name = %s
                        LIMIT 1
                        """,
                        (schema_name, simple_table, column_name),
                    )
                    return cur.fetchone() is not None
        except Exception:
            return None

    def verify_join_key(self, left_table: str, right_table: str, join_key: str) -> dict:
        left_has = self._table_has_column(left_table, join_key)
        right_has = self._table_has_column(right_table, join_key)

        if left_has is None or right_has is None:
            return {
                "status": "not_checked",
                "message": "Schema verification not available (DATABASE_URL missing or metadata query failed).",
                "left_has_join_key": left_has,
                "right_has_join_key": right_has,
            }

        if left_has and right_has:
            return {
                "status": "verified",
                "message": "Join key exists in both tables.",
                "left_has_join_key": True,
                "right_has_join_key": True,
            }

        missing = []
        if not left_has:
            missing.append(f"{left_table}.{join_key}")
        if not right_has:
            missing.append(f"{right_table}.{join_key}")

        return {
            "status": "failed",
            "message": "Join key missing in: " + ", ".join(missing),
            "left_has_join_key": left_has,
            "right_has_join_key": right_has,
        }

    def build_join_draft(
        self,
        question: str,
        intent: QueryIntent,
        candidates: List[QuerySnippet],
    ) -> Optional[JoinDraft]:
        if not self._question_needs_join_draft(question):
            return None

        inferred = self._infer_domain_tables(candidates)
        liability_tables = inferred.get("liability") or []
        wage_tables = inferred.get("wage") or []

        if not liability_tables or not wage_tables:
            return None

        left_table = liability_tables[0]
        right_table = wage_tables[0]
        if left_table == right_table and len(wage_tables) > 1:
            right_table = wage_tables[1]

        if left_table == right_table:
            return None

        join_key = self._guess_join_key(intent)
        sql = self._build_join_sql(
            left_table=left_table,
            right_table=right_table,
            join_key=join_key,
            intent=intent,
        )
        parameters = self._build_join_parameters(intent)
        confidence = self._join_confidence(left_table, right_table, intent)
        verification = self.verify_join_key(left_table, right_table, join_key)
        reason = (
            "Detected a multi-table question (liability + wage context), so generated "
            "a parameterized JOIN draft you can adapt safely."
        )
        return JoinDraft(
            reason=reason,
            sql=sql,
            left_table=left_table,
            right_table=right_table,
            join_key=join_key,
            parameters=parameters,
            confidence=confidence,
            verification=verification,
        )

    def search_queries(
        self,
        question: str,
        top_n: int = 5,
        intent: Optional[QueryIntent] = None,
    ) -> List[QuerySnippet]:
        tokens = set(re.findall(r"[a-zA-Z0-9_]+", question.lower()))
        if intent:
            tokens.update(re.findall(r"[a-zA-Z0-9_]+", " ".join(self._intent_to_terms(intent)).lower()))

        scored = []
        for q in self.queries:
            haystack = f"{q.title}\n{q.sql[:1000]}".lower()
            score = sum(1 for t in tokens if t in haystack)
            if score > 0:
                scored.append((score, q))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [q for _, q in scored[:top_n]]

    def suggest_with_llm(
        self,
        question: str,
        candidates: List[QuerySnippet],
        intent: Optional[QueryIntent] = None,
    ) -> Optional[dict]:
        if self.client is None:
            return None

        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
        candidate_payload = [
            {"id": c.id, "title": c.title, "sql": c.sql[:2000]} for c in candidates
        ]

        system = (
            "You are a SQL assistant. Choose the best query template(s) from candidates or "
            "produce a safe read-only SQL statement. Only output valid JSON."
        )
        user = {
            "question": question,
            "extracted_intent": intent.to_dict() if intent else {},
            "instructions": [
                "Return JSON with keys: action, reason, sql, candidate_ids.",
                "action must be one of: suggest_only, run_query.",
                "Use read-only SQL only (SELECT/WITH).",
                "If uncertain, use suggest_only and provide candidate_ids.",
            ],
            "candidates": candidate_payload,
        }

        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(user)},
                ],
                temperature=0.1,
            )
            raw = response.choices[0].message.content or "{}"
            return json.loads(raw)
        except Exception:
            return None

    @staticmethod
    def _is_read_only_sql(sql: str) -> bool:
        sql_clean = sql.strip().lstrip("(")
        if not sql_clean:
            return False

        forbidden = ["insert", "update", "delete", "drop", "truncate", "alter", "create"]
        lowered = sql_clean.lower()
        if any(re.search(rf"\b{kw}\b", lowered) for kw in forbidden):
            return False

        return bool(re.match(r"^(select|with)\b", lowered))

    @staticmethod
    def _extract_table_references(sql: str) -> List[str]:
        pattern = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][\w\.]*)", re.IGNORECASE)
        return [m.group(1).strip().strip('"').lower() for m in pattern.finditer(sql)]

    def _check_allowed_tables(self, sql: str) -> tuple[bool, List[str]]:
        if not self.allowed_tables:
            return True, []

        disallowed: List[str] = []
        for table in self._extract_table_references(sql):
            base = table.split(".")[-1]
            if table not in self.allowed_tables and base not in self.allowed_tables:
                disallowed.append(table)

        unique_disallowed = sorted(set(disallowed))
        return len(unique_disallowed) == 0, unique_disallowed

    @staticmethod
    def _is_missing_param_value(value) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            v = value.strip()
            return v == "" or v.startswith("<set-")
        return False

    def _normalize_join_parameters(self, params: dict) -> tuple[bool, Optional[dict], str]:
        required = ("quarter", "year")
        for key in required:
            if key not in params or self._is_missing_param_value(params[key]):
                return False, None, f"Missing required JOIN parameter: {key}"

        normalized = dict(params)

        try:
            normalized["quarter"] = int(str(normalized["quarter"]).strip())
        except Exception:
            return False, None, "Invalid quarter. Use an integer from 1 to 4."

        if normalized["quarter"] < 1 or normalized["quarter"] > 4:
            return False, None, "Invalid quarter. Use an integer from 1 to 4."

        try:
            normalized["year"] = int(str(normalized["year"]).strip())
        except Exception:
            return False, None, "Invalid year. Use a 4-digit year like 2025."

        if normalized["year"] < 1900 or normalized["year"] > 2100:
            return False, None, "Invalid year. Use a value between 1900 and 2100."

        for key in ("start_date", "end_date", "fein", "employer_id"):
            if key in normalized and isinstance(normalized[key], str):
                normalized[key] = normalized[key].strip() or None

        return True, normalized, ""

    @staticmethod
    def _adapt_params_for_sqlite(params: Optional[dict]) -> dict:
        if not isinstance(params, dict):
            return {}
        return {str(k): v for k, v in params.items()}

    @staticmethod
    def _adapt_named_placeholders_for_sqlite(sql: str) -> str:
        # Convert psycopg style %(name)s placeholders to sqlite style :name placeholders.
        return re.sub(r"%\((\w+)\)s", r":\1", sql)

    def probe_connection(self) -> dict:
        if not self.database_url:
            return {"ok": False, "mode": "none", "message": "No database configured."}

        mode = self._connection_mode()
        if mode == "sqlite":
            try:
                sqlite_path = self._sqlite_path_from_url(self.database_url)
                with sqlite3.connect(sqlite_path) as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table'")
                    table_count = int((cur.fetchone() or [0])[0])
                return {
                    "ok": True,
                    "mode": "sqlite",
                    "message": f"SQLite connected ({table_count} tables discovered).",
                    "table_count": table_count,
                }
            except Exception as exc:
                return {"ok": False, "mode": "sqlite", "message": f"SQLite connection failed: {exc}"}

        if psycopg is None:
            return {
                "ok": False,
                "mode": "postgres",
                "message": "psycopg is not installed. Run: pip install -r requirements.txt",
            }

        try:
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT count(*)
                        FROM information_schema.tables
                        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                        """
                    )
                    table_count = int((cur.fetchone() or [0])[0])
            return {
                "ok": True,
                "mode": "postgres",
                "message": f"Postgres connected ({table_count} tables discovered).",
                "table_count": table_count,
            }
        except Exception as exc:
            return {"ok": False, "mode": "postgres", "message": f"Postgres connection failed: {exc}"}

    def run_query_with_params(self, sql: str, params: dict) -> dict:
        if not self._is_read_only_sql(sql):
            return {"ok": False, "error": "Only read-only SELECT/WITH SQL is allowed."}

        tables_ok, disallowed_tables = self._check_allowed_tables(sql)
        if not tables_ok:
            return {
                "ok": False,
                "error": "Query references table(s) not in ALLOWED_TABLES: "
                + ", ".join(disallowed_tables),
            }

        if not self.database_url:
            return {"ok": False, "error": "DATABASE_URL is not set."}

        mode = self._connection_mode()
        if mode == "sqlite":
            try:
                sqlite_path = self._sqlite_path_from_url(self.database_url)
                sqlite_sql = self._adapt_named_placeholders_for_sqlite(sql)
                with sqlite3.connect(sqlite_path) as conn:
                    cur = conn.cursor()
                    cur.execute(sqlite_sql, self._adapt_params_for_sqlite(params))
                    rows = cur.fetchall()
                    cols = [d[0] for d in cur.description] if cur.description else []
                return {"ok": True, "columns": cols, "rows": rows, "row_count": len(rows)}
            except Exception as exc:
                return {"ok": False, "error": str(exc)}

        if psycopg is None:
            return {"ok": False, "error": "psycopg is not installed. Run: pip install -r requirements.txt"}

        try:
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)  # type: ignore[arg-type]
                    cols = [d.name for d in cur.description] if cur.description else []
                    rows = cur.fetchall() if cols else []
            return {"ok": True, "columns": cols, "rows": rows, "row_count": len(rows)}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def execute_join_draft(self, join_draft: dict, override_params: Optional[dict] = None) -> dict:
        if not join_draft:
            return {"ok": False, "error": "No join draft found to execute."}

        sql = (join_draft.get("sql") or "").strip()
        if not sql:
            return {"ok": False, "error": "Join draft SQL is empty."}

        merged_params = dict(join_draft.get("parameters") or {})
        if isinstance(override_params, dict):
            for key, value in override_params.items():
                if key in merged_params:
                    merged_params[key] = value

        verification = join_draft.get("verification") or {}
        if verification.get("status") == "failed":
            return {
                "ok": False,
                "error": "Join draft failed schema verification: " + verification.get("message", "unknown"),
            }

        ok, normalized, error = self._normalize_join_parameters(merged_params)
        if not ok:
            return {"ok": False, "error": error}

        return self.run_query_with_params(sql, normalized or {})

    def run_query(self, sql: str) -> dict:
        if not self._is_read_only_sql(sql):
            return {"ok": False, "error": "Only read-only SELECT/WITH SQL is allowed."}

        tables_ok, disallowed_tables = self._check_allowed_tables(sql)
        if not tables_ok:
            return {
                "ok": False,
                "error": "Query references table(s) not in ALLOWED_TABLES: "
                + ", ".join(disallowed_tables),
            }

        if not self.database_url:
            return {"ok": False, "error": "DATABASE_URL is not set."}

        safe_sql = sql.strip().rstrip(";") + f"\nLIMIT {self.max_rows};"

        mode = self._connection_mode()
        if mode == "sqlite":
            try:
                sqlite_path = self._sqlite_path_from_url(self.database_url)
                with sqlite3.connect(sqlite_path) as conn:
                    cur = conn.cursor()
                    cur.execute(safe_sql)
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = cur.fetchall() if cols else []
                return {"ok": True, "columns": cols, "rows": rows, "row_count": len(rows)}
            except Exception as exc:
                return {"ok": False, "error": str(exc)}

        if psycopg is None:
            return {"ok": False, "error": "psycopg is not installed. Run: pip install -r requirements.txt"}

        try:
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(safe_sql)  # type: ignore[arg-type]
                    cols = [d.name for d in cur.description] if cur.description else []
                    rows = cur.fetchall() if cols else []
            return {"ok": True, "columns": cols, "rows": rows, "row_count": len(rows)}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def answer(self, question: str, intent_override: Optional[dict] = None) -> dict:
        intent = self.extract_intent(question)
        intent = self._apply_intent_override(intent, intent_override)

        candidates = self.search_queries(question, top_n=5, intent=intent)
        join_draft = self.build_join_draft(question, intent, candidates)
        llm_plan = self.suggest_with_llm(question, candidates, intent=intent)

        if llm_plan:
            action = llm_plan.get("action", "suggest_only")
            sql = (llm_plan.get("sql") or "").strip()
            candidate_ids = llm_plan.get("candidate_ids") or []

            if action == "run_query" and sql:
                return {
                    "mode": "llm_proposed_query",
                    "plan": llm_plan,
                    "proposed_sql": sql,
                    "intent": intent.to_dict(),
                    "join_draft": join_draft.to_dict() if join_draft else None,
                    "fallback_candidates": [self._q_to_dict(q) for q in candidates],
                }

            chosen = [q for q in self.queries if q.id in candidate_ids][:5]
            suggestions = chosen if chosen else candidates
            return {
                "mode": "llm_suggest",
                "plan": llm_plan,
                "intent": intent.to_dict(),
                "join_draft": join_draft.to_dict() if join_draft else None,
                "suggestions": [self._q_to_dict(q) for q in suggestions],
            }

        return {
            "mode": "join_draft_suggest" if join_draft else "keyword_suggest",
            "intent": intent.to_dict(),
            "join_draft": join_draft.to_dict() if join_draft else None,
            "suggestions": [self._q_to_dict(q) for q in candidates],
        }

    @staticmethod
    def _q_to_dict(q: QuerySnippet) -> dict:
        return {"id": q.id, "title": q.title, "sql_preview": q.sql[:500]}


def print_result(payload: dict):
    mode = payload.get("mode", "unknown")
    print(f"\nMode: {mode}")

    intent = payload.get("intent") or {}
    if any(intent.values()):
        print("Extracted intent:")
        print(json.dumps(intent, indent=2))

    if "plan" in payload:
        print("LLM Plan:")
        print(json.dumps(payload["plan"], indent=2))

    join_draft = payload.get("join_draft")
    if join_draft:
        print("\nJOIN Draft:")
        print(join_draft.get("reason", ""))
        print(
            f"Tables: {join_draft.get('left_table')} JOIN {join_draft.get('right_table')} "
            f"ON {join_draft.get('join_key')}"
        )
        print(f"Confidence: {join_draft.get('confidence', 'unknown')}")
        verification = join_draft.get("verification") or {}
        if verification:
            print(f"Schema verification: {verification.get('status', 'unknown')}")
            print(verification.get("message", ""))
        print("Parameters:")
        print(json.dumps(join_draft.get("parameters", {}), indent=2))
        print(join_draft.get("sql", ""))

    if "result" in payload:
        result = payload["result"]
        if not result.get("ok"):
            print(f"\nQuery execution error: {result.get('error')}")
            return

        cols = result.get("columns", [])
        rows = result.get("rows", [])
        print(f"\nRows returned: {result.get('row_count', 0)}")
        if cols:
            print(" | ".join(cols))
            print("-" * min(140, max(10, len(" | ".join(cols)))))
            for row in rows[:50]:
                print(" | ".join(str(v) for v in row))
            if len(rows) > 50:
                print(f"... truncated {len(rows) - 50} additional row(s)")

    suggestions = payload.get("suggestions") or payload.get("fallback_candidates") or []
    if suggestions:
        print("\nSuggested queries:")
        for s in suggestions:
            print(f"[{s['id']}] {s['title']}")
            print(s["sql_preview"].replace("\n", " ")[:220] + "...")


def main():
    parser = argparse.ArgumentParser(description="SQL Bible chatbot prototype")
    parser.add_argument(
        "--sql-file",
        default="data/SQL_BIBLE_PRIME.sql",
        help="Path to SQL file containing query snippets.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=200,
        help="Max rows returned for executed queries.",
    )
    args = parser.parse_args()

    load_dotenv()

    bot = SQLBibleChatbot(sql_file=Path(args.sql_file), max_rows=args.max_rows)
    print(f"Loaded {len(bot.queries)} query snippets from {args.sql_file}.")
    print("Type your question, or '/list', '/show <id>', '/quit'.")

    while True:
        try:
            question = input("\nYou: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break

        if not question:
            continue
        if question.lower() in {"/q", "/quit", "quit", "exit"}:
            print("Bye.")
            break
        if question.lower() == "/list":
            for q in bot.queries[:100]:
                print(f"[{q.id}] {q.title}")
            continue

        if question.lower().startswith("/show "):
            try:
                query_id = int(question.split()[1])
                match = next((q for q in bot.queries if q.id == query_id), None)
                if not match:
                    print("No query found for that id.")
                else:
                    print(f"\n[{match.id}] {match.title}\n")
                    print(match.sql)
            except Exception:
                print("Usage: /show <id>")
            continue

        payload = bot.answer(question)
        print_result(payload)


if __name__ == "__main__":
    main()
