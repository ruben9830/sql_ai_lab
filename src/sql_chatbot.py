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
            if not title or self._is_generic_title(title):
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
        
        # Specific business patterns
        if "not exists" in sql_lower and "wage" in sql_lower:
            return "Employers Without Wage Reports"
        
        if "inac" in sql_lower or ("status" in sql_lower and "inac" in sql):
            return "Inactive Employers Analysis"
        
        if "delinquent" in sql_lower:
            return "Delinquent Account Summary"
        
        if "tpa" in sql_lower:
            return "TPA Provider Analysis"
        
        # Date range BETWEEN patterns
        if "between" in sql_lower:
            if "rgst_dt" in sql_lower or "registration" in sql_lower:
                return "Employers Registered in Date Range"
            if "incurred" in sql_lower or "liability" in sql_lower:
                return "Liabilities Incurred in Date Range"
            if "wage" in sql_lower or "rpt_" in sql_lower:
                return "Wage Reports in Period"
            return "Records in Date Range"
        
        # Date-based WHERE conditions
        if "rgst_dt" in sql_lower:
            return "Employers by Registration Date"
        
        if "incurred" in sql_lower:
            if "liability" in sql_lower:
                return "Liability Amounts by Incurrence Date"
            return "Records by Incurred Date"
        
        if "due_dt" in sql_lower or "due_date" in sql_lower or "due date" in sql_lower:
            return "Items Due by Payment Date"
        
        # Sorting patterns
        if re.search(r"order by.*desc", sql_lower):
            if "liability" in sql_lower or "amount" in sql_lower:
                return "Top Employers by Amount Due"
            if "count" in sql_lower:
                return "Rankings by Count"
            if "employer" in sql_lower:
                return "High-Impact Employer Analysis"
            return "High-Value Results"
        
        # Grouping patterns
        if "group by" in sql_lower:
            if "quarter" in sql_lower or "year" in sql_lower or "rpt_" in sql_lower:
                if "liability" in sql_lower or "wage" in sql_lower:
                    return "Liability & Wage Trends by Period"
                return "Temporal Trend Analysis"
            if "fein" in sql_lower or "employer" in sql_lower:
                return "Summary by Employer"
            if "status" in sql_lower:
                return "Distribution by Status"
            return "Grouped Analysis"
        
        # Join patterns
        if "join" in sql_lower:
            if "liability" in sql_lower and "wage" in sql_lower:
                return "Employer Liability & Wage Report"
            return "Cross-Table Analysis"
        
        if "distinct" in sql_lower:
            return "Unique Records Query"
        
        if "union" in sql_lower:
            return "Combined Data Query"
        
        # Fallback: extract WHERE condition or main table
        where_match = re.search(r"where\s+([a-z_.\"]+)", sql_lower)
        if where_match:
            condition = where_match.group(1).strip('\"').split('.')[-1].replace('_', ' ').title()
            if condition:
                return f"Filtered by {condition}"
        
        match = re.search(r"from\s+([a-z_][a-z0-9_.]*)", sql_lower)
        if match:
            table = match.group(1).split(".")[-1].replace('_', ' ').title()
            return f"{table} Data"
        
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
        title = re.sub(r"\b(?:run\s+(?:in|on)|run\s+ub)\s+(?:taxpresit|commonpresit)\b", "", title, flags=re.IGNORECASE)
        title = re.sub(r"\b(?:taxpresit|commonpresit)\b", "", title, flags=re.IGNORECASE)
        title = re.sub(r"\s+", " ", title).strip(" -:\t")
        return title[:120]

    @staticmethod
    def _is_generic_title(title: str) -> bool:
        t = (title or "").strip().lower()
        if not t:
            return True
        return bool(
            re.fullmatch(r"query\s*\d*", t)
            or re.fullmatch(r"sql\s*template\s*\d*", t)
            or re.fullmatch(r"template\s*\d*", t)
        )

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

    @staticmethod
    def _extract_cte_names(sql: str) -> List[str]:
        lowered = sql.lstrip()
        if not lowered.lower().startswith("with "):
            return []

        names: List[str] = []
        remainder = lowered[4:]
        pattern = re.compile(r"\s*([a-zA-Z_][\w]*)\s+as\s*\(", re.IGNORECASE)
        idx = 0
        while idx < len(remainder):
            match = pattern.match(remainder, idx)
            if not match:
                break
            names.append(match.group(1).lower())
            idx = match.end()
            depth = 1
            while idx < len(remainder) and depth > 0:
                ch = remainder[idx]
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                idx += 1
            while idx < len(remainder) and remainder[idx] in " \t\r\n,":
                idx += 1
            if idx < len(remainder) and remainder[idx:idx + 6].lower() == "select":
                break
        return names

    def _check_allowed_tables(self, sql: str) -> tuple[bool, List[str]]:
        if not self.allowed_tables:
            return True, []

        disallowed: List[str] = []
        cte_names = set(self._extract_cte_names(sql))
        for table in self._extract_table_references(sql):
            base = table.split(".")[-1]
            if base in cte_names:
                continue
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

    def _sqlite_table_columns(self, table_name: str) -> List[str]:
        if not self.database_url or not self._is_sqlite_url(self.database_url):
            return []

        try:
            sqlite_path = self._sqlite_path_from_url(self.database_url)
            with sqlite3.connect(sqlite_path) as conn:
                cur = conn.cursor()
                cur.execute(f'PRAGMA table_info("{table_name}")')
                return [str(row[1]) for row in cur.fetchall() if row and len(row) > 1]
        except Exception:
            return []

    @staticmethod
    def _quote_sqlite_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    @staticmethod
    def _try_float(value) -> Optional[float]:
        if value is None:
            return None
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        if text.endswith("%"):
            text = text[:-1]
        try:
            return float(text)
        except Exception:
            return None

    @staticmethod
    def _find_first_column(columns: List[str], candidates: List[str]) -> Optional[str]:
        by_lower = {c.lower(): c for c in columns}
        for candidate in candidates:
            if candidate.lower() in by_lower:
                return by_lower[candidate.lower()]
        return None

    @staticmethod
    def _find_name_like_column(columns: List[str]) -> Optional[str]:
        exact = {
            "player_name",
            "batter_name",
            "pitcher_name",
            "last_name_first_name",
            "last_name__first_name",
            "name",
        }
        for column in columns:
            lowered = column.lower()
            if lowered in exact:
                return column

        for column in columns:
            lowered = column.lower()
            if "name" in lowered and any(token in lowered for token in ["player", "batter", "pitcher", "last", "first"]):
                return column

        for column in columns:
            if "name" in column.lower():
                return column
        return None

    def _identity_projection_sql(self, columns: List[str]) -> List[str]:
        name_col = self._find_name_like_column(columns)
        projected: List[str] = []
        if any(c.lower() == "source_table" for c in columns):
            projected.append('"source_table"')
        if name_col:
            projected.append(f'{self._quote_sqlite_identifier(name_col)} AS "player_name"')

        player_id_col = self._find_first_column(columns, ["player_id", "mlbam_id", "id"])
        if player_id_col:
            projected.append(f'{self._quote_sqlite_identifier(player_id_col)} AS "player_id"')

        return projected

    def _sqlite_numeric_expr(self, column_name: str) -> str:
        quoted = self._quote_sqlite_identifier(column_name)
        return f"CAST(NULLIF(REPLACE({quoted}, '%', ''), '') AS REAL)"

    @staticmethod
    def _normalize_analysis_profile(profile: str) -> str:
        allowed = {
            "General Manager",
            "Hitting Analyst",
            "Pitching Analyst",
            "DFS Mode",
            "Betting Mode",
        }
        candidate = (profile or "").strip()
        return candidate if candidate in allowed else "General Manager"

    @staticmethod
    def _format_metric_label(column_name: str) -> str:
        label_map = {
            "whiff_percent": "Whiff Rate",
            "k_percent": "Strikeout Rate",
            "strikeout_percent": "Strikeout Rate",
            "bb_percent": "Walk Rate",
            "barrel_batted_rate": "Barrel Rate",
            "hard_hit_percent": "Hard-Hit Rate",
            "sweet_spot_percent": "Sweet Spot Rate",
            "avg_best_speed": "Bat Speed",
            "avg_hyper_speed": "Bat Speed",
            "xwoba": "xwOBA",
            "woba": "wOBA",
        }
        return label_map.get(column_name.lower(), column_name.replace("_", " ").title())

    def _question_rank_focus(self, question: str, columns: List[str]) -> tuple[Optional[str], Optional[str], str]:
        lowered = (question or "").lower()
        specs = [
            ("whiff_percent", ["whiff rate", "whiff%", "whiff"], "ASC"),
            ("barrel_batted_rate", ["barrel rate", "barrel%", "barrel"], "DESC"),
            ("hard_hit_percent", ["hard hit rate", "hard hit%", "hard-hit", "hard hit"], "DESC"),
            ("sweet_spot_percent", ["sweet spot rate", "sweet spot%", "sweet spot"], "DESC"),
            ("avg_best_speed", ["bat speed", "best speed", "avg best speed"], "DESC"),
            ("avg_hyper_speed", ["hyper speed"], "DESC"),
            ("xwoba", ["xwoba", "expected woba", "expected weighted on-base"], "DESC"),
            ("woba", ["woba", "weighted on-base"], "DESC"),
            ("k_percent", ["strikeout rate", "strikeout%", "k rate", "k%", "k percent"], "DESC"),
            ("bb_percent", ["walk rate", "bb%", "bb percent"], "DESC"),
        ]
        for metric_name, aliases, direction in specs:
            if any(alias in lowered for alias in aliases):
                actual = self._find_first_column(columns, [metric_name])
                if actual:
                    return actual, self._format_metric_label(actual), direction

        return None, None, "DESC"

    def _build_tandem_uploaded_query(self, table_names: List[str], question: str, profile: str) -> tuple[str, List[str], str]:
        if len(table_names) < 2:
            return "", [], "Need at least two tables for tandem mode."

        table_columns: List[tuple[str, List[str], dict[str, str]]] = []
        for table_name in table_names:
            columns = self._sqlite_table_columns(table_name)
            if not columns:
                return "", [], f"Could not read columns for table '{table_name}'."
            column_map = {c.lower(): c for c in columns}
            table_columns.append((table_name, columns, column_map))

        shared_lower = set(table_columns[0][2].keys())
        for _, _, column_map in table_columns[1:]:
            shared_lower &= set(column_map.keys())

        if not shared_lower:
            return "", [], "The selected tables do not share any columns that can be queried together."

        preferred_order = [c for c in table_columns[0][1] if c.lower() in shared_lower]
        if not preferred_order:
            preferred_order = [sorted(shared_lower)[0]]

        cte_parts: List[str] = []
        for table_name, _, column_map in table_columns:
            select_cols = [f"'" + table_name.replace("'", "''") + f"' AS source_table"]
            for lower_name in preferred_order:
                actual = column_map.get(lower_name.lower())
                if actual:
                    select_cols.append(self._quote_sqlite_identifier(actual))
            cte_parts.append(
                "SELECT " + ", ".join(select_cols) + f" FROM {self._quote_sqlite_identifier(table_name)}"
            )

        combined_columns = ["source_table"] + preferred_order
        body_sql, reason = self._heuristic_csv_sql("combined_uploaded", question, combined_columns, profile)
        if not body_sql:
            return "", [], reason

        combined_sql = "WITH combined_uploaded AS (\n" + "\nUNION ALL\n".join(cte_parts) + "\n)\n" + body_sql
        return combined_sql, combined_columns, reason

    def answer_uploaded_tables_question(
        self,
        table_names: List[str],
        question: str,
        analysis_profile: str = "General Manager",
    ) -> dict:
        prompt = (question or "").strip()
        profile = self._normalize_analysis_profile(analysis_profile)
        selected_tables = [str(t).strip() for t in (table_names or []) if str(t).strip()]

        if not prompt:
            return {"ok": False, "error": "Question is empty."}
        if len(selected_tables) < 2:
            return {"ok": False, "error": "Select at least two tables to use tandem mode."}
        if not self.database_url:
            return {"ok": False, "error": "DATABASE_URL is not set."}
        if not self._is_sqlite_url(self.database_url):
            return {"ok": False, "error": "Uploaded CSV queries are only supported in demo SQLite mode."}

        sql, combined_columns, reason = self._build_tandem_uploaded_query(selected_tables, prompt, profile)
        if not sql:
            return {"ok": False, "error": reason}

        result = self.run_query_with_params(sql, {})
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error", "Failed to execute tandem uploaded CSV query.")}

        analysis = self._build_uploaded_analysis(prompt, result)
        return {
            "ok": True,
            "mode": "uploaded_csv_tandem_query",
            "plan": {
                "reason": reason,
                "table_names": selected_tables,
                "columns": combined_columns,
                "analysis_profile": profile,
            },
            "proposed_sql": sql,
            "result": result,
            "analysis": analysis,
            "narrative_card": self._build_narrative_card(prompt, analysis, result, profile),
        }

    @staticmethod
    def _question_mentions_pitchers(question: str) -> bool:
        lowered = (question or "").lower()
        return any(token in lowered for token in ["pitcher", "pitchers", "starter", "starters", "matchup", "matchups"])

    @staticmethod
    def _table_has_pitching_signals(columns: List[str]) -> bool:
        joined = " ".join([str(c).lower() for c in columns])
        signals = ["pitcher", "starter", "k_percent", "csw", "era", "whip", "fip", "swinging_strike", "woba_against"]
        return any(signal in joined for signal in signals)

    @staticmethod
    def _table_has_pitcher_like_rate_metrics(columns: List[str]) -> bool:
        joined = " ".join([str(c).lower() for c in columns])
        signals = ["k_percent", "bb_percent", "whiff_percent", "woba", "xwoba", "barrel_batted_rate", "hard_hit_percent", "swing_percent"]
        return any(signal in joined for signal in signals)

    @staticmethod
    def _confidence_from_score(score: float, top_score: float, pa_value: Optional[float]) -> str:
        ratio = 0.0 if top_score <= 0 else score / top_score
        if ratio >= 0.95:
            band = "High"
        elif ratio >= 0.85:
            band = "Medium"
        else:
            band = "Low"

        if pa_value is not None:
            if pa_value < 40:
                band = "Low"
            elif pa_value < 70 and band == "High":
                band = "Medium"
        return band

    @staticmethod
    def _shift_confidence(band: str, delta: int) -> str:
        levels = ["Low", "Medium", "High"]
        current = levels.index(band) if band in levels else 1
        shifted = max(0, min(len(levels) - 1, current + delta))
        return levels[shifted]

    @staticmethod
    def _first_existing_key(mapping: dict[str, int], candidates: List[str]) -> Optional[str]:
        for candidate in candidates:
            if candidate in mapping:
                return candidate
        return None

    def _context_columns_for_profile(self, columns: List[str], kind: str) -> List[str]:
        candidates_hitting = [
            "opp_pitcher_hand",
            "opponent_pitcher_hand",
            "pitcher_hand",
            "xwoba_vs_rhp",
            "xwoba_vs_lhp",
            "woba_vs_rhp",
            "woba_vs_lhp",
            "recent_xwoba",
            "recent_woba",
            "last_7_woba",
            "last_14_woba",
        ]
        candidates_pitching = [
            "opponent_k_percent",
            "opp_k_percent",
            "opponent_woba",
            "opp_woba",
            "recent_era",
            "last_3_start_era",
            "last_5_start_era",
            "home_away_split",
        ]
        wanted = candidates_hitting if kind == "hitting" else candidates_pitching
        by_lower = {c.lower(): c for c in columns}
        selected: List[str] = []
        for candidate in wanted:
            actual = by_lower.get(candidate)
            if actual and actual not in selected:
                selected.append(actual)
        return selected

    def _build_top_candidates(self, result: dict, profile: str) -> List[dict]:
        rows = result.get("rows") or []
        columns = [str(c) for c in (result.get("columns") or [])]
        if not rows or not columns:
            return []

        col_index = {c.lower(): i for i, c in enumerate(columns)}
        score_col = "hr_likelihood_score" if "hr_likelihood_score" in col_index else "pitching_edge_score"
        if score_col not in col_index:
            return []

        score_idx = col_index[score_col]
        top_score = self._try_float(rows[0][score_idx]) if len(rows[0]) > score_idx else None
        if top_score is None:
            top_score = 0.0

        pa_idx = col_index.get("pa")
        name_idx = col_index.get("player_name")
        id_idx = col_index.get("player_id")

        positive_metrics = ["barrel_batted_rate", "hard_hit_percent", "avg_best_speed", "xwoba", "woba", "k_percent", "csw_percent"]
        suppress_metrics = ["era", "fip", "whip", "bb_percent", "xwoba", "woba_against"]

        top_candidates: List[dict] = []
        for row in rows[:3]:
            if len(row) <= score_idx:
                continue
            score_val = self._try_float(row[score_idx])
            if score_val is None:
                continue

            name = str(row[name_idx]) if name_idx is not None and len(row) > name_idx else "Unknown Player"
            player_id = str(row[id_idx]) if id_idx is not None and len(row) > id_idx else ""
            pa_value = self._try_float(row[pa_idx]) if pa_idx is not None and len(row) > pa_idx else None
            confidence = self._confidence_from_score(score_val, float(top_score), pa_value)

            highlights: List[str] = []
            for metric in positive_metrics:
                idx = col_index.get(metric)
                if idx is None or len(row) <= idx:
                    continue
                metric_val = self._try_float(row[idx])
                if metric_val is None:
                    continue
                highlights.append(f"{metric} {metric_val:.2f}")
                if len(highlights) >= 2:
                    break

            if score_col == "pitching_edge_score":
                for metric in suppress_metrics:
                    idx = col_index.get(metric)
                    if idx is None or len(row) <= idx:
                        continue
                    metric_val = self._try_float(row[idx])
                    if metric_val is None:
                        continue
                    if f"{metric} {metric_val:.2f}" not in highlights:
                        highlights.append(f"{metric} {metric_val:.2f}")
                    if len(highlights) >= 3:
                        break

            confidence_delta = 0
            context_notes: List[str] = []

            if score_col == "hr_likelihood_score":
                hand_key = self._first_existing_key(col_index, ["opp_pitcher_hand", "opponent_pitcher_hand", "pitcher_hand"])
                split_r_key = self._first_existing_key(col_index, ["xwoba_vs_rhp", "woba_vs_rhp"])
                split_l_key = self._first_existing_key(col_index, ["xwoba_vs_lhp", "woba_vs_lhp"])
                recent_key = self._first_existing_key(col_index, ["recent_xwoba", "recent_woba", "last_7_woba", "last_14_woba"])

                if hand_key is not None and len(row) > col_index[hand_key]:
                    hand_val = str(row[col_index[hand_key]]).strip().upper()
                    split_key = split_r_key if hand_val == "R" else split_l_key if hand_val == "L" else None
                    if split_key is not None and len(row) > col_index[split_key]:
                        split_val = self._try_float(row[col_index[split_key]])
                        if split_val is not None:
                            context_notes.append(f"vs {hand_val}HP split {split_val:.3f}")
                            if split_val >= 0.360:
                                confidence_delta += 1
                            elif split_val <= 0.300:
                                confidence_delta -= 1

                if recent_key is not None and len(row) > col_index[recent_key]:
                    recent_val = self._try_float(row[col_index[recent_key]])
                    if recent_val is not None:
                        context_notes.append(f"recent form {recent_val:.3f}")
                        if recent_val >= 0.350:
                            confidence_delta += 1
                        elif recent_val <= 0.290:
                            confidence_delta -= 1

            if score_col == "pitching_edge_score":
                opp_k_key = self._first_existing_key(col_index, ["opponent_k_percent", "opp_k_percent"])
                opp_woba_key = self._first_existing_key(col_index, ["opponent_woba", "opp_woba"])
                recent_era_key = self._first_existing_key(col_index, ["recent_era", "last_3_start_era", "last_5_start_era"])

                if opp_k_key is not None and len(row) > col_index[opp_k_key]:
                    opp_k = self._try_float(row[col_index[opp_k_key]])
                    if opp_k is not None:
                        context_notes.append(f"opponent K% {opp_k:.1f}")
                        if opp_k >= 24.0:
                            confidence_delta += 1
                        elif opp_k <= 18.0:
                            confidence_delta -= 1

                if opp_woba_key is not None and len(row) > col_index[opp_woba_key]:
                    opp_woba = self._try_float(row[col_index[opp_woba_key]])
                    if opp_woba is not None:
                        context_notes.append(f"opponent wOBA {opp_woba:.3f}")
                        if opp_woba <= 0.305:
                            confidence_delta += 1
                        elif opp_woba >= 0.340:
                            confidence_delta -= 1

                if recent_era_key is not None and len(row) > col_index[recent_era_key]:
                    recent_era = self._try_float(row[col_index[recent_era_key]])
                    if recent_era is not None:
                        context_notes.append(f"recent ERA {recent_era:.2f}")
                        if recent_era <= 3.20:
                            confidence_delta += 1
                        elif recent_era >= 4.80:
                            confidence_delta -= 1

            confidence = self._shift_confidence(confidence, confidence_delta)
            reason = "Key drivers: " + ", ".join(highlights) if highlights else "Key drivers unavailable in current columns."
            if context_notes:
                reason += " | Matchup context: " + ", ".join(context_notes)

            top_candidates.append(
                {
                    "name": name,
                    "player_id": player_id,
                    "score": round(score_val, 2),
                    "confidence": confidence,
                    "reason": reason,
                }
            )

        return top_candidates

    def _build_narrative_card(self, question: str, analysis: List[str], result: dict, profile: str) -> dict:
        row_count = int(result.get("row_count") or 0)
        top_line = analysis[0] if analysis else "No standout leader identified from the current result set."
        second_line = analysis[1] if len(analysis) > 1 else "Scores are based on uploaded metrics and selected analyst mode."
        top_candidates = self._build_top_candidates(result, profile)
        rank_metric, rank_label, rank_direction = self._question_rank_focus(question, [str(c) for c in (result.get("columns") or [])])
        rank_label_text = rank_label or (rank_metric.replace("_", " ").title() if rank_metric else "Metric")
        question_mentions_pitchers = self._question_mentions_pitchers(question)
        if top_candidates:
            leader = top_candidates[0]
            top_line = f"Top candidate: {leader.get('name', 'Unknown')} ({leader.get('confidence', 'Medium')} confidence)."

        if rank_metric is not None:
            actions = [
                f"Validate the top 3 names against the {rank_label_text.lower()} and nearby contact metrics.",
                "Compare this view with related stats like wOBA, hard-hit rate, and barrel rate.",
                "Re-run with a minimum PA filter or recent-form split to confirm the ranking.",
            ]
        else:
            actions = [
                "Validate the top 3 names against today's confirmed lineup/starting data.",
                "Compare this view with your baseline projection model to identify disagreements.",
                "Re-run with tighter filters (handedness, recent games, opponent) for final decisions.",
            ]

        if profile == "DFS Mode":
            actions = [
                "Cross-check top values with salary and ownership projections.",
                "Create one high-floor and one high-ceiling build using the top-ranked names.",
                "Re-run excluding chalk to find leverage alternatives.",
            ]
        elif profile == "Betting Mode":
            actions = [
                "Compare top edges with current market lines before placing bets.",
                "Use confidence tiers and avoid overexposure on thin edges.",
                "Re-run after lineup confirmations to refresh pregame assumptions.",
            ]

        if rank_metric is not None:
            title = f"{rank_label_text} Leaders"
            if rank_direction == "ASC":
                title = f"Lowest {rank_label_text} Leaders"
            return {
                "title": title,
                "summary": (
                    f"{top_line} {second_line} "
                    f"This answer is based on {row_count} returned rows for your question: '{question}'."
                ),
                "actions": actions,
                "top_candidates": top_candidates,
            }

        if question_mentions_pitchers:
            return {
                "title": "Pitcher Leaders",
                "summary": (
                    f"{top_line} {second_line} "
                    f"This answer is based on {row_count} returned rows for your question: '{question}'."
                ),
                "actions": actions,
                "top_candidates": top_candidates,
            }

        summary = (
            f"{top_line} {second_line} "
            f"This answer is based on {row_count} returned rows for your question: '{question}'."
        )
        return {
            "title": f"{profile} Brief",
            "summary": summary,
            "actions": actions,
            "top_candidates": top_candidates,
        }

    def _build_homerun_likelihood_sql(self, table_name: str, columns: List[str], profile: str) -> tuple[str, str]:
        pa_col = self._find_first_column(columns, ["pa", "plate_appearances", "ab"])

        metric_specs = [
            ("barrel_batted_rate", 0.35, 1.0),
            ("hard_hit_percent", 0.25, 1.0),
            ("avg_best_speed", 0.20, 1.0),
            ("xwoba", 0.15, 100.0),
            ("woba", 0.05, 100.0),
        ]
        if profile == "Hitting Analyst":
            metric_specs = [
                ("barrel_batted_rate", 0.30, 1.0),
                ("hard_hit_percent", 0.25, 1.0),
                ("xwoba", 0.25, 100.0),
                ("woba", 0.10, 100.0),
                ("avg_best_speed", 0.10, 1.0),
            ]
        elif profile == "DFS Mode":
            metric_specs = [
                ("barrel_batted_rate", 0.35, 1.0),
                ("avg_best_speed", 0.25, 1.0),
                ("hard_hit_percent", 0.20, 1.0),
                ("xwoba", 0.15, 100.0),
                ("woba", 0.05, 100.0),
            ]
        elif profile == "Betting Mode":
            metric_specs = [
                ("xwoba", 0.30, 100.0),
                ("woba", 0.25, 100.0),
                ("hard_hit_percent", 0.20, 1.0),
                ("barrel_batted_rate", 0.15, 1.0),
                ("avg_best_speed", 0.10, 1.0),
            ]

        weighted_terms: List[str] = []
        selected_metrics: List[str] = []
        for metric_name, weight, scale in metric_specs:
            metric_col = self._find_first_column(columns, [metric_name])
            if not metric_col:
                continue
            expr = self._sqlite_numeric_expr(metric_col)
            if scale != 1.0:
                expr = f"({expr} * {scale})"
            weighted_terms.append(f"({weight} * COALESCE({expr}, 0.0))")
            selected_metrics.append(metric_col)

        if not weighted_terms:
            return "", "Could not find power/quality-of-contact columns to score home-run likelihood."

        selected_cols = self._identity_projection_sql(columns)
        for col in selected_metrics:
            quoted = self._quote_sqlite_identifier(col)
            if quoted not in selected_cols:
                selected_cols.append(quoted)
        for col in self._context_columns_for_profile(columns, kind="hitting"):
            quoted = self._quote_sqlite_identifier(col)
            if quoted not in selected_cols:
                selected_cols.append(quoted)
        score_expr = " + ".join(weighted_terms)
        where_clause = ""
        if pa_col:
            pa_expr = self._sqlite_numeric_expr(pa_col)
            where_clause = f"WHERE COALESCE({pa_expr}, 0) >= 50"

        sql = (
            f"SELECT {', '.join(selected_cols)},\n"
            f"       ({score_expr}) AS hr_likelihood_score\n"
            f"FROM {self._quote_sqlite_identifier(table_name)}\n"
            f"{where_clause}\n"
            f"ORDER BY hr_likelihood_score DESC\n"
            f"LIMIT {self.max_rows};"
        )
        return sql, "Scored hitters using barrel rate, hard-hit rate, bat speed, and expected quality-of-contact metrics."

    def _build_pitcher_matchup_sql(self, table_name: str, columns: List[str], profile: str) -> tuple[str, str]:
        positive_specs = [
            ("k_percent", 0.45, 1.0),
            ("strikeout_percent", 0.45, 1.0),
            ("swinging_strike_percent", 0.25, 1.0),
            ("csw_percent", 0.20, 1.0),
        ]
        negative_specs = [
            ("xwoba", 0.35, 100.0),
            ("woba_against", 0.35, 100.0),
            ("era", 0.25, 1.0),
            ("fip", 0.20, 1.0),
            ("whip", 0.20, 1.0),
            ("bb_percent", 0.15, 1.0),
            ("hard_hit_percent", 0.10, 1.0),
            ("barrel_batted_rate", 0.10, 1.0),
        ]
        if profile == "Pitching Analyst":
            positive_specs = [
                ("k_percent", 0.40, 1.0),
                ("strikeout_percent", 0.40, 1.0),
                ("swinging_strike_percent", 0.25, 1.0),
                ("csw_percent", 0.25, 1.0),
            ]
            negative_specs = [
                ("xwoba", 0.30, 100.0),
                ("woba_against", 0.30, 100.0),
                ("fip", 0.20, 1.0),
                ("bb_percent", 0.20, 1.0),
                ("hard_hit_percent", 0.15, 1.0),
                ("barrel_batted_rate", 0.15, 1.0),
            ]
        elif profile == "DFS Mode":
            positive_specs = [
                ("k_percent", 0.50, 1.0),
                ("strikeout_percent", 0.50, 1.0),
                ("swinging_strike_percent", 0.25, 1.0),
            ]
            negative_specs = [
                ("bb_percent", 0.20, 1.0),
                ("xwoba", 0.20, 100.0),
                ("era", 0.10, 1.0),
            ]
        elif profile == "Betting Mode":
            positive_specs = [
                ("k_percent", 0.35, 1.0),
                ("csw_percent", 0.20, 1.0),
            ]
            negative_specs = [
                ("xwoba", 0.30, 100.0),
                ("era", 0.25, 1.0),
                ("whip", 0.20, 1.0),
                ("bb_percent", 0.15, 1.0),
            ]

        used_fallback = False
        if not any(self._find_first_column(columns, [metric_name]) for metric_name, _, _ in positive_specs + negative_specs):
            used_fallback = True
            positive_specs = [
                ("k_percent", 0.35, 1.0),
                ("whiff_percent", 0.25, 1.0),
                ("swing_percent", 0.10, 1.0),
            ]
            negative_specs = [
                ("xwoba", 0.30, 100.0),
                ("woba", 0.30, 100.0),
                ("bb_percent", 0.20, 1.0),
                ("barrel_batted_rate", 0.15, 1.0),
                ("hard_hit_percent", 0.10, 1.0),
                ("avg_best_speed", 0.05, 1.0),
            ]

        positive_terms: List[str] = []
        negative_terms: List[str] = []
        selected_metrics: List[str] = []

        for metric_name, weight, scale in positive_specs:
            metric_col = self._find_first_column(columns, [metric_name])
            if not metric_col:
                continue
            expr = self._sqlite_numeric_expr(metric_col)
            if scale != 1.0:
                expr = f"({expr} * {scale})"
            positive_terms.append(f"({weight} * COALESCE({expr}, 0.0))")
            selected_metrics.append(metric_col)

        for metric_name, weight, scale in negative_specs:
            metric_col = self._find_first_column(columns, [metric_name])
            if not metric_col:
                continue
            expr = self._sqlite_numeric_expr(metric_col)
            if scale != 1.0:
                expr = f"({expr} * {scale})"
            negative_terms.append(f"({weight} * COALESCE({expr}, 0.0))")
            selected_metrics.append(metric_col)

        if not positive_terms and not negative_terms:
            return "", "Could not find pitcher performance columns for matchup scoring."

        selected_cols = self._identity_projection_sql(columns)
        for col in sorted(set(selected_metrics)):
            quoted = self._quote_sqlite_identifier(col)
            if quoted not in selected_cols:
                selected_cols.append(quoted)
        for col in self._context_columns_for_profile(columns, kind="pitching"):
            quoted = self._quote_sqlite_identifier(col)
            if quoted not in selected_cols:
                selected_cols.append(quoted)
        positive_expr = " + ".join(positive_terms) if positive_terms else "0"
        negative_expr = " + ".join(negative_terms) if negative_terms else "0"
        score_expr = f"({positive_expr}) - ({negative_expr})"

        sql = (
            f"SELECT {', '.join(selected_cols)},\n"
            f"       ({score_expr}) AS pitching_edge_score\n"
            f"FROM {self._quote_sqlite_identifier(table_name)}\n"
            f"ORDER BY pitching_edge_score DESC\n"
            f"LIMIT {self.max_rows};"
        )
        if used_fallback:
            return sql, "Scored pitchers using K%, whiff%, swing%, wOBA, xwOBA, BB%, barrel rate, and hard-hit rate."
        return sql, "Scored pitchers using strikeout ability, contact suppression, and run-prevention indicators."

    def _build_uploaded_analysis(self, question: str, result: dict) -> List[str]:
        rows = result.get("rows") or []
        columns = [str(c) for c in (result.get("columns") or [])]
        if not rows or not columns:
            return []

        col_index = {c.lower(): idx for idx, c in enumerate(columns)}
        bullets: List[str] = []
        top_row = rows[0]
        rank_metric, rank_label, rank_direction = self._question_rank_focus(question, columns)
        rank_label_text = rank_label or (rank_metric.replace("_", " ").title() if rank_metric else "Metric")

        name_idx = col_index.get("player_name")
        if name_idx is None:
            detected_name_col = self._find_name_like_column(columns)
            if detected_name_col:
                name_idx = col_index.get(detected_name_col.lower())

        if name_idx is not None and name_idx < len(top_row):
            if rank_metric is not None:
                metric_idx = col_index.get(rank_metric.lower())
                metric_val = self._try_float(top_row[metric_idx]) if metric_idx is not None and metric_idx < len(top_row) else None
                if metric_val is not None:
                    comparator = "lowest" if rank_direction == "ASC" else "highest"
                    bullets.append(f"Top result: {top_row[name_idx]} | {rank_label_text}: {metric_val:.3f}")
                    bullets.append(f"Question interpreted as the {comparator} {rank_label_text.lower()} among the shown rows.")
                else:
                    bullets.append(f"Top result: {top_row[name_idx]}")
            else:
                bullets.append(f"Top result: {top_row[name_idx]}")

        for score_col in ["hr_likelihood_score", "pitching_edge_score"]:
            idx = col_index.get(score_col)
            if idx is not None and idx < len(top_row):
                score_value = self._try_float(top_row[idx])
                if score_value is not None:
                    bullets.append(f"Leading score ({score_col}): {score_value:.2f}")
                    break

        if rank_metric is not None:
            metric_idx = col_index.get(rank_metric.lower())
            if metric_idx is not None:
                values = [self._try_float(row[metric_idx]) for row in rows[: min(len(rows), 25)] if metric_idx < len(row)]
                values = [v for v in values if v is not None]
                if values:
                    best_val = min(values) if rank_direction == "ASC" else max(values)
                    bullets.append(f"Best {rank_label_text.lower()} in shown rows: {best_val:.3f}")
                    return bullets[:3]

        tracked_metrics = [
            "woba",
            "xwoba",
            "hard_hit_percent",
            "barrel_batted_rate",
            "avg_best_speed",
            "k_percent",
            "bb_percent",
            "era",
            "fip",
            "whip",
        ]
        for metric in tracked_metrics:
            idx = col_index.get(metric)
            if idx is None:
                continue
            values = [self._try_float(row[idx]) for row in rows[: min(len(rows), 25)] if idx < len(row)]
            values = [v for v in values if v is not None]
            if values:
                avg_val = sum(values) / len(values)
                bullets.append(f"Average {metric} across shown rows: {avg_val:.2f}")
                if len(bullets) >= 4:
                    break

        if "today" in question.lower() and len(bullets) < 4:
            bullets.append("'Today' context was interpreted using the latest metrics available in your uploaded file.")

        return bullets[:4]

    def _heuristic_csv_sql(self, table_name: str, question: str, columns: List[str], profile: str) -> tuple[str, str]:
        lowered = question.lower()
        pitching_profile = profile in {"Pitching Analyst", "DFS Mode", "Betting Mode"}
        hitting_profile = profile in {"Hitting Analyst", "DFS Mode", "Betting Mode"}
        mentions_pitchers = self._question_mentions_pitchers(question)
        has_pitching_signals = self._table_has_pitching_signals(columns)
        has_pitcher_like_rates = self._table_has_pitcher_like_rate_metrics(columns)

        if mentions_pitchers and not has_pitching_signals and not has_pitcher_like_rates:
            return "", "This uploaded table does not appear to contain pitcher stats. Upload a pitching dataset or ask about a hitter metric that exists in this table."

        if pitching_profile and has_pitching_signals:
            sql, reason = self._build_pitcher_matchup_sql(table_name, columns, profile)
            if sql:
                return sql, reason

        if hitting_profile and any(token in " ".join(columns).lower() for token in ["barrel", "hard_hit", "woba"]):
            sql, reason = self._build_homerun_likelihood_sql(table_name, columns, profile)
            if sql:
                return sql, reason

        if any(token in lowered for token in ["homerun", "home run", "hr", "hottest hitter", "power hitter"]):
            sql, reason = self._build_homerun_likelihood_sql(table_name, columns, profile)
            if sql:
                return sql, reason

        if mentions_pitchers:
            sql, reason = self._build_pitcher_matchup_sql(table_name, columns, profile)
            if sql:
                return sql, reason

        rank_metric, rank_label, rank_direction = self._question_rank_focus(question, columns)
        if rank_metric:
            order_column = self._quote_sqlite_identifier(rank_metric)
            rank_label_text = rank_label or (rank_metric.replace("_", " ").title() if rank_metric else "metric")
            sql = (
                f"SELECT * FROM {self._quote_sqlite_identifier(table_name)} "
                f"ORDER BY {order_column} {rank_direction} "
                f"LIMIT {self.max_rows};"
            )
            comparator = "lowest" if rank_direction == "ASC" else "highest"
            reason = f"Ranked rows by {rank_label_text} because the question asked for the {comparator} {rank_label_text.lower()}."
            return sql, reason

        ranked_column = self._find_first_column(
            columns,
            ["woba", "xwoba", "avg_best_speed", "hard_hit_percent", "barrel_batted_rate", "sweet_spot_percent"],
        )
        order_direction = "ASC" if any(token in lowered for token in ["coldest", "worst", "lowest", "least"]) else "DESC"

        if ranked_column:
            order_column = self._quote_sqlite_identifier(ranked_column)
            sql = (
                f"SELECT * FROM {self._quote_sqlite_identifier(table_name)} "
                f"ORDER BY {order_column} {order_direction} "
                f"LIMIT {self.max_rows};"
            )
            reason = f"Ranked rows by {ranked_column} based on the question."
            return sql, reason

        if columns:
            sql = f"SELECT * FROM {self._quote_sqlite_identifier(table_name)} LIMIT {self.max_rows};"
            return sql, "No obvious ranking column found, so returned the first rows from the uploaded table."

        return "", "No columns were discovered for the uploaded table."

    def answer_uploaded_table_question(
        self,
        table_name: str,
        question: str,
        analysis_profile: str = "General Manager",
    ) -> dict:
        prompt = (question or "").strip()
        profile = self._normalize_analysis_profile(analysis_profile)
        if not prompt:
            return {"ok": False, "error": "Question is empty."}

        if not self.database_url:
            return {"ok": False, "error": "DATABASE_URL is not set."}

        if not self._is_sqlite_url(self.database_url):
            return {"ok": False, "error": "Uploaded CSV queries are only supported in demo SQLite mode."}

        columns = self._sqlite_table_columns(table_name)
        if not columns:
            return {"ok": False, "error": f"Could not read columns for table '{table_name}'."}

        if self.client is not None:
            model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
            system = (
                "You write one safe, read-only SQLite query for a single uploaded CSV table. "
                "Do not use any SQL template library or other tables."
            )
            user = {
                "question": prompt,
                "table_name": table_name,
                "columns": columns,
                "analysis_profile": profile,
                "instructions": [
                    "Return JSON with keys: reason, sql.",
                    "Use only the provided table and columns.",
                    "Use read-only SELECT/WITH SQL only.",
                    "Prefer ORDER BY and LIMIT when the question asks for best, hottest, top, lowest, or similar ranking.",
                ],
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
                raw = (response.choices[0].message.content or "{}").strip()
                parsed = json.loads(raw)
                sql = (parsed.get("sql") or "").strip()
                reason = str(parsed.get("reason") or "Generated a query from the uploaded CSV schema.")
                if sql and self._is_read_only_sql(sql):
                    tables_ok, disallowed_tables = self._check_allowed_tables(sql)
                    if tables_ok:
                        result = self.run_query_with_params(sql, {})
                        if result.get("ok"):
                            return {
                                "ok": True,
                                "mode": "uploaded_csv_query",
                                "plan": {
                                    "reason": reason,
                                    "table_name": table_name,
                                    "columns": columns,
                                    "analysis_profile": profile,
                                },
                                "proposed_sql": sql,
                                "result": result,
                                "analysis": self._build_uploaded_analysis(prompt, result),
                                "narrative_card": self._build_narrative_card(
                                    prompt,
                                    self._build_uploaded_analysis(prompt, result),
                                    result,
                                    profile,
                                ),
                            }
                    if not tables_ok:
                        return {
                            "ok": False,
                            "error": "Query references table(s) not in ALLOWED_TABLES: " + ", ".join(disallowed_tables),
                        }
            except Exception:
                pass

        sql, reason = self._heuristic_csv_sql(table_name, prompt, columns, profile)
        if not sql:
            return {"ok": False, "error": reason}

        result = self.run_query_with_params(sql, {})
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error", "Failed to execute uploaded CSV query.")}

        analysis = self._build_uploaded_analysis(prompt, result)
        return {
            "ok": True,
            "mode": "uploaded_csv_query",
            "plan": {
                "reason": reason,
                "table_name": table_name,
                "columns": columns,
                "analysis_profile": profile,
            },
            "proposed_sql": sql,
            "result": result,
            "analysis": analysis,
            "narrative_card": self._build_narrative_card(prompt, analysis, result, profile),
        }

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
