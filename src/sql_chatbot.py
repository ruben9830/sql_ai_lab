import argparse
import json
import os
import re
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

    def to_dict(self) -> dict:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "fein": self.fein,
            "employer_id": self.employer_id,
        }


class SQLBibleChatbot:
    def __init__(self, sql_file: Path, max_rows: int = 200):
        self.sql_file = sql_file
        self.max_rows = max_rows
        self.queries: List[QuerySnippet] = self._load_queries()
        self.client = self._build_openai_client()
        self.database_url = os.getenv("DATABASE_URL", "").strip()
        self.allowed_tables = self._load_allowed_tables()

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

            title = self._title_from_comments(comment_buffer) or f"Query {len(snippets) + 1}"
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

        return intent

    @staticmethod
    def _apply_intent_override(base: QueryIntent, override: Optional[dict]) -> QueryIntent:
        if not override:
            return base

        for key in ("start_date", "end_date", "fein", "employer_id"):
            value = (override.get(key) or "").strip() if isinstance(override, dict) else ""
            if value:
                setattr(base, key, value)
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
        return terms

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

        if psycopg is None:
            return {"ok": False, "error": "psycopg is not installed. Run: pip install -r requirements.txt"}

        safe_sql = sql.strip().rstrip(";") + f"\nLIMIT {self.max_rows};"

        try:
            with psycopg.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(safe_sql)
                    cols = [d.name for d in cur.description] if cur.description else []
                    rows = cur.fetchall() if cols else []
            return {"ok": True, "columns": cols, "rows": rows, "row_count": len(rows)}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def answer(self, question: str, intent_override: Optional[dict] = None) -> dict:
        intent = self.extract_intent(question)
        intent = self._apply_intent_override(intent, intent_override)

        candidates = self.search_queries(question, top_n=5, intent=intent)
        llm_plan = self.suggest_with_llm(question, candidates, intent=intent)

        if llm_plan:
            action = llm_plan.get("action", "suggest_only")
            sql = (llm_plan.get("sql") or "").strip()
            candidate_ids = llm_plan.get("candidate_ids") or []

            if action == "run_query" and sql:
                result = self.run_query(sql)
                return {
                    "mode": "llm_run_query",
                    "plan": llm_plan,
                    "result": result,
                    "intent": intent.to_dict(),
                    "fallback_candidates": [self._q_to_dict(q) for q in candidates],
                }

            chosen = [q for q in self.queries if q.id in candidate_ids][:5]
            suggestions = chosen if chosen else candidates
            return {
                "mode": "llm_suggest",
                "plan": llm_plan,
                "intent": intent.to_dict(),
                "suggestions": [self._q_to_dict(q) for q in suggestions],
            }

        return {
            "mode": "keyword_suggest",
            "intent": intent.to_dict(),
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
