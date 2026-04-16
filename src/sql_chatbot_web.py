from pathlib import Path
import csv
import io
import os
import re
import time

import streamlit as st
from dotenv import load_dotenv

from demo_data import ensure_demo_database
from sql_chatbot import SQLBibleChatbot


def _polish_title(raw_title: str) -> str:
    cleaned = re.sub(r"\s+", " ", (raw_title or "").replace("_", " ")).strip(" -\t\n.")
    if not cleaned:
        return "SQL template"

    tokens = []
    for token in cleaned.split(" "):
        upper_token = token.upper()
        if upper_token in {"SQL", "FEIN", "TPA", "ID"}:
            tokens.append(upper_token)
        elif token.isupper() and len(token) > 1:
            tokens.append(token)
        else:
            tokens.append(token.capitalize())
    return " ".join(tokens)


def _render_suggestions(suggestions: list[dict]) -> None:
    if not suggestions:
        return

    st.markdown("### Recommended SQL Templates")
    st.caption("Curated options based on your question and detected filters.")
    for idx, s in enumerate(suggestions, start=1):
        polished_title = _polish_title(s.get("title", ""))
        st.markdown(f"**Recommendation {idx}: {polished_title}**")
        if s.get("id") is not None:
            st.caption(f"Template ID: {s['id']}")
        st.code(s.get("sql_preview", ""), language="sql")


def _rows_to_csv(columns: list, rows: list) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    writer.writerows(rows)
    return buffer.getvalue()


def _render_result(result: dict, key_prefix: str) -> None:
    if not result:
        return

    if not result.get("ok"):
        st.error(result.get("error", "Unknown query execution error."))
        return

    st.success(f"Rows returned: {result.get('row_count', 0)}")
    columns = result.get("columns", [])
    rows = result.get("rows", [])
    if columns and rows is not None:
        st.dataframe(rows, column_config=None, use_container_width=True)
        csv_data = _rows_to_csv(columns, rows)
        st.download_button(
            label="Download result as CSV",
            data=csv_data,
            file_name="sql_result.csv",
            mime="text/csv",
            key=f"download_{key_prefix}",
        )


def _render_metrics(payload: dict) -> None:
    metrics = payload.get("_metrics") or {}
    if not metrics:
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Response Time", f"{metrics.get('response_seconds', 0):.2f}s")
    c2.metric("Recommended Templates", int(metrics.get("candidate_count", 0)))
    c3.metric("Total Library Size", int(metrics.get("library_size", 0)))


def _render_payload(payload: dict, key_prefix: str, bot: SQLBibleChatbot | None = None) -> None:
    _render_metrics(payload)

    mode = payload.get("mode", "unknown")
    st.caption(f"Mode: {mode}")

    intent = payload.get("intent") or {}
    if any(intent.values()):
        with st.expander("Detected filters"):
            st.json(intent)

    plan = payload.get("plan")
    if plan:
        with st.expander("Reasoning summary"):
            st.json(plan)

    join_draft = payload.get("join_draft")
    if join_draft:
        with st.expander("Generated JOIN draft"):
            st.write(join_draft.get("reason", ""))
            st.caption(f"Confidence: {join_draft.get('confidence', 'unknown')}")
            st.caption(
                f"Tables: {join_draft.get('left_table')} JOIN {join_draft.get('right_table')} "
                f"on {join_draft.get('join_key')}"
            )
            st.markdown("**Parameters**")
            st.json(join_draft.get("parameters", {}))

            verification = join_draft.get("verification") or {}
            if verification:
                status = verification.get("status", "unknown")
                message = verification.get("message", "")
                st.markdown("**Schema verification**")
                if status == "verified":
                    st.success(message)
                elif status == "failed":
                    st.error(message)
                else:
                    st.info(message)

            st.code(join_draft.get("sql", ""), language="sql")

            if bot is not None:
                st.markdown("**Run JOIN Draft Safely**")
                params = dict(join_draft.get("parameters") or {})

                run_start_date = st.text_input(
                    "start_date",
                    value="" if params.get("start_date") is None else str(params.get("start_date")),
                    key=f"jd_start_date_{key_prefix}",
                )
                run_end_date = st.text_input(
                    "end_date",
                    value="" if params.get("end_date") is None else str(params.get("end_date")),
                    key=f"jd_end_date_{key_prefix}",
                )
                run_fein = st.text_input(
                    "fein",
                    value="" if params.get("fein") is None else str(params.get("fein")),
                    key=f"jd_fein_{key_prefix}",
                )
                run_employer_id = st.text_input(
                    "employer_id",
                    value="" if params.get("employer_id") is None else str(params.get("employer_id")),
                    key=f"jd_employer_id_{key_prefix}",
                )

                default_quarter = params.get("quarter")
                if isinstance(default_quarter, str):
                    default_quarter = 0
                run_quarter = st.number_input(
                    "quarter (required)",
                    min_value=0,
                    max_value=4,
                    value=int(default_quarter or 0),
                    step=1,
                    key=f"jd_quarter_{key_prefix}",
                )

                default_year = params.get("year")
                if isinstance(default_year, str):
                    default_year = 0
                run_year = st.number_input(
                    "year (required)",
                    min_value=0,
                    max_value=2100,
                    value=int(default_year or 0),
                    step=1,
                    key=f"jd_year_{key_prefix}",
                )

                run_disabled = (join_draft.get("verification") or {}).get("status") == "failed"
                if st.button("Run JOIN Draft", key=f"run_join_{key_prefix}", disabled=run_disabled):
                    run_result = bot.execute_join_draft(
                        join_draft,
                        override_params={
                            "start_date": run_start_date,
                            "end_date": run_end_date,
                            "fein": run_fein,
                            "employer_id": run_employer_id,
                            "quarter": int(run_quarter),
                            "year": int(run_year),
                        },
                    )
                    _render_result(run_result, key_prefix=f"join_exec_{key_prefix}")

    result = payload.get("result")
    if result:
        _render_result(result, key_prefix=key_prefix)

    proposed_sql = (payload.get("proposed_sql") or "").strip()
    if proposed_sql:
        st.markdown("### Generated SQL (Review & Approve)")
        st.caption("Inspect the SQL below. Click the button to execute.")
        st.code(proposed_sql, language="sql")
        if bot is not None and st.button("Execute Query", key=f"run_proposed_{key_prefix}", type="primary"):
            run_result = bot.run_query(proposed_sql)
            _render_result(run_result, key_prefix=f"proposed_exec_{key_prefix}")

    suggestions = payload.get("suggestions") or payload.get("fallback_candidates") or []
    _render_suggestions(suggestions)


def _get_bot(
    sql_file: str,
    max_rows: int,
    database_url: str,
    allowed_tables: set[str] | None,
) -> SQLBibleChatbot:
    cache_key = f"bot::{sql_file}::{max_rows}::{database_url}::{','.join(sorted(allowed_tables or set()))}"
    cached_key = st.session_state.get("bot_key")
    if cached_key != cache_key:
        st.session_state["bot"] = SQLBibleChatbot(
            sql_file=Path(sql_file),
            max_rows=max_rows,
            database_url=database_url,
            allowed_tables=allowed_tables,
        )
        st.session_state["bot_key"] = cache_key
    return st.session_state["bot"]


def _clean_filter(value: str) -> str:
    return value.strip()


def _build_intent_override(
    start_date: str,
    end_date: str,
    fein: str,
    employer_id: str,
    quarter: int,
    year: int,
) -> dict:
    return {
        "start_date": _clean_filter(start_date),
        "end_date": _clean_filter(end_date),
        "fein": _clean_filter(fein),
        "employer_id": _clean_filter(employer_id),
        "quarter": quarter if quarter in (1, 2, 3, 4) else None,
        "year": year if year >= 1900 else None,
    }


def main() -> None:
    load_dotenv()
    st.set_page_config(page_title="SQL AI Copilot", page_icon="🧠", layout="wide")
    st.title("🧠 SQL AI Copilot")
    st.write("Convert business questions into SQL queries. Review, execute, and export results—all in seconds.")

    with st.sidebar:
        st.header("⚙️ Configuration")
        sql_file = st.text_input("SQL file", value="data/SQL_BIBLE_PRIME.sql")
        max_rows = st.number_input("Max rows", min_value=1, max_value=5000, value=200, step=50)
        data_mode = st.radio("Data mode", options=["Demo (SQLite)", "Enterprise (Postgres)"], index=0)
        st.caption("Safe mode: All queries are read-only (SELECT/WITH only).")

        if data_mode == "Demo (SQLite)":
            demo_db_path = st.text_input("Demo DB file", value="data/demo_hackathon.db")
            ensure_demo_database(Path(demo_db_path))
            database_url = f"sqlite:///{demo_db_path}"
            allowed_tables = {"employers", "liabilities", "wage_reports"}
            st.success("✓ Demo mode active — no VPN or setup required.")
        else:
            db_override = st.text_input("DATABASE_URL override (optional)", value="", type="password")
            database_url = db_override.strip() or os.getenv("DATABASE_URL", "").strip()
            raw_allowed = os.getenv("ALLOWED_TABLES", "").strip()
            allowed_tables = {item.strip().lower() for item in raw_allowed.split(",") if item.strip()} if raw_allowed else None
            st.info("Enterprise mode — connects to your Postgres instance.")

        st.divider()
        if st.button("Clear current result", use_container_width=True):
            st.session_state.pop("current_exchange", None)
            st.session_state.pop("queued_question", None)
            st.rerun()

        st.divider()
        st.subheader("🔍 Optional Filters")
        st.caption("Refine results by date range, identifiers, and time period.")
        start_date = st.text_input("Start date (YYYY-MM-DD)", value="")
        end_date = st.text_input("End date (YYYY-MM-DD)", value="")
        fein = st.text_input("FEIN", value="", placeholder="12-3456789")
        employer_id = st.text_input("Employer ID", value="")
        quarter = st.number_input("Quarter", min_value=0, max_value=4, value=0, step=1)
        year = st.number_input("Year", min_value=0, max_value=2100, value=0, step=1)

        st.divider()
        st.subheader("💡 Quick Start")
        st.caption("Click any example below to run an instant demo.")
        if st.button("Example 1: Top Employers", use_container_width=True, type="secondary"):
            st.session_state["queued_question"] = "Which employers had the largest month-over-month increase in liability amount due?"
            st.rerun()
        if st.button("Example 2: Variance Analysis", use_container_width=True, type="secondary"):
            st.session_state["queued_question"] = "Show top FEINs by liability variance between start and end date."
            st.rerun()
        if st.button("Example 3: Trends", use_container_width=True, type="secondary"):
            st.session_state["queued_question"] = "Summarize liability and wage amount trends by quarter and year."
            st.rerun()

    bot = _get_bot(
        sql_file=sql_file,
        max_rows=int(max_rows),
        database_url=database_url,
        allowed_tables=allowed_tables,
    )
    col1, col2 = st.columns(2)
    col1.info(f"📚 Library: {len(bot.queries)} SQL templates loaded")
    health = bot.probe_connection()
    if health.get("ok"):
        col2.success(f"✓ {health.get('message', 'Database connected.')}")
    else:
        col2.warning(f"⚠️ {health.get('message', 'No database configured.')}")

    if "queued_question" not in st.session_state:
        st.session_state["queued_question"] = ""
    if "current_exchange" not in st.session_state:
        st.session_state["current_exchange"] = None

    # Chat input
    chat_question = st.chat_input("Ask your question (e.g. 'Show me top employers by liability amount')...")
    question = (chat_question or "").strip() or st.session_state.pop("queued_question", "")

    # Results render below chat input (no scroll needed)
    if not question:
        current = st.session_state.get("current_exchange")
        if current:
            with st.chat_message("user"):
                st.markdown(current["question"])
            with st.chat_message("assistant"):
                _render_payload(current["payload"], key_prefix="current", bot=bot)
    else:
        with st.chat_message("user"):
            st.markdown(question)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                started = time.perf_counter()
                intent_override = _build_intent_override(
                    start_date=start_date,
                    end_date=end_date,
                    fein=fein,
                    employer_id=employer_id,
                    quarter=int(quarter),
                    year=int(year),
                )
                payload = bot.answer(question, intent_override=intent_override)
                elapsed = time.perf_counter() - started
                payload["_metrics"] = {
                    "response_seconds": elapsed,
                    "candidate_count": len(payload.get("suggestions") or payload.get("fallback_candidates") or []),
                    "library_size": len(bot.queries),
                }
            _render_payload(payload, key_prefix="latest", bot=bot)
        st.session_state["current_exchange"] = {"question": question, "payload": payload}


if __name__ == "__main__":
    main()
