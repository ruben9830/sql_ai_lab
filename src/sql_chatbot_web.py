from pathlib import Path
import csv
import io

import streamlit as st
from dotenv import load_dotenv

from sql_chatbot import SQLBibleChatbot


def _render_suggestions(suggestions: list[dict]) -> None:
    if not suggestions:
        return

    st.markdown("### Suggested queries")
    for s in suggestions:
        st.markdown(f"**[{s['id']}] {s['title']}**")
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


def _render_payload(payload: dict, key_prefix: str, bot: SQLBibleChatbot | None = None) -> None:
    mode = payload.get("mode", "unknown")
    st.caption(f"Mode: {mode}")

    intent = payload.get("intent") or {}
    if any(intent.values()):
        with st.expander("Detected filters"):
            st.json(intent)

    plan = payload.get("plan")
    if plan:
        with st.expander("LLM plan"):
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

    suggestions = payload.get("suggestions") or payload.get("fallback_candidates") or []
    _render_suggestions(suggestions)


def _get_bot(sql_file: str, max_rows: int) -> SQLBibleChatbot:
    cache_key = f"bot::{sql_file}::{max_rows}"
    cached_key = st.session_state.get("bot_key")
    if cached_key != cache_key:
        st.session_state["bot"] = SQLBibleChatbot(sql_file=Path(sql_file), max_rows=max_rows)
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
    st.set_page_config(page_title="SQL AI Lab", page_icon="🧠", layout="wide")
    st.title("SQL AI Lab")
    st.write("Ask a question and get relevant SQL snippets, with optional read-only query execution.")

    with st.sidebar:
        st.header("Settings")
        sql_file = st.text_input("SQL file", value="data/SQL_BIBLE_PRIME.sql")
        max_rows = st.number_input("Max rows", min_value=1, max_value=5000, value=200, step=50)
        st.caption("Read-only mode allows only SELECT/WITH statements.")
        st.divider()
        st.subheader("Quick filters")
        st.caption("Optional: these help intent extraction even if your question is short.")
        start_date = st.text_input("Start date (YYYY-MM-DD)", value="")
        end_date = st.text_input("End date (YYYY-MM-DD)", value="")
        fein = st.text_input("FEIN", value="", placeholder="12-3456789")
        employer_id = st.text_input("Employer ID", value="")
        quarter = st.number_input("Quarter", min_value=0, max_value=4, value=0, step=1)
        year = st.number_input("Year", min_value=0, max_value=2100, value=0, step=1)

    bot = _get_bot(sql_file=sql_file, max_rows=int(max_rows))
    st.info(f"Loaded {len(bot.queries)} query snippets.")

    if "history" not in st.session_state:
        st.session_state["history"] = []

    for idx, item in enumerate(st.session_state["history"]):
        with st.chat_message("user"):
            st.markdown(item["question"])
        with st.chat_message("assistant"):
            _render_payload(item["payload"], key_prefix=f"history_{idx}", bot=bot)

    question = st.chat_input("Ask about employers, liabilities, wage reports, or FEIN patterns...")
    if not question:
        return

    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            intent_override = _build_intent_override(
                start_date=start_date,
                end_date=end_date,
                fein=fein,
                employer_id=employer_id,
                quarter=int(quarter),
                year=int(year),
            )
            payload = bot.answer(question, intent_override=intent_override)
        _render_payload(payload, key_prefix="latest", bot=bot)

    st.session_state["history"].append({"question": question, "payload": payload})


if __name__ == "__main__":
    main()
