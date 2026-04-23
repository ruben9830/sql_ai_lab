from pathlib import Path
import csv
import importlib
import io
import json
import os
import re
import time
from datetime import datetime, timezone

import streamlit as st
from dotenv import load_dotenv
from mlb_today_data import load_today_snapshot, refresh_mlb_today

from demo_data import ensure_demo_database

try:
    from demo_data import import_csv_url_to_demo_sqlite, list_demo_tables, preview_demo_table
    PUBLIC_IMPORT_AVAILABLE = True
except ImportError:
    PUBLIC_IMPORT_AVAILABLE = False

    def import_csv_url_to_demo_sqlite(*args, **kwargs):
        return {
            "ok": False,
            "error": "Public CSV import helpers are unavailable.",
        }

    def list_demo_tables(*args, **kwargs):
        return []

    def preview_demo_table(*args, **kwargs):
        return {
            "ok": False,
            "error": "Table preview is unavailable in this environment. Pull latest code and restart.",
        }


def import_csv_bytes_to_demo_sqlite(
    db_path: Path,
    csv_bytes: bytes,
    table_name: str,
    replace_table: bool = False,
    max_rows: int = 50000,
    source_name: str = "uploaded_file.csv",
) -> dict:
    import csv
    import io
    import re
    import sqlite3

    def sanitize_identifier(value: str, fallback: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", (value or "").strip()).strip("_").lower()
        if not cleaned:
            cleaned = fallback
        if cleaned[0].isdigit():
            cleaned = f"c_{cleaned}"
        return cleaned

    def dedupe_names(names: list[str]) -> list[str]:
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

    table = sanitize_identifier(table_name, "public_dataset")
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

    rows = list(csv.reader(io.StringIO(decoded)))
    if not rows:
        return {"ok": False, "error": "CSV file is empty."}

    header = rows[0]
    if not header:
        return {"ok": False, "error": "CSV header row is missing."}

    columns = dedupe_names([sanitize_identifier(col, f"col_{idx + 1}") for idx, col in enumerate(rows[0])])
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
import sql_chatbot as sql_chatbot_module

importlib.reload(sql_chatbot_module)
SQLBibleChatbot = sql_chatbot_module.SQLBibleChatbot


BOT_CACHE_VERSION = "uploaded_csv_v3"
RECENT_DATASETS_FILE = Path("data/recent_uploaded_datasets.json")
DEMO_PROFILE_STATE_FILE = Path("data/demo_profile_state.json")


def _polish_title(raw_title: str) -> str:
    cleaned = re.sub(r"\s+", " ", (raw_title or "").replace("_", " ")).strip(" -\t\n.")
    cleaned = re.sub(
        r"\b(?:run\s+(?:in|on)|run\s+ub)\s+(?:taxpresit|commonpresit)\b",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\b(?:taxpresit|commonpresit)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -:\t\n.")
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


def _is_generic_title(title: str) -> bool:
    t = (title or "").strip().lower()
    if not t:
        return True
    return bool(
        re.fullmatch(r"query\s*\d*", t)
        or re.fullmatch(r"sql\s*template\s*\d*", t)
        or re.fullmatch(r"template\s*\d*", t)
    )


def _display_title_for_suggestion(suggestion: dict) -> str:
    raw_title = suggestion.get("title", "")
    polished = _polish_title(raw_title)
    if not _is_generic_title(polished):
        return polished

    sql_preview = (suggestion.get("sql_preview") or "").strip()
    if sql_preview:
        infer_fn = getattr(SQLBibleChatbot, "_infer_title_from_sql", None)
        if not callable(infer_fn):
            return polished or "Business Query"
        inferred = infer_fn(sql_preview)
        inferred_polished = _polish_title(str(inferred or ""))
        if inferred_polished and not _is_generic_title(inferred_polished):
            return inferred_polished

    return "Business Query"


def _render_suggestions(suggestions: list[dict]) -> None:
    if not suggestions:
        return

    st.markdown("### Recommended SQL Templates")
    st.caption("Curated options based on your question and detected filters.")
    for idx, s in enumerate(suggestions, start=1):
        polished_title = _display_title_for_suggestion(s)
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
        display_rows = [dict(zip(columns, row)) for row in rows]
        st.dataframe(display_rows, use_container_width=True)
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

    narrative_card = payload.get("narrative_card") or {}
    if narrative_card:
        title = str(narrative_card.get("title") or "Analyst Brief")
        summary = str(narrative_card.get("summary") or "")
        actions = narrative_card.get("actions") or []
        top_candidates = narrative_card.get("top_candidates") or []
        st.markdown(f"### {title}")
        if summary:
            st.write(summary)
        if top_candidates:
            st.markdown("**Top Candidates**")
            for idx, candidate in enumerate(top_candidates, start=1):
                name = str(candidate.get("name") or "Unknown")
                confidence = str(candidate.get("confidence") or "Medium")
                score = candidate.get("score")
                reason = str(candidate.get("reason") or "")
                player_id = str(candidate.get("player_id") or "").strip()
                who_line = f"{idx}. {name}"
                if player_id:
                    who_line += f" (ID: {player_id})"
                if score is not None:
                    who_line += f" | Score: {score}"
                who_line += f" | Confidence: {confidence}"
                st.write(who_line)
                if reason:
                    st.caption(reason)
        if actions:
            st.markdown("**Recommended Actions**")
            for action in actions[:3]:
                st.write(f"- {action}")

    analysis = payload.get("analysis") or []
    if analysis:
        st.markdown("### Top-Level Analysis")
        for point in analysis:
            st.write(f"- {point}")

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
    cache_key = f"{BOT_CACHE_VERSION}::bot::{sql_file}::{max_rows}::{database_url}::{','.join(sorted(allowed_tables or set()))}"
    cached_key = st.session_state.get("bot_key")
    bot = st.session_state.get("bot")
    if cached_key != cache_key or bot is None or not hasattr(bot, "answer_uploaded_table_question"):
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


def _queue_example(question: str) -> None:
    st.session_state["queued_question"] = question
    st.rerun()


def _queue_uploaded_table_question(
    db_path: Path,
    table_name: str,
    user_question: str,
    analysis_profile: str,
) -> tuple[bool, str]:
    prompt = (user_question or "").strip()
    if not prompt:
        return False, "Enter a question first."

    st.session_state["queued_uploaded_table_request"] = {
        "table_name": table_name,
        "question": prompt,
        "analysis_profile": analysis_profile,
    }
    return True, ""


def _load_recent_datasets() -> list[dict]:
    if not RECENT_DATASETS_FILE.exists():
        return []
    try:
        raw = json.loads(RECENT_DATASETS_FILE.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, dict)]
    except Exception:
        return []
    return []


def _save_recent_datasets(items: list[dict]) -> None:
    RECENT_DATASETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RECENT_DATASETS_FILE.write_text(json.dumps(items[:25], indent=2), encoding="utf-8")


def _record_recent_dataset(
    table_name: str,
    source_name: str,
    row_count: int,
    db_path: str,
    profile_name: str,
) -> None:
    items = _load_recent_datasets()
    filtered = [
        item
        for item in items
        if not (
            str(item.get("table_name", "")).strip().lower() == table_name.strip().lower()
            and str(item.get("profile_name", "")).strip().lower() == profile_name.strip().lower()
        )
    ]
    filtered.insert(
        0,
        {
            "table_name": table_name,
            "source_name": source_name,
            "row_count": int(row_count),
            "db_path": db_path,
            "profile_name": profile_name,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    _save_recent_datasets(filtered)


def _default_demo_profile_state() -> dict:
    return {
        "profiles": [{"name": "Default", "db_path": "data/demo_hackathon.db"}],
        "last_profile": "Default",
        "last_active_table_by_profile": {},
        "favorite_tables_by_profile": {},
    }


def _load_demo_profile_state() -> dict:
    if not DEMO_PROFILE_STATE_FILE.exists():
        return _default_demo_profile_state()
    try:
        raw = json.loads(DEMO_PROFILE_STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return _default_demo_profile_state()
    except Exception:
        return _default_demo_profile_state()

    state = _default_demo_profile_state()
    profiles = raw.get("profiles")
    if isinstance(profiles, list) and profiles:
        cleaned = []
        for item in profiles:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            db_path = str(item.get("db_path", "")).strip()
            if name and db_path:
                cleaned.append({"name": name, "db_path": db_path})
        if cleaned:
            state["profiles"] = cleaned

    state["last_profile"] = str(raw.get("last_profile") or state["last_profile"])
    state["last_active_table_by_profile"] = raw.get("last_active_table_by_profile") or {}
    state["favorite_tables_by_profile"] = raw.get("favorite_tables_by_profile") or {}
    return state


def _save_demo_profile_state(state: dict) -> None:
    DEMO_PROFILE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    DEMO_PROFILE_STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _profile_names(state: dict) -> list[str]:
    return [str(p.get("name", "")).strip() for p in state.get("profiles", []) if str(p.get("name", "")).strip()]


def _profile_db_path(state: dict, profile_name: str) -> str:
    for profile in state.get("profiles", []):
        if str(profile.get("name", "")).strip() == profile_name:
            return str(profile.get("db_path", "")).strip() or "data/demo_hackathon.db"
    return "data/demo_hackathon.db"


def _set_profile_db_path(state: dict, profile_name: str, db_path: str) -> None:
    for profile in state.get("profiles", []):
        if str(profile.get("name", "")).strip() == profile_name:
            profile["db_path"] = db_path
            return


def _add_profile(state: dict, profile_name: str, db_path: str) -> bool:
    name = profile_name.strip()
    if not name:
        return False
    existing = {n.lower() for n in _profile_names(state)}
    if name.lower() in existing:
        return False
    state["profiles"].append({"name": name, "db_path": db_path})
    state["last_profile"] = name
    return True


def _remove_profile(state: dict, profile_name: str) -> bool:
    if profile_name == "Default":
        return False
    before = len(state.get("profiles", []))
    state["profiles"] = [p for p in state.get("profiles", []) if str(p.get("name", "")).strip() != profile_name]
    if len(state["profiles"]) == before:
        return False
    if state.get("last_profile") == profile_name:
        state["last_profile"] = "Default"
    state.get("last_active_table_by_profile", {}).pop(profile_name, None)
    state.get("favorite_tables_by_profile", {}).pop(profile_name, None)
    return True


def _get_last_active_table(state: dict, profile_name: str) -> str:
    mapping = state.get("last_active_table_by_profile", {}) or {}
    return str(mapping.get(profile_name, "")).strip()


def _set_last_active_table(state: dict, profile_name: str, table_name: str) -> None:
    mapping = state.setdefault("last_active_table_by_profile", {})
    if table_name:
        mapping[profile_name] = table_name
    else:
        mapping.pop(profile_name, None)


def _favorite_tables(state: dict, profile_name: str) -> set[str]:
    mapping = state.get("favorite_tables_by_profile", {}) or {}
    raw = mapping.get(profile_name, [])
    if not isinstance(raw, list):
        return set()
    return {str(item).strip() for item in raw if str(item).strip()}


def _set_favorite_tables(state: dict, profile_name: str, tables: set[str]) -> None:
    mapping = state.setdefault("favorite_tables_by_profile", {})
    mapping[profile_name] = sorted(tables)


def _drop_demo_table(db_path: Path, table_name: str) -> tuple[bool, str]:
    import sqlite3

    safe_table = re.sub(r"[^a-zA-Z0-9_]", "", table_name or "")
    if not safe_table:
        return False, "Invalid table name."

    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute(f'DROP TABLE IF EXISTS "{safe_table}"')
            conn.commit()
        return True, ""
    except Exception as exc:
        return False, str(exc)


def _infer_domain_name_from_signals(table_names: list[str], columns: list[str]) -> str:
    def tokenize(values: list[str]) -> set[str]:
        tokens: set[str] = set()
        for value in values:
            parts = re.split(r"[^a-zA-Z0-9]+", (value or "").lower())
            for p in parts:
                if p:
                    tokens.add(p)
        return tokens

    table_tokens = tokenize(table_names)
    col_tokens = tokenize(columns)
    all_tokens = table_tokens.union(col_tokens)

    baseball = {
        "mlb", "baseball", "pitcher", "batting", "lineup", "inning", "homer", "homerun", "ops", "woba",
        "xwoba", "barrel", "barrel_batted_rate", "hard_hit", "hard_hit_percent", "sweet_spot", "sweet_spot_percent",
        "whiff", "whiff_percent", "swing", "swing_percent", "strikeout", "k_percent", "bb_percent", "pa",
        "player", "player_id", "bat", "slugging", "era", "whip", "gamepk", "starter", "bullpen", "atbat",
        "avg_best_speed", "avg_hyper_speed",
    }
    payroll = {
        "employer", "employers", "liability", "liabilities", "wage", "wages", "fein", "tax", "payroll",
        "quarter", "tp", "tpa", "filing", "ein", "claims",
    }
    travel = {
        "destination", "destinations", "country", "city", "beach", "resort", "hotel", "trip", "travel",
        "tourism", "flight", "airline", "budget", "attraction", "climate", "temperature",
    }

    scores = {
        "baseball": sum(1 for t in all_tokens if t in baseball),
        "payroll": sum(1 for t in all_tokens if t in payroll),
        "travel": sum(1 for t in all_tokens if t in travel),
    }

    if "mlb_games_today" in [t.lower() for t in table_names]:
        scores["baseball"] += 4
    if "employers" in [t.lower() for t in table_names]:
        scores["payroll"] += 3

    best = max(scores.items(), key=lambda item: item[1])[0]
    return best if scores[best] >= 2 else "generic"


def _infer_domain_name_from_text(value: str) -> str:
    text = (value or "").strip().lower()
    if not text:
        return "generic"
    if any(token in text for token in ("baseball", "mlb", "stats", "homer", "pitch", "bat")):
        return "baseball"
    if any(token in text for token in ("payroll", "employer", "liability", "wage", "bible")):
        return "payroll"
    if any(token in text for token in ("travel", "vacation", "destination", "tour", "trip")):
        return "travel"
    return "generic"


def _domain_config(domain_name: str) -> dict:
    configs = {
        "baseball": {
            "name": "baseball",
            "label": "Baseball",
            "examples": [
                ("Today's Probables", "Who are today's probable starters and which matchups look favorable?"),
                ("Lineup Check", "Show games with confirmed lineups and list batting orders."),
                ("Power Targets", "Who are strong home run candidates on today's slate?"),
            ],
            "chat_placeholder": "Ask your question (e.g. 'Which pitchers have favorable matchups today?')...",
            "show_mlb_tools": True,
        },
        "payroll": {
            "name": "payroll",
            "label": "SQL Bible Prime",
            "examples": [
                ("Top Employers", "Which employers had the largest month-over-month increase in liability amount due?"),
                ("Variance Analysis", "Show top FEINs by liability variance between start and end date."),
                ("Trends", "Summarize liability and wage amount trends by quarter and year."),
            ],
            "chat_placeholder": "Ask your question (e.g. 'Show me top employers by liability amount')...",
            "show_mlb_tools": False,
        },
        "travel": {
            "name": "travel",
            "label": "Travel",
            "examples": [
                ("Top Destinations", "Which destinations have the best rating-to-cost value?"),
                ("Budget Picks", "Show top destinations under my target budget with strongest reviews."),
                ("Weather-Friendly", "Which destinations have mild weather and strong traveler scores?"),
            ],
            "chat_placeholder": "Ask your question (e.g. 'Best beach destinations under $1500')...",
            "show_mlb_tools": False,
        },
        "generic": {
            "name": "generic",
            "label": "Dataset",
            "examples": [
                ("Overview", "Summarize key columns and high-level patterns in this dataset."),
                ("Top Entities", "Show top entities by the main numeric metric in this table."),
                ("Outliers", "Find unusual rows and explain why they stand out."),
            ],
            "chat_placeholder": "Ask your question about the active dataset...",
            "show_mlb_tools": False,
        },
    }
    return configs.get(domain_name, configs["generic"])


def _detect_demo_domain_context(
    db_path: Path,
    table_names: list[str],
    active_table: str,
    base_tables: set[str] | None = None,
    profile_name: str = "",
) -> dict:
    base_tables = {str(t).strip().lower() for t in (base_tables or set()) if str(t).strip()}
    upload_tables = [t for t in table_names if str(t).strip().lower() not in base_tables]

    profile_hint = _infer_domain_name_from_text(profile_name)
    if profile_hint != "generic":
        return _domain_config(profile_hint)

    probe_tables: list[str] = []
    if active_table:
        probe_tables.append(active_table)

    preferred_tables = upload_tables if upload_tables else table_names
    for t in preferred_tables:
        if t not in probe_tables:
            probe_tables.append(t)

    columns: list[str] = []
    for t in probe_tables[:3]:
        preview = preview_demo_table(db_path, t, limit=1)
        if preview.get("ok"):
            columns.extend([str(c) for c in (preview.get("columns") or [])])

    table_hint = _infer_domain_name_from_text(" ".join(probe_tables))
    if table_hint != "generic":
        return _domain_config(table_hint)

    domain_name = _infer_domain_name_from_signals(probe_tables, columns)
    return _domain_config(domain_name)


def _render_model_status(
    bot: SQLBibleChatbot,
    use_uploaded_table_in_chat: bool,
    active_table: str,
    domain_label: str,
) -> None:
    if bot.client is not None:
        st.success("OpenAI is available for uploaded CSV queries.")
    else:
        st.info("OpenAI is not available. Uploaded CSV queries use a local heuristic.")

    if use_uploaded_table_in_chat and active_table:
        st.caption(f"Main chat is bound to uploaded table: {active_table}")
    elif active_table:
        st.caption(f"Uploaded table selected: {active_table}")

    active_profile = str(st.session_state.get("uploaded_analysis_profile") or "General Manager")
    if domain_label == "Baseball":
        st.caption(f"Active analysis mode: {active_profile}")
    else:
        st.caption(f"Active dataset domain: {domain_label}")


def _mlb_auto_refresh_key(demo_db_path: str, target_date: str) -> str:
    return f"mlb_auto_refresh_last_attempt::{demo_db_path}::{target_date or 'today'}"


def _mlb_auto_refresh_remaining_minutes(
    demo_db_path: str,
    target_date: str,
    cooldown_seconds: int = 15 * 60,
) -> int:
    key = _mlb_auto_refresh_key(demo_db_path, target_date)
    now_epoch = time.time()
    last_attempt = float(st.session_state.get(key, 0.0) or 0.0)
    seconds_left = cooldown_seconds - (now_epoch - last_attempt)
    if seconds_left <= 0:
        return 0
    return max(1, int(seconds_left // 60))


def _render_today_slate_panel(demo_db_path: str, auto_refresh_on_zero_games: bool = False) -> None:
    st.subheader("Today's MLB Slate")
    snapshot = load_today_snapshot(Path(demo_db_path))
    games_count = int(snapshot.get("games_count", 0))
    target_date = str(snapshot.get("target_date") or "")

    if auto_refresh_on_zero_games and games_count == 0:
        key = _mlb_auto_refresh_key(demo_db_path, target_date)
        now_epoch = time.time()
        last_attempt = float(st.session_state.get(key, 0.0) or 0.0)
        cooldown_seconds = 15 * 60
        if now_epoch - last_attempt >= cooldown_seconds:
            with st.spinner("No games found. Running automatic MLB refresh..."):
                refresh_result = refresh_mlb_today(Path(demo_db_path), target_date=(target_date or None))
            st.session_state[key] = now_epoch
            if refresh_result.ok:
                st.info(
                    f"Auto-refresh completed: {refresh_result.games_loaded} games, "
                    f"{refresh_result.lineup_rows_loaded} lineup rows, "
                    f"{refresh_result.starter_rows_loaded} starter rows."
                )
            else:
                st.warning(f"Auto-refresh failed: {refresh_result.error}")

            snapshot = load_today_snapshot(Path(demo_db_path), target_date=(target_date or None))
            games_count = int(snapshot.get("games_count", 0))
        else:
            minutes_left = _mlb_auto_refresh_remaining_minutes(demo_db_path, target_date, cooldown_seconds)
            st.caption(f"Auto-refresh cooldown active ({minutes_left} minute(s) remaining).")

    freshness = snapshot.get("freshness") or {}

    c1, c2, c3 = st.columns(3)
    c1.metric("Games", games_count)
    c2.metric("Confirmed Lineups", int(snapshot.get("lineups_confirmed_total", 0)))
    c3.metric("Rows Loaded", int(freshness.get("records_loaded", 0)))

    last_success = str(freshness.get("last_success_at") or "")
    stale_minutes: int | None = None
    if last_success:
        try:
            parsed = datetime.strptime(last_success, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            stale_minutes = int((datetime.now(timezone.utc) - parsed).total_seconds() // 60)
        except ValueError:
            stale_minutes = None

    if last_success:
        st.caption(f"Last refresh: {last_success}")
    else:
        st.caption("No MLB refresh has run yet.")

    if stale_minutes is None and last_success:
        st.warning("Refresh timestamp format is invalid. Run a manual MLB refresh.")
    elif stale_minutes is not None and stale_minutes > 90:
        st.error(f"Data is stale ({stale_minutes} minutes old). Click 'Refresh MLB Data Now'.")
    elif stale_minutes is not None and stale_minutes > 30:
        st.warning(f"Data may be stale ({stale_minutes} minutes old). Consider refreshing now.")
    elif stale_minutes is not None:
        st.success(f"Data freshness looks good ({stale_minutes} minutes old).")

    if games_count == 0:
        st.warning(
            f"No games found for {target_date or 'today'}. This can happen on off-days or if data is stale."
        )

    notes = str(freshness.get("notes") or "")
    if notes:
        st.caption(notes)

    games = snapshot.get("games") or []
    if games:
        with st.expander("View Games"):
            for game in games:
                away = game.get("away_team", "")
                home = game.get("home_team", "")
                status = game.get("status", "")
                hp = game.get("home_probable", "TBD")
                ap = game.get("away_probable", "TBD")
                l_home = "Y" if int(game.get("home_lineup_confirmed", 0)) == 1 else "N"
                l_away = "Y" if int(game.get("away_lineup_confirmed", 0)) == 1 else "N"
                st.write(f"{away} @ {home} | {status}")
                st.caption(f"Probables: {ap} vs {hp} | Lineups confirmed: away={l_away}, home={l_home}")


def main() -> None:
    load_dotenv()
    st.set_page_config(page_title="SQL AI Lab", page_icon="🧠", layout="wide")
    st.title("🧠 SQL AI Lab")
    st.write("Convert business questions into SQL queries. Review, execute, and export results—all in seconds.")

    if "queued_question" not in st.session_state:
        st.session_state["queued_question"] = ""
    if "current_exchange" not in st.session_state:
        st.session_state["current_exchange"] = None
    if "queued_uploaded_table_request" not in st.session_state:
        st.session_state["queued_uploaded_table_request"] = None

    start_date = ""
    end_date = ""
    fein = ""
    employer_id = ""
    quarter = 0
    year = 0
    domain_context = _domain_config("payroll")
    demo_db_path = "data/demo_hackathon.db"
    sql_file = "data/SQL_BIBLE_PRIME.sql"
    sidebar_profile_name = ""
    sidebar_active_table = ""
    sidebar_demo_db_path = demo_db_path
    sidebar_base_tables: set[str] = {"employers", "liabilities", "wage_reports"}
    sidebar_uploaded_tables: list[str] = []
    sidebar_favorite_tables: set[str] = set()
    sidebar_all_tables_now: list[str] = []

    with st.sidebar:
        st.header("⚙️ Configuration")
        max_rows = st.number_input("Max rows", min_value=1, max_value=5000, value=200, step=50)
        data_mode = st.radio("Data mode", options=["Demo (SQLite)", "Enterprise (Postgres)"], index=0)
        st.caption("Safe mode: All queries are read-only (SELECT/WITH only).")

        if data_mode == "Demo (SQLite)":
            st.caption("Using the shared query library behind the scenes.")
            profile_state = _load_demo_profile_state()
            profile_names = _profile_names(profile_state)
            if not profile_names:
                profile_state = _default_demo_profile_state()
                _save_demo_profile_state(profile_state)
                profile_names = _profile_names(profile_state)

            default_profile = str(profile_state.get("last_profile") or profile_names[0])
            if default_profile not in profile_names:
                default_profile = profile_names[0]

            demo_profile_name = st.selectbox(
                "Dataset workspace",
                options=profile_names,
                index=profile_names.index(default_profile) if default_profile in profile_names else 0,
                key="demo_profile_name",
                help="Each workspace keeps its own demo DB and dataset history.",
            )
            sidebar_profile_name = demo_profile_name

            if profile_state.get("last_profile") != demo_profile_name:
                profile_state["last_profile"] = demo_profile_name
                _save_demo_profile_state(profile_state)

            with st.expander("Manage Workspaces"):
                new_profile_name = st.text_input(
                    "New workspace name",
                    value="",
                    key="new_demo_profile_name",
                    placeholder="e.g. Baseball, Finance, Claims",
                )
                if st.button("Create Workspace", use_container_width=True):
                    slug = re.sub(r"[^a-zA-Z0-9_]+", "_", new_profile_name.strip().lower()).strip("_") or "workspace"
                    default_db_path = f"data/demo_{slug}.db"
                    state_for_create = _load_demo_profile_state()
                    if _add_profile(state_for_create, new_profile_name.strip(), default_db_path):
                        _save_demo_profile_state(state_for_create)
                        st.success(f"Workspace '{new_profile_name.strip()}' created.")
                        st.rerun()
                    else:
                        st.error("Could not create workspace. Use a unique non-empty name.")

                if demo_profile_name != "Default" and st.button("Remove Current Workspace", use_container_width=True):
                    state_for_remove = _load_demo_profile_state()
                    if _remove_profile(state_for_remove, demo_profile_name):
                        _save_demo_profile_state(state_for_remove)
                        st.success(f"Workspace '{demo_profile_name}' removed.")
                        st.rerun()
                    else:
                        st.error("Could not remove workspace.")

            demo_db_path = _profile_db_path(profile_state, demo_profile_name)
            sidebar_demo_db_path = demo_db_path
            custom_db_path = st.text_input("Workspace DB file", value=demo_db_path, key="demo_db_path_input")
            if custom_db_path.strip() and custom_db_path.strip() != demo_db_path:
                profile_state = _load_demo_profile_state()
                _set_profile_db_path(profile_state, demo_profile_name, custom_db_path.strip())
                _save_demo_profile_state(profile_state)
                demo_db_path = custom_db_path.strip()

            ensure_demo_database(Path(demo_db_path))
            database_url = f"sqlite:///{demo_db_path}"
            sidebar_base_tables = {"employers", "liabilities", "wage_reports"}

            all_tables_now = list_demo_tables(Path(demo_db_path))
            sidebar_all_tables_now = all_tables_now
            persisted_uploaded_tables = sorted([t for t in all_tables_now if t not in sidebar_base_tables])
            sidebar_uploaded_tables = persisted_uploaded_tables
            st.session_state["demo_extra_tables"] = persisted_uploaded_tables

            profile_state = _load_demo_profile_state()
            favorite_tables = _favorite_tables(profile_state, demo_profile_name)
            sidebar_favorite_tables = favorite_tables
            last_active_table = _get_last_active_table(profile_state, demo_profile_name)

            if persisted_uploaded_tables:
                active_from_state = str(st.session_state.get("active_uploaded_table") or "").strip()
                if active_from_state not in persisted_uploaded_tables:
                    if last_active_table in persisted_uploaded_tables:
                        st.session_state["active_uploaded_table"] = last_active_table
                        st.session_state["uploaded_query_table"] = last_active_table
                    else:
                        st.session_state["active_uploaded_table"] = persisted_uploaded_tables[0]
                        st.session_state["uploaded_query_table"] = persisted_uploaded_tables[0]

            sidebar_active_table = str(st.session_state.get("active_uploaded_table") or "").strip()

            domain_context = _detect_demo_domain_context(
                Path(demo_db_path),
                all_tables_now,
                sidebar_active_table,
                sidebar_base_tables,
                demo_profile_name,
            )

            allowed_tables = sidebar_base_tables.union(set(persisted_uploaded_tables))
            st.success("✓ Demo mode active — no VPN or setup required.")
            st.caption(f"Active domain: {domain_context['label']}")

            if bool(domain_context.get("show_mlb_tools")):
                with st.expander("MLB Live Refresh"):
                    st.caption("Refresh today's games, probable starters, and lineups from MLB Stats API.")
                    st.checkbox(
                        "Auto-refresh when games show 0",
                        value=True,
                        key="auto_refresh_zero_games",
                        help="If enabled, the app attempts one automatic refresh when the slate is empty (15-minute cooldown).",
                    )
                    if st.button("Refresh MLB Data Now", use_container_width=True):
                        refresh_result = refresh_mlb_today(Path(demo_db_path))
                        if refresh_result.ok:
                            st.success(
                                f"Loaded {refresh_result.games_loaded} games, "
                                f"{refresh_result.lineup_rows_loaded} lineup rows, "
                                f"{refresh_result.starter_rows_loaded} starter rows."
                            )
                        else:
                            st.error(f"MLB refresh failed: {refresh_result.error}")

                    with st.expander("Automation Command"):
                        st.code(
                            f'.\\.venv\\Scripts\\python.exe src\\ingest_mlb_today.py --db "{demo_db_path}"',
                            language="powershell",
                        )

                if bool(st.session_state.get("auto_refresh_zero_games", True)):
                    cooldown_target_date = time.strftime("%Y-%m-%d")
                    cooldown_left = _mlb_auto_refresh_remaining_minutes(demo_db_path, cooldown_target_date)
                    if cooldown_left > 0:
                        st.caption(f"Auto-refresh cooldown: {cooldown_left} minute(s) remaining")
                    else:
                        st.caption("Auto-refresh cooldown: ready")

            with st.expander("Public Data Lake Import (CSV Upload)"):
                st.caption("Upload a CSV file from your computer and load it into demo SQLite.")
                uploaded_csv = st.file_uploader(
                    "Upload CSV file",
                    type=["csv"],
                    help="Recommended: upload a CSV file directly from your machine.",
                )
                import_table = st.text_input("Table name", value="public_dataset")
                import_replace = st.checkbox("Replace table if it already exists", value=True)
                import_max_rows = st.number_input(
                    "Max rows to import",
                    min_value=100,
                    max_value=200000,
                    value=50000,
                    step=1000,
                )
                if st.button("Import Uploaded CSV", use_container_width=True):
                    if uploaded_csv is None:
                        st.error("Choose a CSV file first.")
                    else:
                        import_result = import_csv_bytes_to_demo_sqlite(
                            db_path=Path(demo_db_path),
                            csv_bytes=uploaded_csv.getvalue(),
                            table_name=import_table,
                            replace_table=import_replace,
                            max_rows=int(import_max_rows),
                            source_name=uploaded_csv.name,
                        )
                        if not import_result.get("ok"):
                            st.error(import_result.get("error", "Import failed."))
                        else:
                            imported_table = str(import_result.get("table"))
                            extras = set(persisted_uploaded_tables)
                            extras.add(imported_table)
                            st.session_state["demo_extra_tables"] = sorted(extras)
                            st.session_state["active_uploaded_table"] = imported_table
                            st.session_state["uploaded_query_table"] = imported_table
                            st.success(
                                f"Imported {import_result.get('row_count', 0)} rows into table '{imported_table}'."
                            )
                            st.caption(f"Columns: {', '.join(import_result.get('columns', []))}")
                            st.caption(f"Source file: {import_result.get('source_name', '')}")
                            _record_recent_dataset(
                                table_name=imported_table,
                                source_name=str(import_result.get("source_name", "")),
                                row_count=int(import_result.get("row_count", 0)),
                                db_path=demo_db_path,
                                profile_name=demo_profile_name,
                            )

                            profile_state = _load_demo_profile_state()
                            _set_last_active_table(profile_state, demo_profile_name, imported_table)
                            _save_demo_profile_state(profile_state)
                            st.rerun()

                all_demo_tables = list_demo_tables(Path(demo_db_path))
                if all_demo_tables:
                    st.caption("Preview table is for inspecting any table already loaded in this workspace.")
                    preview_table_name = st.selectbox("Preview table", options=all_demo_tables)
                    preview_limit = st.number_input(
                        "Preview rows",
                        min_value=5,
                        max_value=100,
                        value=20,
                        step=5,
                    )
                    if st.button("Preview Selected Table", use_container_width=True):
                        preview = preview_demo_table(Path(demo_db_path), preview_table_name, limit=int(preview_limit))
                        if preview.get("ok"):
                            st.write(f"Showing {preview.get('row_count', 0)} rows from {preview.get('table')}")
                            columns = preview.get("columns", [])
                            rows = preview.get("rows", [])
                            display_rows = [dict(zip(columns, row)) for row in rows]
                            st.dataframe(display_rows, use_container_width=True)
                        else:
                            st.error(preview.get("error", "Preview failed."))

                uploaded_tables = sorted(set(st.session_state.get("demo_extra_tables", [])))

                favorite_list = [t for t in uploaded_tables if t in favorite_tables]
                if favorite_list:
                    with st.expander("Favorite Datasets"):
                        st.caption("Pinned datasets for this workspace.")
                        for fav_table in favorite_list:
                            c1, c2 = st.columns(2)
                            if c1.button("Use", key=f"fav_use_{demo_profile_name}_{fav_table}"):
                                st.session_state["active_uploaded_table"] = fav_table
                                st.session_state["uploaded_query_table"] = fav_table
                                profile_state = _load_demo_profile_state()
                                _set_last_active_table(profile_state, demo_profile_name, fav_table)
                                _save_demo_profile_state(profile_state)
                                st.rerun()
                            if c2.button("Unfavorite", key=f"fav_remove_{demo_profile_name}_{fav_table}"):
                                profile_state = _load_demo_profile_state()
                                current_favorites = _favorite_tables(profile_state, demo_profile_name)
                                current_favorites.discard(fav_table)
                                _set_favorite_tables(profile_state, demo_profile_name, current_favorites)
                                _save_demo_profile_state(profile_state)
                                st.rerun()
                            st.caption(fav_table)

                recent_items = [
                    item
                    for item in _load_recent_datasets()
                    if (
                        str(item.get("profile_name", "")).strip() == demo_profile_name
                        or (
                            not str(item.get("profile_name", "")).strip()
                            and str(item.get("db_path", "")).strip() == demo_db_path
                        )
                    )
                ]
                if recent_items:
                    with st.expander("Recent Datasets"):
                        st.caption("Previously imported datasets are kept in your demo DB until removed.")
                        for item in recent_items[:10]:
                            table_name = str(item.get("table_name", "")).strip()
                            if not table_name:
                                continue
                            src = str(item.get("source_name", ""))
                            rows = int(item.get("row_count", 0))
                            updated = str(item.get("updated_at", ""))
                            st.write(f"{table_name} • {rows} rows • {src} • {updated}")
                            c1, c2, c3 = st.columns(3)
                            if c1.button("Use", key=f"use_recent_{table_name}"):
                                st.session_state["active_uploaded_table"] = table_name
                                st.session_state["uploaded_query_table"] = table_name
                                profile_state = _load_demo_profile_state()
                                _set_last_active_table(profile_state, demo_profile_name, table_name)
                                _save_demo_profile_state(profile_state)
                                st.rerun()
                            fav_label = "Unfavorite" if table_name in favorite_tables else "Favorite"
                            if c2.button(fav_label, key=f"fav_recent_{demo_profile_name}_{table_name}"):
                                profile_state = _load_demo_profile_state()
                                current_favorites = _favorite_tables(profile_state, demo_profile_name)
                                if table_name in current_favorites:
                                    current_favorites.discard(table_name)
                                else:
                                    current_favorites.add(table_name)
                                _set_favorite_tables(profile_state, demo_profile_name, current_favorites)
                                _save_demo_profile_state(profile_state)
                                st.rerun()
                            if c3.button("Remove", key=f"remove_recent_{demo_profile_name}_{table_name}"):
                                ok, err = _drop_demo_table(Path(demo_db_path), table_name)
                                if not ok:
                                    st.error(f"Failed to remove '{table_name}': {err}")
                                else:
                                    all_recent = _load_recent_datasets()
                                    filtered_items = [
                                        it
                                        for it in all_recent
                                        if not (
                                            str(it.get("table_name", "")).strip().lower() == table_name.lower()
                                            and str(it.get("profile_name", "")).strip() == demo_profile_name
                                        )
                                    ]
                                    _save_recent_datasets(filtered_items)
                                    remaining = [t for t in uploaded_tables if t.lower() != table_name.lower()]
                                    st.session_state["demo_extra_tables"] = remaining
                                    if str(st.session_state.get("active_uploaded_table", "")).strip().lower() == table_name.lower():
                                        st.session_state["active_uploaded_table"] = remaining[0] if remaining else ""
                                    profile_state = _load_demo_profile_state()
                                    current_favorites = _favorite_tables(profile_state, demo_profile_name)
                                    if table_name in current_favorites:
                                        current_favorites.discard(table_name)
                                        _set_favorite_tables(profile_state, demo_profile_name, current_favorites)
                                    if _get_last_active_table(profile_state, demo_profile_name).lower() == table_name.lower():
                                        _set_last_active_table(profile_state, demo_profile_name, remaining[0] if remaining else "")
                                    _save_demo_profile_state(profile_state)
                                    st.success(f"Removed dataset table '{table_name}'.")
                                    st.rerun()

                with st.expander("Query Uploaded CSV"):
                    if not uploaded_tables:
                        st.info("Import a CSV file above to enable uploaded-table chat queries.")
                    else:
                        ordered_tables = favorite_list + [t for t in uploaded_tables if t not in favorite_list]
                        default_tables = st.session_state.get("uploaded_query_tables") or []
                        if not isinstance(default_tables, list):
                            default_tables = []
                        default_tables = [str(t) for t in default_tables if str(t).strip() in ordered_tables]
                        if not default_tables and ordered_tables:
                            default_tables = [ordered_tables[0]]

                        query_tables = st.multiselect(
                            "Uploaded tables (tandem)",
                            options=ordered_tables,
                            default=default_tables,
                            key="uploaded_query_tables",
                            help="Pick one table for a single dataset, or pick two tables to combine them in tandem.",
                        )
                        query_tables = [str(t).strip() for t in query_tables if str(t).strip()]
                        if not query_tables and ordered_tables:
                            query_tables = [ordered_tables[0]]
                        query_table = query_tables[0] if query_tables else ""
                        st.session_state["active_uploaded_table"] = query_table
                        st.session_state["uploaded_query_table"] = query_table
                        domain_context = _detect_demo_domain_context(
                            Path(demo_db_path),
                            all_tables_now,
                            query_table,
                            sidebar_base_tables,
                            demo_profile_name,
                        )
                        profile_state = _load_demo_profile_state()
                        if _get_last_active_table(profile_state, demo_profile_name) != query_table:
                            _set_last_active_table(profile_state, demo_profile_name, query_table)
                            _save_demo_profile_state(profile_state)
                        query_preview = preview_demo_table(Path(demo_db_path), query_table, limit=1)
                        if query_preview.get("ok"):
                            cols = query_preview.get("columns", [])
                            if cols:
                                st.caption(f"Columns: {', '.join(cols[:12])}")

                        if len(query_tables) > 1:
                            st.caption(f"Tandem mode enabled: combining {len(query_tables)} tables.")

                        st.checkbox(
                            "Use selected uploaded CSV in main chat",
                            value=True,
                            key="use_uploaded_table_in_chat",
                            help="When enabled, questions typed in the main chat bar will query the selected uploaded table.",
                        )

                        if domain_context["name"] == "baseball":
                            st.selectbox(
                                "Analysis mode",
                                options=[
                                    "General Manager",
                                    "Hitting Analyst",
                                    "Pitching Analyst",
                                    "DFS Mode",
                                    "Betting Mode",
                                ],
                                key="uploaded_analysis_profile",
                                help="Changes ranking logic and narrative framing for uploaded CSV answers.",
                            )
                        else:
                            st.session_state["uploaded_analysis_profile"] = "General Manager"
                            st.caption("Analysis mode auto-set for non-baseball datasets.")

                        uploaded_table_question = st.text_input(
                            "Ask about this uploaded table",
                            value="",
                            key="uploaded_table_question",
                            placeholder="e.g. show the top 10 rows by total amount",
                        )
                        if st.button("Ask Uploaded Table in Chat", use_container_width=True):
                            if len(query_tables) > 1:
                                st.session_state["queued_uploaded_table_request"] = {
                                    "table_names": query_tables,
                                    "question": uploaded_table_question,
                                    "analysis_profile": str(st.session_state.get("uploaded_analysis_profile") or "General Manager"),
                                }
                            else:
                                ok, error = _queue_uploaded_table_question(
                                    db_path=Path(demo_db_path),
                                    table_name=str(query_table or ""),
                                    user_question=uploaded_table_question,
                                    analysis_profile=str(st.session_state.get("uploaded_analysis_profile") or "General Manager"),
                                )
                                if not ok:
                                    st.error(error)
                                else:
                                    st.rerun()
        else:
            sql_file = st.text_input("Query template source", value=sql_file)
            db_override = st.text_input("DATABASE_URL override (optional)", value="", type="password")
            database_url = db_override.strip() or os.getenv("DATABASE_URL", "").strip()
            raw_allowed = os.getenv("ALLOWED_TABLES", "").strip()
            allowed_tables = {item.strip().lower() for item in raw_allowed.split(",") if item.strip()} if raw_allowed else None
            st.info("Enterprise mode — connects to your Postgres instance.")
            domain_context = _domain_config("payroll")

        st.divider()
        if st.button("Clear current result", use_container_width=True):
            st.session_state.pop("current_exchange", None)
            st.session_state.pop("queued_question", None)
            st.rerun()

        st.divider()
        if data_mode == "Enterprise (Postgres)":
            st.subheader("🔍 Query Filters")
            st.caption("Refine enterprise SQL results by date range and identifiers.")
            start_date = st.text_input("Start date (YYYY-MM-DD)", value="")
            end_date = st.text_input("End date (YYYY-MM-DD)", value="")
            fein = st.text_input("FEIN", value="", placeholder="12-3456789")
            employer_id = st.text_input("Employer ID", value="")
            quarter = st.number_input("Quarter", min_value=0, max_value=4, value=0, step=1)
            year = st.number_input("Year", min_value=0, max_value=2100, value=0, step=1)
        else:
            st.subheader("📚 Dataset Context")
            st.caption(f"Demo mode is currently tailored to: {domain_context['label']}")
            st.caption("Load another dataset and this sidebar will automatically adapt.")

    st.subheader("Quick Start Examples")
    st.caption("Run a dataset-aware example instantly.")
    example_col1, example_col2, example_col3 = st.columns(3)
    examples = domain_context.get("examples", [])
    if len(examples) >= 3:
        if example_col1.button(str(examples[0][0]), use_container_width=True, key="main_example_1"):
            _queue_example(str(examples[0][1]))
        if example_col2.button(str(examples[1][0]), use_container_width=True, key="main_example_2"):
            _queue_example(str(examples[1][1]))
        if example_col3.button(str(examples[2][0]), use_container_width=True, key="main_example_3"):
            _queue_example(str(examples[2][1]))

    bot = _get_bot(
        sql_file=sql_file,
        max_rows=int(max_rows),
        database_url=database_url,
        allowed_tables=allowed_tables,
    )
    col1, col2 = st.columns(2)
    col1.info(f"📚 Library: {len(bot.queries)} query templates loaded")
    health = bot.probe_connection()
    if health.get("ok"):
        col2.success(f"✓ {health.get('message', 'Database connected.')}")
    else:
        col2.warning(f"⚠️ {health.get('message', 'No database configured.')}")

    active_uploaded_table = str(st.session_state.get("active_uploaded_table") or "").strip()
    use_uploaded_table_in_chat = bool(st.session_state.get("use_uploaded_table_in_chat"))
    _render_model_status(bot, use_uploaded_table_in_chat, active_uploaded_table, domain_context["label"])

    if data_mode == "Demo (SQLite)" and bool(domain_context.get("show_mlb_tools")):
        _render_today_slate_panel(
            demo_db_path,
            auto_refresh_on_zero_games=bool(st.session_state.get("auto_refresh_zero_games", True)),
        )

    # Chat input
    chat_placeholder = str(domain_context.get("chat_placeholder") or "Ask your question...")
    chat_question = st.chat_input(chat_placeholder)
    uploaded_request = st.session_state.get("queued_uploaded_table_request")
    question = (chat_question or "").strip() or st.session_state.pop("queued_question", "")

    # Results render below chat input (no scroll needed)
    if uploaded_request:
        uploaded_question = str(uploaded_request.get("question") or "").strip()
        analysis_profile = str(uploaded_request.get("analysis_profile") or st.session_state.get("uploaded_analysis_profile") or "General Manager")

        table_names = uploaded_request.get("table_names") or []
        if isinstance(table_names, str):
            table_names = [table_names]
        table_names = [str(t).strip() for t in table_names if str(t).strip()]
        table_name = str(uploaded_request.get("table_name") or "").strip()

        if not uploaded_question or (not table_name and len(table_names) < 2):
            st.session_state["queued_uploaded_table_request"] = None
        else:
            with st.chat_message("user"):
                if table_names:
                    st.markdown(f"[{', '.join(table_names)}] {uploaded_question}")
                else:
                    st.markdown(f"[{table_name}] {uploaded_question}")

            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    started = time.perf_counter()
                    if table_names and len(table_names) > 1:
                        payload = bot.answer_uploaded_tables_question(
                            table_names,
                            uploaded_question,
                            analysis_profile=analysis_profile,
                        )
                    else:
                        payload = bot.answer_uploaded_table_question(
                            table_name,
                            uploaded_question,
                            analysis_profile=analysis_profile,
                        )
                    elapsed = time.perf_counter() - started
                    payload["_metrics"] = {
                        "response_seconds": elapsed,
                        "candidate_count": 0,
                        "library_size": len(bot.queries),
                    }
                _render_payload(payload, key_prefix="uploaded", bot=bot)
            st.session_state["queued_uploaded_table_request"] = None
            st.session_state["current_exchange"] = {"question": uploaded_question, "payload": payload}
            st.stop()

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
                selected_uploaded_tables = [str(t).strip() for t in (st.session_state.get("uploaded_query_tables") or []) if str(t).strip()]
                if use_uploaded_table_in_chat and selected_uploaded_tables:
                    if len(selected_uploaded_tables) > 1:
                        payload = bot.answer_uploaded_tables_question(
                            selected_uploaded_tables,
                            question,
                            analysis_profile=str(st.session_state.get("uploaded_analysis_profile") or "General Manager"),
                        )
                    else:
                        payload = bot.answer_uploaded_table_question(
                            selected_uploaded_tables[0],
                            question,
                            analysis_profile=str(st.session_state.get("uploaded_analysis_profile") or "General Manager"),
                        )
                else:
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
