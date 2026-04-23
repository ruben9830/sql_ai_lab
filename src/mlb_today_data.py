from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from urllib.request import urlopen


SCHEDULE_URL = (
    "https://statsapi.mlb.com/api/v1/schedule"
    "?sportId=1&date={target_date}&hydrate=probablePitcher"
)
LIVE_FEED_URL = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"


@dataclass
class RefreshResult:
    ok: bool
    target_date: str
    games_loaded: int
    lineup_rows_loaded: int
    starter_rows_loaded: int
    updated_at: str
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "target_date": self.target_date,
            "games_loaded": self.games_loaded,
            "lineup_rows_loaded": self.lineup_rows_loaded,
            "starter_rows_loaded": self.starter_rows_loaded,
            "updated_at": self.updated_at,
            "error": self.error,
        }


def _now_utc_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _http_get_json(url: str, timeout_seconds: int = 20) -> dict[str, Any]:
    with urlopen(url, timeout=timeout_seconds) as response:
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw)
    return parsed if isinstance(parsed, dict) else {}


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except Exception:
        return None


def _safe_str(value: Any) -> str:
    return "" if value is None else str(value)


def ensure_tables(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mlb_games_today (
                game_pk INTEGER PRIMARY KEY,
                target_date TEXT NOT NULL,
                start_time_utc TEXT,
                detailed_state TEXT,
                status_code TEXT,
                abstract_state TEXT,
                home_team_id INTEGER,
                home_team_name TEXT,
                away_team_id INTEGER,
                away_team_name TEXT,
                venue_id INTEGER,
                venue_name TEXT,
                home_probable_pitcher_id INTEGER,
                home_probable_pitcher_name TEXT,
                away_probable_pitcher_id INTEGER,
                away_probable_pitcher_name TEXT,
                lineup_confirmed_home INTEGER DEFAULT 0,
                lineup_confirmed_away INTEGER DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mlb_lineups_today (
                game_pk INTEGER NOT NULL,
                team_side TEXT NOT NULL,
                batting_order_slot INTEGER NOT NULL,
                player_id INTEGER,
                player_name TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (game_pk, team_side, batting_order_slot)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mlb_starting_pitchers_today (
                game_pk INTEGER NOT NULL,
                team_side TEXT NOT NULL,
                pitcher_id INTEGER,
                pitcher_name TEXT,
                throwing_hand TEXT,
                source TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (game_pk, team_side, source)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mlb_data_freshness (
                source_name TEXT PRIMARY KEY,
                last_success_at TEXT,
                records_loaded INTEGER,
                notes TEXT
            )
            """
        )

        conn.commit()


def _extract_lineup_rows(game_pk: int, team_side: str, team_box: dict[str, Any], updated_at: str) -> list[tuple]:
    rows: list[tuple] = []
    batting_order = team_box.get("battingOrder") or []
    players = team_box.get("players") or {}

    for idx, player_id in enumerate(batting_order, start=1):
        player_key = f"ID{player_id}"
        player_obj = players.get(player_key) or {}
        person = player_obj.get("person") or {}
        rows.append(
            (
                game_pk,
                team_side,
                idx,
                _safe_int(player_id),
                _safe_str(person.get("fullName")),
                updated_at,
            )
        )
    return rows


def _extract_starting_pitcher(game_pk: int, team_side: str, team_box: dict[str, Any], updated_at: str) -> Optional[tuple]:
    pitchers = team_box.get("pitchers") or []
    players = team_box.get("players") or {}
    if not pitchers:
        return None

    starter_id = _safe_int(pitchers[0])
    if starter_id is None:
        return None

    starter_obj = players.get(f"ID{starter_id}") or {}
    person = starter_obj.get("person") or {}
    pitch_hand = (starter_obj.get("pitchHand") or {}).get("code")
    return (
        game_pk,
        team_side,
        starter_id,
        _safe_str(person.get("fullName")),
        _safe_str(pitch_hand),
        "live_boxscore",
        updated_at,
    )


def refresh_mlb_today(db_path: Path, target_date: Optional[str] = None) -> RefreshResult:
    ensure_tables(db_path)
    today = target_date or date.today().strftime("%Y-%m-%d")
    updated_at = _now_utc_iso()

    try:
        schedule = _http_get_json(SCHEDULE_URL.format(target_date=today))
        dates = schedule.get("dates") or []
        games = (dates[0].get("games") or []) if dates else []

        games_rows: list[tuple] = []
        lineup_rows: list[tuple] = []
        starter_rows: list[tuple] = []

        for game in games:
            game_pk = _safe_int(game.get("gamePk"))
            if game_pk is None:
                continue

            status = game.get("status") or {}
            teams = game.get("teams") or {}
            home = teams.get("home") or {}
            away = teams.get("away") or {}
            home_team = home.get("team") or {}
            away_team = away.get("team") or {}
            venue = game.get("venue") or {}
            home_probable = home.get("probablePitcher") or {}
            away_probable = away.get("probablePitcher") or {}

            lineup_confirmed_home = 0
            lineup_confirmed_away = 0

            try:
                live = _http_get_json(LIVE_FEED_URL.format(game_pk=game_pk))
                box = ((live.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}
                home_box = box.get("home") or {}
                away_box = box.get("away") or {}

                home_lineup = _extract_lineup_rows(game_pk, "home", home_box, updated_at)
                away_lineup = _extract_lineup_rows(game_pk, "away", away_box, updated_at)
                lineup_rows.extend(home_lineup)
                lineup_rows.extend(away_lineup)

                lineup_confirmed_home = 1 if home_lineup else 0
                lineup_confirmed_away = 1 if away_lineup else 0

                home_starter = _extract_starting_pitcher(game_pk, "home", home_box, updated_at)
                away_starter = _extract_starting_pitcher(game_pk, "away", away_box, updated_at)
                if home_starter:
                    starter_rows.append(home_starter)
                if away_starter:
                    starter_rows.append(away_starter)
            except Exception:
                # Keep game row even if live feed is temporarily unavailable.
                pass

            games_rows.append(
                (
                    game_pk,
                    today,
                    _safe_str(game.get("gameDate")),
                    _safe_str(status.get("detailedState")),
                    _safe_str(status.get("statusCode")),
                    _safe_str(status.get("abstractGameState")),
                    _safe_int(home_team.get("id")),
                    _safe_str(home_team.get("name")),
                    _safe_int(away_team.get("id")),
                    _safe_str(away_team.get("name")),
                    _safe_int(venue.get("id")),
                    _safe_str(venue.get("name")),
                    _safe_int(home_probable.get("id")),
                    _safe_str(home_probable.get("fullName")),
                    _safe_int(away_probable.get("id")),
                    _safe_str(away_probable.get("fullName")),
                    lineup_confirmed_home,
                    lineup_confirmed_away,
                    updated_at,
                )
            )

            if _safe_int(home_probable.get("id")) is not None:
                starter_rows.append(
                    (
                        game_pk,
                        "home",
                        _safe_int(home_probable.get("id")),
                        _safe_str(home_probable.get("fullName")),
                        "",
                        "schedule_probable",
                        updated_at,
                    )
                )
            if _safe_int(away_probable.get("id")) is not None:
                starter_rows.append(
                    (
                        game_pk,
                        "away",
                        _safe_int(away_probable.get("id")),
                        _safe_str(away_probable.get("fullName")),
                        "",
                        "schedule_probable",
                        updated_at,
                    )
                )

        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM mlb_games_today WHERE target_date = ?", (today,))
            cur.execute(
                "DELETE FROM mlb_lineups_today WHERE game_pk IN (SELECT game_pk FROM mlb_games_today WHERE target_date = ?)",
                (today,),
            )
            cur.execute(
                "DELETE FROM mlb_starting_pitchers_today WHERE game_pk IN (SELECT game_pk FROM mlb_games_today WHERE target_date = ?)",
                (today,),
            )

            if games_rows:
                cur.executemany(
                    """
                    INSERT INTO mlb_games_today (
                        game_pk, target_date, start_time_utc, detailed_state, status_code, abstract_state,
                        home_team_id, home_team_name, away_team_id, away_team_name,
                        venue_id, venue_name,
                        home_probable_pitcher_id, home_probable_pitcher_name,
                        away_probable_pitcher_id, away_probable_pitcher_name,
                        lineup_confirmed_home, lineup_confirmed_away, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    games_rows,
                )

            if lineup_rows:
                cur.executemany(
                    """
                    INSERT OR REPLACE INTO mlb_lineups_today (
                        game_pk, team_side, batting_order_slot, player_id, player_name, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    lineup_rows,
                )

            if starter_rows:
                cur.executemany(
                    """
                    INSERT OR REPLACE INTO mlb_starting_pitchers_today (
                        game_pk, team_side, pitcher_id, pitcher_name, throwing_hand, source, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    starter_rows,
                )

            cur.execute(
                """
                INSERT OR REPLACE INTO mlb_data_freshness (source_name, last_success_at, records_loaded, notes)
                VALUES (?, ?, ?, ?)
                """,
                (
                    "mlb_today",
                    updated_at,
                    len(games_rows),
                    f"games={len(games_rows)}, lineups={len(lineup_rows)}, starters={len(starter_rows)}",
                ),
            )
            conn.commit()

        return RefreshResult(
            ok=True,
            target_date=today,
            games_loaded=len(games_rows),
            lineup_rows_loaded=len(lineup_rows),
            starter_rows_loaded=len(starter_rows),
            updated_at=updated_at,
        )
    except Exception as exc:
        return RefreshResult(
            ok=False,
            target_date=today,
            games_loaded=0,
            lineup_rows_loaded=0,
            starter_rows_loaded=0,
            updated_at=updated_at,
            error=str(exc),
        )


def load_today_snapshot(db_path: Path, target_date: Optional[str] = None) -> dict[str, Any]:
    ensure_tables(db_path)
    today = target_date or date.today().strftime("%Y-%m-%d")

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(lineup_confirmed_home), 0),
                   COALESCE(SUM(lineup_confirmed_away), 0),
                   MAX(updated_at)
            FROM mlb_games_today
            WHERE target_date = ?
            """,
            (today,),
        )
        row = cur.fetchone() or (0, 0, 0, None)

        cur.execute(
            """
            SELECT game_pk, away_team_name, home_team_name, detailed_state,
                   home_probable_pitcher_name, away_probable_pitcher_name,
                   lineup_confirmed_home, lineup_confirmed_away, start_time_utc
            FROM mlb_games_today
            WHERE target_date = ?
            ORDER BY start_time_utc
            LIMIT 15
            """,
            (today,),
        )
        games = cur.fetchall()

        cur.execute(
            """
            SELECT last_success_at, records_loaded, notes
            FROM mlb_data_freshness
            WHERE source_name = 'mlb_today'
            """
        )
        freshness = cur.fetchone()

    return {
        "target_date": today,
        "games_count": int(row[0] or 0),
        "lineups_confirmed_total": int((row[1] or 0) + (row[2] or 0)),
        "last_game_update": _safe_str(row[3]),
        "games": [
            {
                "game_pk": g[0],
                "away_team": g[1],
                "home_team": g[2],
                "status": g[3],
                "home_probable": g[4],
                "away_probable": g[5],
                "home_lineup_confirmed": int(g[6] or 0),
                "away_lineup_confirmed": int(g[7] or 0),
                "start_time_utc": g[8],
            }
            for g in games
        ],
        "freshness": {
            "last_success_at": _safe_str(freshness[0]) if freshness else "",
            "records_loaded": int(freshness[1] or 0) if freshness else 0,
            "notes": _safe_str(freshness[2]) if freshness else "",
        },
    }
