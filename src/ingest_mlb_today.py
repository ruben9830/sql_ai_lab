from __future__ import annotations

import argparse
from pathlib import Path

from mlb_today_data import refresh_mlb_today


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh today's MLB slate into local SQLite tables.")
    parser.add_argument("--db", default="data/demo_hackathon.db", help="Path to SQLite database file.")
    parser.add_argument("--date", default="", help="Target date in YYYY-MM-DD format. Defaults to today.")
    args = parser.parse_args()

    result = refresh_mlb_today(Path(args.db), target_date=(args.date.strip() or None))
    payload = result.to_dict()

    if result.ok:
        print(
            "MLB refresh OK:",
            f"date={payload['target_date']}",
            f"games={payload['games_loaded']}",
            f"lineups={payload['lineup_rows_loaded']}",
            f"starters={payload['starter_rows_loaded']}",
            f"updated_at={payload['updated_at']}",
        )
        return 0

    print("MLB refresh FAILED:", payload.get("error", "unknown error"))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
