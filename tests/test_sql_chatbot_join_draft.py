import unittest
import sqlite3
import tempfile
import os
from pathlib import Path

from src.sql_chatbot import SQLBibleChatbot


class SQLChatbotJoinDraftTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot = SQLBibleChatbot(Path("data/SQL_BIBLE_PRIME.sql"))

    def test_extract_intent_quarter_year_and_fein(self):
        intent = self.bot.extract_intent(
            "Show liability for FEIN 12-3456789 in quarter 2 of 2025"
        )
        self.assertEqual(intent.fein, "12-3456789")
        self.assertEqual(intent.quarter, 2)
        self.assertEqual(intent.year, 2025)

    def test_join_draft_generated_for_liability_and_wages(self):
        payload = self.bot.answer(
            "Get liability incurred date for employers who also owe wages for quarter 2 of 2025"
        )
        draft = payload.get("join_draft")
        self.assertIsNotNone(draft)
        self.assertIn("JOIN", draft["sql"].upper())
        self.assertIn("%(quarter)s", draft["sql"])
        self.assertIn("%(year)s", draft["sql"])
        self.assertIn("parameters", draft)
        self.assertIn("confidence", draft)
        self.assertIn("verification", draft)

    def test_execute_join_draft_blocks_missing_required_params(self):
        payload = self.bot.answer(
            "Get liability incurred date for employers who also owe wages"
        )
        draft = payload.get("join_draft")
        self.assertIsNotNone(draft)

        result = self.bot.execute_join_draft(draft)
        self.assertFalse(result["ok"])
        self.assertIn("Missing required JOIN parameter", result["error"])

    def test_execute_join_draft_validates_quarter_range(self):
        payload = self.bot.answer(
            "Get liability incurred date for employers who also owe wages for quarter 2 of 2025"
        )
        draft = payload.get("join_draft")
        self.assertIsNotNone(draft)

        result = self.bot.execute_join_draft(draft, override_params={"quarter": 9, "year": 2025})
        self.assertFalse(result["ok"])
        self.assertIn("Invalid quarter", result["error"])

    def test_execute_join_draft_blocks_failed_schema_verification(self):
        payload = self.bot.answer(
            "Get liability incurred date for employers who also owe wages for quarter 2 of 2025"
        )
        draft = payload.get("join_draft")
        self.assertIsNotNone(draft)

        draft["verification"] = {
            "status": "failed",
            "message": "Join key missing in: fake.left.employer_id",
            "left_has_join_key": False,
            "right_has_join_key": True,
        }

        result = self.bot.execute_join_draft(draft, override_params={"quarter": 2, "year": 2025})
        self.assertFalse(result["ok"])
        self.assertIn("failed schema verification", result["error"])

    def test_uploaded_csv_question_uses_uploaded_table_not_template_library(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "demo.db"
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE batting (
                        player_name TEXT,
                        woba REAL,
                        hard_hit_percent REAL
                    )
                    """
                )
                cur.executemany(
                    "INSERT INTO batting (player_name, woba, hard_hit_percent) VALUES (?, ?, ?)",
                    [
                        ("Player A", 0.510, 55.0),
                        ("Player B", 0.390, 42.0),
                        ("Player C", 0.455, 48.0),
                    ],
                )
                conn.commit()

            bot = SQLBibleChatbot(
                Path("data/SQL_BIBLE_PRIME.sql"),
                database_url=f"sqlite:///{db_path}",
                allowed_tables={"batting"},
            )

            payload = bot.answer_uploaded_table_question("batting", "show me the hottest hitter")

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["mode"], "uploaded_csv_query")
            self.assertIn("batting", payload["proposed_sql"].lower())
            self.assertIn("order by", payload["proposed_sql"].lower())
            self.assertIn("hr_likelihood_score", payload["proposed_sql"].lower())
            self.assertEqual(payload["result"]["row_count"], 3)
            self.assertEqual(payload["result"]["rows"][0][0], "Player A")
            self.assertTrue(payload.get("analysis"))
            self.assertEqual(payload["plan"].get("analysis_profile"), "General Manager")
            self.assertTrue(payload.get("narrative_card"))
            self.assertIn("summary", payload["narrative_card"])
            self.assertTrue(payload["narrative_card"].get("top_candidates"))
            self.assertIn("name", payload["narrative_card"]["top_candidates"][0])
            self.assertIn("confidence", payload["narrative_card"]["top_candidates"][0])
            self.assertIn("reason", payload["narrative_card"]["top_candidates"][0])
        finally:
            try:
                os.remove(db_path)
            except OSError:
                pass
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass

    def test_uploaded_csv_pitcher_matchup_query_builds_edge_score(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "demo.db"
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE pitching (
                        pitcher_name TEXT,
                        k_percent REAL,
                        xwoba REAL,
                        era REAL,
                        bb_percent REAL
                    )
                    """
                )
                cur.executemany(
                    "INSERT INTO pitching (pitcher_name, k_percent, xwoba, era, bb_percent) VALUES (?, ?, ?, ?, ?)",
                    [
                        ("Pitcher A", 31.0, 0.260, 2.90, 6.0),
                        ("Pitcher B", 24.0, 0.315, 3.95, 8.5),
                        ("Pitcher C", 28.0, 0.280, 3.20, 7.0),
                    ],
                )
                conn.commit()

            bot = SQLBibleChatbot(
                Path("data/SQL_BIBLE_PRIME.sql"),
                database_url=f"sqlite:///{db_path}",
                allowed_tables={"pitching"},
            )

            payload = bot.answer_uploaded_table_question("pitching", "Which pitchers have favorable matchups today?")

            self.assertTrue(payload["ok"])
            self.assertIn("pitching_edge_score", payload["proposed_sql"].lower())
            self.assertEqual(payload["result"]["rows"][0][0], "Pitcher A")
            self.assertTrue(payload.get("analysis"))
            self.assertTrue(payload.get("narrative_card"))
        finally:
            try:
                os.remove(db_path)
            except OSError:
                pass

    def test_uploaded_csv_respects_pitching_profile_without_pitcher_keywords(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "demo.db"
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE pitching (
                        pitcher_name TEXT,
                        k_percent REAL,
                        xwoba REAL,
                        era REAL,
                        bb_percent REAL
                    )
                    """
                )
                cur.executemany(
                    "INSERT INTO pitching (pitcher_name, k_percent, xwoba, era, bb_percent) VALUES (?, ?, ?, ?, ?)",
                    [
                        ("Pitcher A", 31.0, 0.260, 2.90, 6.0),
                        ("Pitcher B", 24.0, 0.315, 3.95, 8.5),
                    ],
                )
                conn.commit()

            bot = SQLBibleChatbot(
                Path("data/SQL_BIBLE_PRIME.sql"),
                database_url=f"sqlite:///{db_path}",
                allowed_tables={"pitching"},
            )

            payload = bot.answer_uploaded_table_question(
                "pitching",
                "who stands out today",
                analysis_profile="Pitching Analyst",
            )

            self.assertTrue(payload["ok"])
            self.assertIn("pitching_edge_score", payload["proposed_sql"].lower())
            self.assertEqual(payload["plan"].get("analysis_profile"), "Pitching Analyst")
        finally:
            try:
                os.remove(db_path)
            except OSError:
                pass
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass

    def test_uploaded_csv_name_like_column_is_projected_as_player_name(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "demo.db"
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE batting (
                        last_name__first_name TEXT,
                        player_id INTEGER,
                        woba REAL,
                        xwoba REAL,
                        hard_hit_percent REAL,
                        barrel_batted_rate REAL,
                        avg_best_speed REAL,
                        pa INTEGER
                    )
                    """
                )
                cur.executemany(
                    "INSERT INTO batting (last_name__first_name, player_id, woba, xwoba, hard_hit_percent, barrel_batted_rate, avg_best_speed, pa) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("Caissie, Owen", 683357, 0.323, 0.329, 50.0, 21.4, 104.3, 57),
                        ("Kurtz, Nick", 701762, 0.316, 0.347, 58.6, 13.8, 106.6, 76),
                    ],
                )
                conn.commit()

            bot = SQLBibleChatbot(
                Path("data/SQL_BIBLE_PRIME.sql"),
                database_url=f"sqlite:///{db_path}",
                allowed_tables={"batting"},
            )

            payload = bot.answer_uploaded_table_question("batting", "who is likely to hit a homerun today")

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["result"]["columns"][0], "player_name")
            self.assertEqual(payload["result"]["columns"][1], "player_id")
            self.assertIn("Top result:", " ".join(payload.get("analysis") or []))
            self.assertTrue(payload["narrative_card"].get("top_candidates"))
            self.assertEqual(payload["narrative_card"]["top_candidates"][0]["name"], payload["result"]["rows"][0][0])
        finally:
            try:
                os.remove(db_path)
            except OSError:
                pass
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass

    def test_uploaded_csv_top_whiff_rate_uses_lowest_whiff_percent_and_metric_title(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "demo.db"
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE batting_whiff (
                        player_name TEXT,
                        player_id INTEGER,
                        whiff_percent REAL,
                        woba REAL,
                        xwoba REAL,
                        pa INTEGER
                    )
                    """
                )
                cur.executemany(
                    "INSERT INTO batting_whiff (player_name, player_id, whiff_percent, woba, xwoba, pa) VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        ("Player High Whiff", 1, 41.2, 0.301, 0.315, 88),
                        ("Player Low Whiff", 2, 12.5, 0.420, 0.445, 92),
                        ("Player Mid Whiff", 3, 26.8, 0.355, 0.360, 81),
                    ],
                )
                conn.commit()

            bot = SQLBibleChatbot(
                Path("data/SQL_BIBLE_PRIME.sql"),
                database_url=f"sqlite:///{db_path}",
                allowed_tables={"batting_whiff"},
            )

            payload = bot.answer_uploaded_table_question("batting_whiff", "Top whiff rate")

            self.assertTrue(payload["ok"])
            self.assertIn("order by", payload["proposed_sql"].lower())
            self.assertIn("asc", payload["proposed_sql"].lower())
            self.assertEqual(payload["result"]["rows"][0][0], "Player Low Whiff")
            self.assertIn("whiff", payload["narrative_card"]["title"].lower())
            self.assertNotIn("general manager brief", payload["narrative_card"]["title"].lower())
            self.assertTrue(any("whiff" in bullet.lower() for bullet in payload.get("analysis") or []))
        finally:
            try:
                os.remove(db_path)
            except OSError:
                pass
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass

    def test_uploaded_csv_pitcher_question_rejects_hitter_only_table(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "demo.db"
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE batting_only (
                        player_name TEXT,
                        player_id INTEGER,
                        batting_avg REAL,
                        ops REAL,
                        pa INTEGER
                    )
                    """
                )
                cur.executemany(
                    "INSERT INTO batting_only (player_name, player_id, batting_avg, ops, pa) VALUES (?, ?, ?, ?, ?)",
                    [
                        ("Jazz Chisholm Jr.", 123, 0.268, 0.782, 89),
                        ("Player B", 456, 0.245, 0.744, 95),
                    ],
                )
                conn.commit()

            bot = SQLBibleChatbot(
                Path("data/SQL_BIBLE_PRIME.sql"),
                database_url=f"sqlite:///{db_path}",
                allowed_tables={"batting_only"},
            )

            payload = bot.answer_uploaded_table_question("batting_only", "top pitchers")

            self.assertFalse(payload["ok"])
            self.assertIn("does not appear to contain pitcher stats", payload["error"].lower())
        finally:
            try:
                os.remove(db_path)
            except OSError:
                pass
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass

    def test_uploaded_csv_top_pitchers_uses_pitcher_rate_table(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "demo.db"
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE pitching_rates (
                        player_name TEXT,
                        player_id INTEGER,
                        k_percent REAL,
                        bb_percent REAL,
                        whiff_percent REAL,
                        woba REAL,
                        xwoba REAL,
                        barrel_batted_rate REAL,
                        hard_hit_percent REAL,
                        pa INTEGER
                    )
                    """
                )
                cur.executemany(
                    "INSERT INTO pitching_rates (player_name, player_id, k_percent, bb_percent, whiff_percent, woba, xwoba, barrel_batted_rate, hard_hit_percent, pa) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("Pitcher High K", 1, 31.5, 4.1, 32.0, 0.255, 0.260, 5.1, 30.0, 88),
                        ("Pitcher Control", 2, 24.0, 2.9, 27.5, 0.240, 0.248, 4.3, 26.0, 92),
                        ("Pitcher Average", 3, 27.0, 5.0, 28.5, 0.275, 0.281, 6.2, 32.0, 85),
                    ],
                )
                conn.commit()

            bot = SQLBibleChatbot(
                Path("data/SQL_BIBLE_PRIME.sql"),
                database_url=f"sqlite:///{db_path}",
                allowed_tables={"pitching_rates"},
            )

            payload = bot.answer_uploaded_table_question("pitching_rates", "top pitchers")

            self.assertTrue(payload["ok"])
            self.assertIn("pitching_edge_score", payload["proposed_sql"].lower())
            self.assertNotEqual(payload["result"]["rows"][0][0], "Jazz Chisholm Jr.")
            self.assertIn(payload["result"]["rows"][0][0], {"Pitcher High K", "Pitcher Control", "Pitcher Average"})
            self.assertIn("pitcher", payload["narrative_card"]["title"].lower())
        finally:
            try:
                os.remove(db_path)
            except OSError:
                pass
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass

    def test_uploaded_csv_includes_matchup_context_in_reason(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "demo.db"
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE batting_context (
                        player_name TEXT,
                        player_id INTEGER,
                        pa INTEGER,
                        barrel_batted_rate REAL,
                        hard_hit_percent REAL,
                        avg_best_speed REAL,
                        xwoba REAL,
                        woba REAL,
                        opp_pitcher_hand TEXT,
                        xwoba_vs_rhp REAL,
                        xwoba_vs_lhp REAL,
                        recent_xwoba REAL
                    )
                    """
                )
                cur.executemany(
                    "INSERT INTO batting_context (player_name, player_id, pa, barrel_batted_rate, hard_hit_percent, avg_best_speed, xwoba, woba, opp_pitcher_hand, xwoba_vs_rhp, xwoba_vs_lhp, recent_xwoba) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("Player Context A", 1, 120, 18.0, 52.0, 104.0, 0.365, 0.350, "R", 0.382, 0.331, 0.374),
                        ("Player Context B", 2, 95, 14.0, 45.0, 101.0, 0.332, 0.321, "R", 0.341, 0.305, 0.320),
                    ],
                )
                conn.commit()

            bot = SQLBibleChatbot(
                Path("data/SQL_BIBLE_PRIME.sql"),
                database_url=f"sqlite:///{db_path}",
                allowed_tables={"batting_context"},
            )

            payload = bot.answer_uploaded_table_question("batting_context", "who is likely to hit a homerun today")

            self.assertTrue(payload["ok"])
            self.assertTrue(payload.get("narrative_card"))
            top_candidate = payload["narrative_card"]["top_candidates"][0]
            self.assertIn("Matchup context", top_candidate["reason"])
        finally:
            try:
                os.remove(db_path)
            except OSError:
                pass
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass

    def test_uploaded_csv_tandem_query_combines_two_tables_and_keeps_source_table(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "demo.db"
        try:
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE batting_alpha (
                        player_name TEXT,
                        player_id INTEGER,
                        pa INTEGER,
                        woba REAL,
                        xwoba REAL,
                        hard_hit_percent REAL,
                        barrel_batted_rate REAL,
                        avg_best_speed REAL
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE batting_beta (
                        player_name TEXT,
                        player_id INTEGER,
                        pa INTEGER,
                        woba REAL,
                        xwoba REAL,
                        hard_hit_percent REAL,
                        barrel_batted_rate REAL,
                        avg_best_speed REAL
                    )
                    """
                )
                cur.executemany(
                    "INSERT INTO batting_alpha (player_name, player_id, pa, woba, xwoba, hard_hit_percent, barrel_batted_rate, avg_best_speed) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("Alpha One", 1, 90, 0.470, 0.485, 56.0, 20.0, 105.0),
                        ("Alpha Two", 2, 88, 0.350, 0.360, 41.0, 12.0, 99.0),
                    ],
                )
                cur.executemany(
                    "INSERT INTO batting_beta (player_name, player_id, pa, woba, xwoba, hard_hit_percent, barrel_batted_rate, avg_best_speed) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ("Beta One", 3, 93, 0.455, 0.462, 54.0, 18.0, 104.0),
                        ("Beta Two", 4, 77, 0.332, 0.341, 39.0, 10.0, 97.0),
                    ],
                )
                conn.commit()

            bot = SQLBibleChatbot(
                Path("data/SQL_BIBLE_PRIME.sql"),
                database_url=f"sqlite:///{db_path}",
                allowed_tables={"batting_alpha", "batting_beta"},
            )

            payload = bot.answer_uploaded_tables_question(
                ["batting_alpha", "batting_beta"],
                "who is likely to hit a homerun today",
            )

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["mode"], "uploaded_csv_tandem_query")
            self.assertIn("union all", payload["proposed_sql"].lower())
            self.assertEqual(payload["result"]["columns"][0], "source_table")
            self.assertEqual(payload["result"]["row_count"], 4)
            self.assertIn(payload["result"]["rows"][0][0], {"batting_alpha", "batting_beta"})
            self.assertEqual(payload["plan"]["table_names"], ["batting_alpha", "batting_beta"])
        finally:
            try:
                os.remove(db_path)
            except OSError:
                pass
            try:
                os.rmdir(temp_dir)
            except OSError:
                pass


if __name__ == "__main__":
    unittest.main()
