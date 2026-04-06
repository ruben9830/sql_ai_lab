import unittest
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


if __name__ == "__main__":
    unittest.main()
