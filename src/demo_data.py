from __future__ import annotations

import sqlite3
from pathlib import Path


def ensure_demo_database(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS employers (
                employer_id TEXT PRIMARY KEY,
                fein TEXT NOT NULL,
                employer_name TEXT NOT NULL,
                state TEXT NOT NULL,
                tpa_name TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS liabilities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employer_id TEXT NOT NULL,
                fein TEXT NOT NULL,
                liability_incurred_date TEXT NOT NULL,
                amount_due REAL NOT NULL,
                FOREIGN KEY (employer_id) REFERENCES employers(employer_id)
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS wage_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employer_id TEXT NOT NULL,
                quarter INTEGER NOT NULL,
                year INTEGER NOT NULL,
                amount_due REAL NOT NULL,
                FOREIGN KEY (employer_id) REFERENCES employers(employer_id)
            )
            """
        )

        cur.execute("SELECT COUNT(*) FROM employers")
        employer_count = int((cur.fetchone() or [0])[0])
        if employer_count == 0:
            cur.executemany(
                """
                INSERT INTO employers (employer_id, fein, employer_name, state, tpa_name)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    ("EMP1001", "12-3456789", "Acme Manufacturing", "KY", "BlueRiver TPA"),
                    ("EMP1002", "98-7654321", "Riverfront Logistics", "KY", "Summit Admin"),
                    ("EMP1003", "11-2233445", "Pioneer Health Group", "TN", "BlueRiver TPA"),
                    ("EMP1004", "44-5566778", "Peak Retail Co", "OH", None),
                ],
            )

        cur.execute("SELECT COUNT(*) FROM liabilities")
        liability_count = int((cur.fetchone() or [0])[0])
        if liability_count == 0:
            cur.executemany(
                """
                INSERT INTO liabilities (employer_id, fein, liability_incurred_date, amount_due)
                VALUES (?, ?, ?, ?)
                """,
                [
                    ("EMP1001", "12-3456789", "2025-01-10", 2200.50),
                    ("EMP1001", "12-3456789", "2025-02-14", 3100.10),
                    ("EMP1002", "98-7654321", "2025-01-09", 900.00),
                    ("EMP1002", "98-7654321", "2025-02-20", 1850.25),
                    ("EMP1003", "11-2233445", "2025-03-02", 4050.00),
                    ("EMP1004", "44-5566778", "2025-03-15", 1200.75),
                ],
            )

        cur.execute("SELECT COUNT(*) FROM wage_reports")
        wage_count = int((cur.fetchone() or [0])[0])
        if wage_count == 0:
            cur.executemany(
                """
                INSERT INTO wage_reports (employer_id, quarter, year, amount_due)
                VALUES (?, ?, ?, ?)
                """,
                [
                    ("EMP1001", 1, 2025, 1980.00),
                    ("EMP1001", 2, 2025, 2405.00),
                    ("EMP1002", 1, 2025, 1100.00),
                    ("EMP1002", 2, 2025, 1400.00),
                    ("EMP1003", 1, 2025, 3899.00),
                    ("EMP1004", 1, 2025, 1000.00),
                ],
            )

        conn.commit()
