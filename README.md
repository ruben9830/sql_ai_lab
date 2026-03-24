# SQL AI Lab - Chatbot Prototype

This project now includes a Python CLI chatbot that can:

- parse your query library from `data/SQL_BIBLE_PRIME.sql`
- match natural-language questions to likely SQL snippets
- optionally use an LLM to suggest a best query or draft read-only SQL
- optionally execute read-only SQL (`SELECT`/`WITH`) against Postgres

## Files

- `src/sql_chatbot.py`: chatbot application
- `.env.example`: environment variable template
- `requirements.txt`: Python dependencies

## Quick Start

1. Create and activate a Python environment.
2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Copy env file:

```powershell
copy .env.example .env
```

4. Edit `.env`:

- Set `OPENAI_API_KEY` (optional, for AI planning)
- Set `DATABASE_URL` (optional, required to execute SQL)

5. Run:

```powershell
python src/sql_chatbot.py
```

## Chat Commands

- `/list`: list loaded query snippets
- `/show <id>`: show full SQL for a snippet
- `/quit`: exit

## Example Questions

- "Find inactive employers with liability incurred date 2024-01-01 and no wages"
- "Which employers tied to a TPA have both unpaid amounts and missing wage reports"
- "Show queries related to delinquent employer staging"

## Notes

- Execution mode is read-only by design.
- The app blocks obvious write operations (`INSERT`, `UPDATE`, `DELETE`, etc.).
- If no OpenAI key is provided, it still works using keyword matching and query suggestions.

## Prevent Losing Work

Run this command anytime you want a timestamped backup snapshot:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/save_snapshot.ps1
```

Each run creates a folder under `backups/` containing:

- `data/SQL_BIBLE_PRIME.sql`
- `src/sql_chatbot.py`
- key project files and a `manifest.json`

## Git Checkpoints (Version History)

Create a versioned restore point anytime with:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/git_checkpoint.ps1 -Message "what changed"
```

By default, this command now commits and pushes to GitHub (`origin`) in one step.
Use this if you want local commit only:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/git_checkpoint.ps1 -Message "local checkpoint" -NoPush
```

Examples:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/git_checkpoint.ps1 -Message "added new delinquent employer query"
powershell -ExecutionPolicy Bypass -File scripts/git_checkpoint.ps1 -Message "improved chatbot ranking"
```

This command initializes git (first run), stages all changes, creates a timestamped commit, and pushes the current branch.
