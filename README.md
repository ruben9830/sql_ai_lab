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
- Set `ALLOWED_TABLES` (optional, comma-separated allowlist for execution safety)
	- Example: `ALLOWED_TABLES=public.employers,public.liabilities,wage_reports`

5. Run:

```powershell
python src/sql_chatbot.py
```

## Optional Web UI (Streamlit)

Run the chat app in a browser:

```powershell
streamlit run src/sql_chatbot_web.py
```

If Streamlit is not installed yet, run:

```powershell
pip install -r requirements.txt
```

Web UI features:

- Chat interface with conversation history
- Dual data modes: `Demo (SQLite)` and `Enterprise (Postgres)`
- Quick filters for `start date`, `end date`, `FEIN`, and `employer id`
- Quarter/year filters for wage-period questions
- One-click business prompts for fast demos
- SQL approval step (`Approve and Run SQL`) before execution
- Download query results as CSV when a query is executed
- Auto-generated JOIN drafts for multi-table questions (liability + wages)
- JOIN draft confidence and parameter map for safer execution review
- One-click `Run JOIN Draft` with required parameter validation
- Schema-aware join-key verification before JOIN draft execution
- Public CSV import from web/GitHub raw URLs into Demo SQLite mode
- In-app table preview for imported public datasets

## Easiest Way to Run (Non-Technical)

On any Windows machine (work laptop or home PC):

1. Open the project folder.
2. Double-click `run_streamlit.bat`.
3. Wait for setup (first run only), then your browser opens automatically.

Tip:

- If you accidentally run `run_stramlit.bat` (common typo), it now redirects to the correct launcher.

What this launcher does for you automatically:

- Pulls latest changes from GitHub when possible
- Creates `.venv` if missing
- Installs/updates required packages
- Creates `.env` from `.env.example` (if needed)
- Starts the Streamlit app

Notes:

- Demo mode works immediately with no VPN and no enterprise database.
- You can use the same project folder on both machines.
- If GitHub login is needed, the launcher will still run and skip sync.

Demo mode notes:

- Demo mode auto-creates `data/demo_hackathon.db` with synthetic data
- No VPN or enterprise DB setup required for a live demo
- Connection health status is shown in the UI (mode + table discovery)

## Smart JOIN Drafting

When the user asks a multi-table question (for example, liabilities + wages),
the chatbot can now draft a parameterized JOIN query template.

Safety-oriented behavior:

- Uses named placeholders like `%(quarter)s` and `%(year)s`
- Returns a `parameters` object to fill and review
- Returns a `confidence` level (`low`, `medium`, `high`)
- Still enforces read-only SQL policy for execution
- Attempts join-key verification against database metadata (`information_schema.columns`)

Execution controls:

- `Run JOIN Draft` requires valid `quarter` and `year`
- Quarter validation: integer in `[1, 4]`
- Year validation: integer in `[1900, 2100]`
- Parameters are sent separately to SQL execution (no unsafe interpolation)
- If join-key verification fails, JOIN execution is blocked

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

## Run Tests

```powershell
c:\Users\rvalenciajr\sql_ai_lab\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_*.py"
```

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
