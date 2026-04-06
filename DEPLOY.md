# Deploy to Streamlit Cloud

This app runs on the web at no cost using Streamlit Cloud.

## Step 1: Push to GitHub

Make sure all changes are committed and pushed:

```powershell
git add .
git commit -m "add streamlit cloud config"
git push origin master
```

## Step 2: Create Streamlit Cloud Account

1. Go to https://share.streamlit.io
2. Sign in with GitHub
3. Authorize Streamlit to access your repositories

## Step 3: Deploy the App

1. Click "New app"
2. Select repository: `ruben9830/sql_ai_lab`
3. Select branch: `master`
4. Set main file path: `src/sql_chatbot_web.py`
5. Click "Deploy"

The app will be live at a URL like: `https://sql-ai-lab-xxxxx.streamlit.app`

## Step 4: Add Secrets (Optional)

If you want to use OpenAI or connect to a database on the web:

1. In Streamlit Cloud dashboard, go to your deployed app
2. Click the three dots menu → Settings
3. Go to "Secrets"
4. Paste your secrets in this format:

```
OPENAI_API_KEY = "your-key-here"
DATABASE_URL = "your-db-url-here"
ALLOWED_TABLES = "public.employers,public.liabilities"
```

The app will safely read these at runtime.

## Local Testing Before Deploy

Test locally first to ensure everything works:

```powershell
$env:STREAMLIT_SUPPRESS_EMAIL_PROMPT="true"
c:\Users\rvalenciajr\sql_ai_lab\.venv\Scripts\python.exe -m streamlit run src\sql_chatbot_web.py
```

Then visit: http://localhost:8501

## Notes

- Your app is public by default (free tier)
- It goes to sleep after 7 days of no traffic (but wakes instantly when visited)
- No credit card required
- Free tier has 1 app limit (upgrade for more)

## Troubleshooting

If deployment fails:
1. Check GitHub is up to date: `git status`
2. Verify `.env` and `.streamlit/secrets.toml` are in `.gitignore`
3. Check Streamlit Cloud logs in the dashboard
