@echo off
setlocal
title SQL AI Lab Launcher

REM Always run from the project folder (works on any machine/path).
cd /d "%~dp0"

set "VENV_PY=.venv\Scripts\python.exe"
set "LOG_FILE=.launcher_last_run.log"

echo [%date% %time%] Launcher started > "%LOG_FILE%"

echo ==========================================
echo SQL AI Lab - One-Click Launcher
echo ==========================================

REM Optional sync: if this is a git repo and git is installed, pull latest changes.
where git >nul 2>&1
if not errorlevel 1 (
	if exist ".git" (
		echo [Sync] Checking for updates from GitHub...
		git pull --ff-only >nul 2>&1
		if errorlevel 1 (
			echo [Sync] Skipped ^(offline or login needed^).
		) else (
			echo [Sync] Project updated.
		)
	)
)

REM First-run setup: create virtual environment if needed.
if not exist "%VENV_PY%" (
	echo [Setup] Creating Python virtual environment...
	py -3 -m venv .venv >nul 2>&1
	if errorlevel 1 (
		python -m venv .venv >nul 2>&1
	)
)

if not exist "%VENV_PY%" (
	echo [Error] Could not create .venv. Install Python 3.10+ and try again.
	pause
	exit /b 1
)

REM Ensure dependencies are installed (safe to re-run).
echo [Setup] Ensuring dependencies are installed...
"%VENV_PY%" -m pip install --upgrade pip >nul
"%VENV_PY%" -m pip install -r requirements.txt
if errorlevel 1 (
	echo [Error] Dependency install failed. Check internet connection and try again.
	echo [Error] Dependency install failed >> "%LOG_FILE%"
	pause
	exit /b 1
)

REM Create .env from template if missing.
if not exist ".env" (
	if exist ".env.example" (
		copy /Y .env.example .env >nul
		echo [Setup] Created .env from .env.example
	)
)

echo [Launch] Starting SQL AI Lab in your browser...
start "" "http://localhost:8501"
"%VENV_PY%" -m streamlit run src/sql_chatbot_web.py
if errorlevel 1 (
	echo [Warn] Streamlit failed on port 8501. Retrying on port 8502...
	echo [Warn] Retrying Streamlit on 8502 after non-zero exit >> "%LOG_FILE%"
	start "" "http://localhost:8502"
	"%VENV_PY%" -m streamlit run src/sql_chatbot_web.py --server.port 8502
	if errorlevel 1 (
		echo [Error] Streamlit exited with an error on fallback port 8502. See details above.
		echo [Error] Streamlit process failed on both 8501 and 8502 >> "%LOG_FILE%"
	)
)

echo.
echo SQL AI Lab stopped.
echo If nothing opened, run this command from PowerShell for full logs:
echo   .\.venv\Scripts\python.exe -m streamlit run src\sql_chatbot_web.py
pause
