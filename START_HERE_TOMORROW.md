# Start Here Tomorrow (Beginner Steps)

Use this exact checklist when you come back.

## 1) Open the project

- Open VS Code.
- Click File -> Open Folder.
- Choose this folder:
  C:\Users\rvalenciajr\sql_ai_lab

## 2) Update from GitHub

- In VS Code, click Terminal -> New Terminal.
- Run:

```powershell
git pull origin master
```

## 3) Read the handoff note

- Open this file:
  NEXT_SESSION.md

## 4) Ask Copilot to continue

- Open Copilot Chat.
- Paste this exact message:

Continue from NEXT_SESSION.md. I am a beginner, explain everything step by step.

## 5) Start the chatbot when ready

- Run:

```powershell
c:/Users/rvalenciajr/sql_ai_lab/.venv/Scripts/python.exe src/sql_chatbot.py
```

## If something fails

- Copy the error text.
- Paste it into Copilot Chat.
- Add: "Please fix this step-by-step."

You are not going to lose your work as long as you keep using the checkpoint command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/git_checkpoint.ps1 -Message "what changed"
```
