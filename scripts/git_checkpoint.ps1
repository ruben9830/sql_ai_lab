param(
    [string]$Message = "checkpoint"
)

$ErrorActionPreference = "Stop"

$workspace = (Resolve-Path "$PSScriptRoot\..").Path
Set-Location $workspace

function Get-GitExe {
    $candidates = @(
        "git",
        "C:\Program Files\Git\cmd\git.exe",
        "$env:LOCALAPPDATA\Programs\Git\cmd\git.exe",
        "$env:LOCALAPPDATA\Programs\Git\bin\git.exe"
    )

    foreach ($candidate in $candidates) {
        try {
            if ($candidate -eq "git") {
                & git --version *> $null
                if ($LASTEXITCODE -eq 0) { return "git" }
            } elseif (Test-Path $candidate) {
                & $candidate --version *> $null
                if ($LASTEXITCODE -eq 0) { return $candidate }
            }
        } catch {
            # Try next candidate.
        }
    }

    throw "Git executable not found. Install Git first."
}

$git = Get-GitExe

if (-not (Test-Path (Join-Path $workspace ".git"))) {
    & $git init
}

& $git config user.name *> $null
if ($LASTEXITCODE -ne 0) {
    & $git config user.name "SQL AI Lab User"
}

& $git config user.email *> $null
if ($LASTEXITCODE -ne 0) {
    & $git config user.email "sql-ai-lab@local"
}

& $git add -A

& $git diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "No changes to commit."
    exit 0
}

$stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$finalMessage = "$Message [$stamp]"

& $git commit -m $finalMessage

Write-Host "Checkpoint created: $finalMessage"
