param(
    [Parameter(Mandatory = $true)]
    [string]$Message,
    [switch]$NoPull
)

$ErrorActionPreference = "Stop"

$workspace = (Resolve-Path "$PSScriptRoot\..").Path
Set-Location $workspace

function Require-Command {
    param([string]$Name)

    $cmd = Get-Command $Name -ErrorAction SilentlyContinue
    if (-not $cmd) {
        throw "Required command '$Name' is not installed or not on PATH."
    }
}

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$GitArgs
    )

    & git @GitArgs
    if ($LASTEXITCODE -ne 0) {
        throw "git command failed: git $($GitArgs -join ' ')"
    }
}

Require-Command -Name "git"

$branch = (& git rev-parse --abbrev-ref HEAD).Trim()
if (-not $branch -or $branch -eq "HEAD") {
    $branch = "master"
}

if (-not $NoPull) {
    Invoke-Git -GitArgs @("fetch", "origin", "--prune")
    Invoke-Git -GitArgs @("pull", "--rebase", "origin", $branch)
}

Invoke-Git -GitArgs @("add", "-A")
& git diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host "No staged changes to commit."
    exit 0
}

$stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$finalMessage = "$Message [$stamp]"

Invoke-Git -GitArgs @("commit", "-m", $finalMessage)
Invoke-Git -GitArgs @("push", "-u", "origin", $branch)

Write-Host "Pushed to origin/$branch"
