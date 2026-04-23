param(
    [string]$Repo = "ruben9830/sql_ai_lab"
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

function Invoke-Cli {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Tool,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )

    if ($Tool -eq "git") {
        & git @Args
    } elseif ($Tool -eq "gh") {
        & gh @Args
    } else {
        throw "Unsupported tool '$Tool'."
    }

    if ($LASTEXITCODE -ne 0) {
        throw "$Tool command failed: $Tool $($Args -join ' ')"
    }
}

Require-Command -Name "git"
Require-Command -Name "gh"

Write-Host "Setting up GitHub sync for $Repo ..."

# GH_TOKEN sessions should not run interactive login unless token is invalid.
if ($env:GH_TOKEN) {
    & gh auth status -h github.com *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Detected invalid GH_TOKEN in environment; falling back to stored credentials."
        Remove-Item Env:GH_TOKEN -ErrorAction SilentlyContinue
    }
}

& gh auth status -h github.com *> $null
if ($LASTEXITCODE -ne 0) {
    # Opens browser/device flow if not already signed in.
    Invoke-Cli gh auth login -h github.com -p https -w
}

# Ensures git operations use GH CLI credentials.
Invoke-Cli gh auth setup-git -h github.com

$originUrl = "https://github.com/$Repo.git"

& git remote get-url origin *> $null
if ($LASTEXITCODE -ne 0) {
    Invoke-Cli git remote add origin $originUrl
} else {
    Invoke-Cli git remote set-url origin $originUrl
}

$branch = (& git rev-parse --abbrev-ref HEAD).Trim()
if (-not $branch -or $branch -eq "HEAD") {
    $branch = "master"
}

Invoke-Cli git fetch origin --prune
& git branch --set-upstream-to "origin/$branch" $branch *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "No upstream branch yet for '$branch'. It will be set on first push."
}

Write-Host "Sync setup complete."
Write-Host "Use scripts/sync_pull.ps1 before work and scripts/sync_push.ps1 after work."
