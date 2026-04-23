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
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )

    & git @Args
    if ($LASTEXITCODE -ne 0) {
        throw "git command failed: git $($Args -join ' ')"
    }
}

Require-Command -Name "git"

$branch = (& git rev-parse --abbrev-ref HEAD).Trim()
if (-not $branch -or $branch -eq "HEAD") {
    $branch = "master"
}

Invoke-Git fetch origin --prune
Invoke-Git pull --rebase origin $branch

Write-Host "Up to date on origin/$branch"
