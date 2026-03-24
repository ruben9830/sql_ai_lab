param(
    [string]$WorkspaceRoot = "$PSScriptRoot\.."
)

$ErrorActionPreference = "Stop"

$workspace = (Resolve-Path $WorkspaceRoot).Path
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$target = Join-Path $workspace ("backups\snapshot_" + $timestamp)

New-Item -ItemType Directory -Path $target -Force | Out-Null

$itemsToCopy = @(
    "data\SQL_BIBLE_PRIME.sql",
    "src\sql_chatbot.py",
    "README.md",
    "requirements.txt",
    ".env.example"
)

foreach ($item in $itemsToCopy) {
    $source = Join-Path $workspace $item
    if (Test-Path $source) {
        $destDir = Join-Path $target (Split-Path $item -Parent)
        if (-not (Test-Path $destDir)) {
            New-Item -ItemType Directory -Path $destDir -Force | Out-Null
        }
        Copy-Item -Path $source -Destination (Join-Path $target $item) -Force
    }
}

$manifest = @{
    created_at = (Get-Date).ToString("s")
    workspace = $workspace
    snapshot_dir = $target
    included = $itemsToCopy
}

$manifest | ConvertTo-Json -Depth 5 | Set-Content -Path (Join-Path $target "manifest.json") -Encoding UTF8

Write-Host "Snapshot saved to: $target"
