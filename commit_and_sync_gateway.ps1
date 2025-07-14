param (
    [string]$RemoteName = 'origin'
)

# Stop on any error
$ErrorActionPreference = 'Stop'
# Check your credential helper configuration
git config --get-regexp credential.helper

# To remove the problematic credential helper
git config --unset-all credential.helper

# Or set a new credential helper
git config --global credential.helper manager-core
# 1) Figure out current branch
$branch = (git rev-parse --abbrev-ref HEAD).Trim()
Write-Host "Current branch: $branch"

# 2) Stage everything
git add -A

# 3) Commit if there are staged changes
if (-not (git diff-index --quiet HEAD --)) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $msg = "Auto-commit on $branch at $timestamp"
    git commit -m $msg
    Write-Host "Committed: $msg"
} else {
    Write-Host "No changes to commit on '$branch'."
}

# 4) Pull & rebase
Write-Host "Pulling and rebasing from $RemoteName/$branch..."
git pull --rebase $RemoteName $branch

# 5) Push
Write-Host "Pushing to $RemoteName/$branch..."
git push $RemoteName $branch

Write-Host "Branch '$branch' is now in sync with $RemoteName."
