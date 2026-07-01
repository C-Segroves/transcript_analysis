#!/usr/bin/env bash
# update.sh - pull the latest code on a worker / GPU box.
# Run from anywhere; it operates on the repo this script lives in.
set -euo pipefail

# cd into the repo root (the dir this script is in), regardless of caller's cwd.
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

branch="$(git rev-parse --abbrev-ref HEAD)"
echo "Repo:   $(pwd)"
echo "Branch: ${branch}"
echo "Before: $(git rev-parse --short HEAD) - $(git log -1 --pretty=%s)"
echo

# Fetch + fast-forward only. Fails loudly if local commits/edits would conflict,
# rather than creating a surprise merge.
git fetch --prune origin
if ! git merge --ff-only "origin/${branch}"; then
  echo
  echo "!! Fast-forward failed - you have local changes or diverging commits."
  echo "   Inspect with:  git status   /   git stash   (then re-run)."
  exit 1
fi

# Normalize line endings on shell scripts in case any got committed with CRLF,
# and make launchers executable (harmless if already set).
sed -i 's/\r$//' ./*.sh 2>/dev/null || true
chmod +x ./*.sh 2>/dev/null || true

echo
echo "After:  $(git rev-parse --short HEAD) - $(git log -1 --pretty=%s)"
echo "Done."
