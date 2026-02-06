#!/bin/bash

set -euo pipefail

APP_DIR="/opt/myapp"
KEEP=4   # current + last 3

# Names in APP_DIR that must NEVER be deleted
PROTECTED=(
  "current"
  "logs"
  "config"
  "data"
  ".env"
)

APPLY=false
if [[ "${1:-}" == "--apply" ]]; then
  APPLY=true
fi

cd "$APP_DIR"

CURRENT_DIR="$(readlink -f current)"

is_protected() {
  local name="$1"
  for p in "${PROTECTED[@]}"; do
    [[ "$name" == "$p" ]] && return 0
  done
  return 1
}

# Collect only unprotected directories
mapfile -t VERSIONS < <(
  find . -maxdepth 1 -type d ! -name "." \
    -printf '%T@ %f\n' \
  | sort -nr \
  | awk '{print $2}' \
  | while read -r dir; do
      if ! is_protected "$dir"; then
        echo "$dir"
      fi
    done
)

TO_KEEP=()
for dir in "${VERSIONS[@]}"; do
  FULL_PATH="$(readlink -f "$dir")"
  if [[ "$FULL_PATH" == "$CURRENT_DIR" ]] || [[ "${#TO_KEEP[@]}" -lt $((KEEP-1)) ]]; then
    TO_KEEP+=("$FULL_PATH")
  fi
done

echo "Protected entries (never deleted):"
for p in "${PROTECTED[@]}"; do
  echo "  $p"
done
echo

echo "Current version:"
echo "  $CURRENT_DIR"
echo

echo "Keeping release directories:"
for d in "${TO_KEEP[@]}"; do
  echo "  $d"
done
echo

echo "Candidates for removal:"
FOUND=false
for dir in "${VERSIONS[@]}"; do
  FULL_PATH="$(readlink -f "$dir")"
  if [[ ! " ${TO_KEEP[*]} " =~ " $FULL_PATH " ]]; then
    FOUND=true
    echo "  $FULL_PATH"
    if $APPLY; then
      rm -rf "$FULL_PATH"
    fi
  fi
done

if ! $FOUND; then
  echo "  (none)"
fi

if ! $APPLY; then
  echo
  echo "Dry-run mode: nothing was deleted."
  echo "Run with --apply to actually remove old versions."
fi