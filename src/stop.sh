#!/bin/bash
set -euo pipefail

# === CONFIGURATION ===
APP_NAME=${APP_NAME:-"my-spark-app"}   # Can be overridden by environment
YARN_CMD=${YARN_CMD:-"yarn"}           # In case your yarn binary is in a custom path

# === FUNCTIONS ===
get_app_id() {
  $YARN_CMD application -list 2>/dev/null | grep "$APP_NAME" | awk '{print $1}' | head -n 1
}

# === MAIN LOGIC ===
APP_ID=$(get_app_id)

if [[ -z "$APP_ID" ]]; then
  echo "✅ No running instance of '$APP_NAME' found."
  exit 0
fi

echo "⚙️  Found running app: $APP_ID (name: $APP_NAME)"
echo "🛑 Attempting to kill it..."

if $YARN_CMD application -kill "$APP_ID" >/dev/null 2>&1; then
  echo "✅ Successfully killed application $APP_ID"
  exit 0
else
  echo "❌ Failed to kill application $APP_ID"
  exit 1
fi