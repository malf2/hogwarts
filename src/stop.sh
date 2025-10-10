#!/bin/bash
set -euo pipefail  # Exit on error, undefined vars, or failed pipeline

# Default values
APP_NAME=${APP_NAME:-"my-spark-app"}   # Spark app name to stop
YARN_CMD=${YARN_CMD:-"yarn"}           # Yarn command path

# Function to get the YARN app ID based on app name
get_app_id() {
  $YARN_CMD application -list 2>/dev/null | grep "$APP_NAME" | awk '{print $1}' | head -n 1
}

# Check if an app with the given name is running
echo "🔍 Checking for running Spark application with name: '$APP_NAME'..."
APP_ID=$(get_app_id)

# Exit successfully if no running app is found
if [[ -z "$APP_ID" ]]; then
  echo "✅ No running instance of '$APP_NAME' found."
  exit 0
fi

# Kill the running YARN app
echo "⚙️  Found running app: $APP_ID (name: $APP_NAME)"
echo "🛑 Attempting to kill it..."

if $YARN_CMD application -kill "$APP_ID" >/dev/null 2>&1; then
  echo "✅ Successfully killed application $APP_ID"
  exit 0
else
  echo "❌ Failed to kill application $APP_ID"
  exit 1
fi