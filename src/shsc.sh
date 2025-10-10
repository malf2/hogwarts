#!/bin/bash
# ------------------------------------------------------------------------------
# Spark job manager script for Autosys
# ------------------------------------------------------------------------------
# Features:
#  - Ensures only one Spark app with the same name runs
#  - Submits a Spark job using spark3-submit
#  - Gracefully handles termination (SIGTERM/SIGINT)
#  - Streams logs incrementally to $STDOUT (no -follow)
#  - Keeps alive for Autosys monitoring
#  - Returns correct exit code based on Spark job result
# ------------------------------------------------------------------------------

# --- CONFIGURATION -------------------------------------------------------------
APP_NAME="my-spring-kafka-consumer"
JAR_PATH="/path/to/myapp.jar"
MAIN_CLASS="com.example.kafka.KafkaConsumerApp"
QUEUE="default"
STDOUT="/path/to/driver.log"
INTERVAL=10  # seconds between status/log checks
YARN_CMD=$(which yarn)
SPARK_SUBMIT_CMD=$(which spark3-submit)

# --- FUNCTIONS ----------------------------------------------------------------

get_app_id() {
  $YARN_CMD application -list 2>/dev/null | grep "$APP_NAME" | awk '{print $1}'
}

kill_app() {
  local app_id="$1"
  if [ -n "$app_id" ]; then
    echo "[INFO] Killing YARN app: $app_id"
    $YARN_CMD application -kill "$app_id" >/dev/null 2>&1
  fi
}

stream_logs() {
  local app_id="$1"
  local last_line=0
  local tmp_file
  tmp_file=$(mktemp)

  echo "[INFO] Streaming logs for $app_id into $STDOUT"
  : > "$STDOUT"

  while true; do
    $YARN_CMD logs -applicationId "$app_id" > "$tmp_file" 2>/dev/null
    local total_lines
    total_lines=$(wc -l < "$tmp_file")

    if [ "$total_lines" -gt "$last_line" ]; then
      tail -n $((total_lines - last_line)) "$tmp_file" >> "$STDOUT"
      last_line=$total_lines
    fi

    sleep "$INTERVAL"
  done
}

cleanup() {
  echo "[INFO] Received termination signal, killing YARN app..."
  local app_id
  app_id=$(get_app_id)
  kill_app "$app_id"
  echo "[INFO] Exiting gracefully."
  exit 0
}

# --- MAIN LOGIC ---------------------------------------------------------------

trap cleanup SIGTERM SIGINT

existing_app_id=$(get_app_id)
if [ -n "$existing_app_id" ]; then
  echo "[INFO] Existing app detected: $existing_app_id"
  kill_app "$existing_app_id"
  sleep 5
fi

echo "[INFO] Submitting new Spark job..."
$SPARK_SUBMIT_CMD \
  --class "$MAIN_CLASS" \
  --name "$APP_NAME" \
  --master yarn \
  --deploy-mode cluster \
  --queue "$QUEUE" \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.dynamicAllocation.enabled=false \
  "$JAR_PATH"

# Wait for app registration
sleep 10
app_id=$(get_app_id)

if [ -z "$app_id" ]; then
  echo "[ERROR] Unable to retrieve new app ID from YARN."
  exit 2
fi

echo "[INFO] Spark app submitted: $app_id"

# Start log streaming in background
stream_logs "$app_id" &
LOG_PID=$!

# --- Keep script alive & monitor Spark status ---------------------------------

while true; do
  app_state=$($YARN_CMD application -status "$app_id" 2>/dev/null | grep "State :" | awk '{print $3}')
  if [ -z "$app_state" ]; then
    echo "[WARN] Unable to get app status (maybe app not yet registered?)"
  else
    echo "[INFO] App $app_id state: $app_state"
  fi

  case "$app_state" in
    FINISHED)
      final_status=$($YARN_CMD application -status "$app_id" 2>/dev/null | grep "Final-State :" | awk '{print $3}')
      echo "[INFO] App $app_id finished with final state: $final_status"
      kill $LOG_PID 2>/dev/null
      if [ "$final_status" == "SUCCEEDED" ]; then
        exit 0   # ✅ success for Autosys
      else
        exit 1   # ❌ failed
      fi
      ;;
    FAILED|KILLED)
      echo "[ERROR] App $app_id ended with state: $app_state"
      kill $LOG_PID 2>/dev/null
      exit 1
      ;;
  esac

  sleep "$INTERVAL"
done