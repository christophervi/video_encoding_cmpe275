#!/bin/bash

# This script starts the two components of the Master: 
# 1. The Master Health Monitor (master_health_monitor.py)
# 2. The Master Server (client-facing) (master_server_main.py)

# Navigate to the script's directory to ensure relative paths are correct
cd "$(dirname "$0")" || exit

# Project root directory (assuming Master is a direct child of video_encoding-main)
PROJECT_ROOT="$(cd .. && pwd)"
MASTER_HEALTH_MONITOR_SCRIPT="$PROJECT_ROOT/Master/master_health_monitor.py"
MASTER_SERVER_SCRIPT="$PROJECT_ROOT/Master/master_server_main.py"

# Default ports
MASTER_SERVER_PORT=${MASTER_SERVER_PORT:-50050}
MASTER_HEALTH_MONITOR_PORT=${MASTER_HEALTH_MONITOR_PORT:-50071}

# Python virtual environment
VENV_PATH="$PROJECT_ROOT/venv"

if [ ! -f "$MASTER_HEALTH_MONITOR_SCRIPT" ]; then
    echo "Error: Master Health Monitor script not found at $MASTER_HEALTH_MONITOR_SCRIPT"
    exit 1
fi

if [ ! -f "$MASTER_SERVER_SCRIPT" ]; then
    echo "Error: Master Server script not found at $MASTER_SERVER_SCRIPT"
    exit 1
fi

if [ -d "$VENV_PATH" ]; then
    echo "Activating Python virtual environment from $VENV_PATH..."
    # shellcheck disable=SC1091
    source "$VENV_PATH/bin/activate"
else
    echo "Warning: Python virtual environment not found at $VENV_PATH. Assuming dependencies are globally installed."
fi

# Prompt for worker addresses required by the health monitor
if [ -z "$WORKERS_ARG" ]; then
  read -r -p "Enter comma-separated worker addresses for the Health Monitor (e.g., localhost:50061,localhost:50062): " WORKER_ADDRESSES_INPUT
  if [ -z "$WORKER_ADDRESSES_INPUT" ]; then
    echo "Error: Worker addresses are required for the Health Monitor."
    if [ -d "$VENV_PATH" ]; then
        deactivate
    fi
    exit 1
  fi
  WORKERS_ARG="$WORKER_ADDRESSES_INPUT"
fi

HEALTH_MONITOR_TARGET="localhost:$MASTER_HEALTH_MONITOR_PORT"

echo "Starting Master Health Monitor on port $MASTER_HEALTH_MONITOR_PORT with workers: $WORKERS_ARG..."
python3 "$MASTER_HEALTH_MONITOR_SCRIPT" --port "$MASTER_HEALTH_MONITOR_PORT" --workers "$WORKERS_ARG" & 
HEALTH_MONITOR_PID=$!
echo "Master Health Monitor started with PID: $HEALTH_MONITOR_PID"

# Give the health monitor a moment to start up
sleep 2

echo "Starting Master Server (client-facing) on port $MASTER_SERVER_PORT..."
echo "Master Server will connect to Health Monitor at: $HEALTH_MONITOR_TARGET"

python3 "$MASTER_SERVER_SCRIPT" --port "$MASTER_SERVER_PORT" --health_monitor_target "$HEALTH_MONITOR_TARGET"
SERVER_RESULT=$?

# Cleanup: kill the health monitor when the main server exits
if ps -p $HEALTH_MONITOR_PID > /dev/null
then
   echo "Stopping Master Health Monitor (PID: $HEALTH_MONITOR_PID)..."
   kill $HEALTH_MONITOR_PID
   wait $HEALTH_MONITOR_PID 2>/dev/null
   echo "Master Health Monitor stopped."
fi

if [ -d "$VENV_PATH" ]; then
    deactivate
fi

if [ $SERVER_RESULT -eq 0 ]; then
    echo "Master Server script finished."
else
    echo "Master Server script failed with exit code $SERVER_RESULT."
fi

exit $SERVER_RESULT

