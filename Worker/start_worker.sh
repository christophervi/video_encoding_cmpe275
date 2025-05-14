#!/bin/bash

# This script starts the worker.py gRPC server.

# Determine directories
SCRIPT_DIR_WORKER="$(cd "$(dirname "$0")" && pwd)" # Should be /home/ubuntu/video_encoding_project/video_encoding-main/Worker
PROJECT_ROOT_WORKER="$(cd "$SCRIPT_DIR_WORKER/.." && pwd)" # Should be /home/ubuntu/video_encoding_project/video_encoding-main
VENV_PATH_WORKER="$PROJECT_ROOT_WORKER/venv"

# Navigate to the script's directory to ensure relative paths for worker.py are correct
cd "$SCRIPT_DIR_WORKER" || exit

# WORKER_NAME is for logging/identification within this script if needed, not passed to worker.py
WORKER_NAME=$1 
WORKER_PORT=$2
WORKER_HOST=${3:-localhost} # Default host to localhost if not provided as 3rd arg

if [ -z "$WORKER_NAME" ] || [ -z "$WORKER_PORT" ]; then
    echo "Usage: ./start_worker.sh <worker_name_for_log> <port_number> [host_address]"
    echo "Example: ./start_worker.sh worker1 50061"
    echo "Example: ./start_worker.sh worker2 50062 0.0.0.0"
    exit 1
fi

if [ ! -d "$VENV_PATH_WORKER" ]; then
    echo "Error: Python virtual environment not found at $VENV_PATH_WORKER."
    echo "Please run setup_env.sh from the Worker directory, or ensure venv is in project root."
    exit 1
fi

echo "Activating Python virtual environment from $VENV_PATH_WORKER..."
# shellcheck disable=SC1091
source "$VENV_PATH_WORKER/bin/activate"

# Check for psutil and install if missing (already in previous version of script)
if ! python -c "import psutil" &> /dev/null; then
    echo "psutil not found in venv, attempting to install..."
    pip install psutil
    if ! python -c "import psutil" &> /dev/null; then
        echo "Failed to install or find psutil even after attempting install. Please check venv."
        deactivate
        exit 1
    fi
fi

echo "Starting worker.py (name: $WORKER_NAME) on host: $WORKER_HOST, port: $WORKER_PORT..."

# worker.py expects --host and --port arguments.
# The worker_id is constructed internally by worker.py from host and port.
python worker.py --host "$WORKER_HOST" --port "$WORKER_PORT"

RESULT=$?

echo "Deactivating Python virtual environment."
deactivate

if [ $RESULT -eq 0 ]; then
    echo "Worker script for $WORKER_NAME (on $WORKER_HOST:$WORKER_PORT) finished (this usually means it was manually stopped or encountered an issue if it's a long-running server)."
else
    echo "Worker script for $WORKER_NAME (on $WORKER_HOST:$WORKER_PORT) failed with exit code $RESULT."
fi

exit $RESULT

