#!/bin/bash

# This script runs the client.py to upload a video to the master server.
# It assumes the master server and worker nodes are already running.

# Navigate to the script's directory to ensure relative paths are correct
cd "$(dirname "$0")" || exit

# Project root directory (assuming Client is a direct child of video_encoding-main)
PROJECT_ROOT="$(cd .. && pwd)"
CLIENT_SCRIPT="$PROJECT_ROOT/Client/client.py"

# Path to the sample video
# This path will be updated when sample_videos is moved to the root in a later step.
# For now, it points to its location within the Master folder.
SAMPLE_VIDEO_PATH="$PROJECT_ROOT/sample_videos/file_example_MP4_1920_18MG.mp4"

# Master server address (default used by client.py if not specified)
MASTER_ADDRESS="localhost:50050"

# Python virtual environment (expected in Worker directory)
VENV_PATH="$PROJECT_ROOT/venv"

if [ ! -f "$CLIENT_SCRIPT" ]; then
    echo "Error: Client script not found at $CLIENT_SCRIPT"
    exit 1
fi

if [ ! -f "$SAMPLE_VIDEO_PATH" ]; then
    echo "Error: Sample video not found at $SAMPLE_VIDEO_PATH"
    echo "Please ensure the sample video is downloaded and in the correct location."
    exit 1
fi

if [ -d "$VENV_PATH" ]; then
    echo "Activating Python virtual environment from $VENV_PATH..."
    # shellcheck disable=SC1091
    source "$VENV_PATH/bin/activate"
else
    echo "Warning: Python virtual environment not found at $VENV_PATH. Assuming dependencies are globally installed."
fi

echo "Running client.py to upload video: $SAMPLE_VIDEO_PATH to master: $MASTER_ADDRESS"

python3 "$CLIENT_SCRIPT" --master "$MASTER_ADDRESS" --video_path "$SAMPLE_VIDEO_PATH"

RESULT=$?

if [ -d "$VENV_PATH" ]; then
    deactivate
fi

if [ $RESULT -eq 0 ]; then
    echo "Client script finished successfully."
else
    echo "Client script failed with exit code $RESULT."
fi

exit $RESULT

