#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Determine directories
# Assuming this script (setup_env.sh) is in the project root directory
PROJECT_ROOT_SETUP="$(cd "$(dirname "$0")" && pwd)"
WORKER_DIR_SETUP="$PROJECT_ROOT_SETUP/Worker"
MASTER_DIR_SETUP="$PROJECT_ROOT_SETUP/Master"
VENV_DIR_SETUP="$PROJECT_ROOT_SETUP/venv"

echo "--- Project Root: $PROJECT_ROOT_SETUP ---"
echo "--- Worker Dir: $WORKER_DIR_SETUP ---"
echo "--- Master Dir: $MASTER_DIR_SETUP ---"
echo "--- Venv Dir: $VENV_DIR_SETUP ---"

echo "--- Setting up Python virtual environment in $VENV_DIR_SETUP ---"

# Check if the virtual environment directory already exists
if [ ! -d "$VENV_DIR_SETUP" ]; then
  echo "Creating virtual environment in $VENV_DIR_SETUP"
  python3 -m venv "$VENV_DIR_SETUP"
else
  echo "Virtual environment already exists in $VENV_DIR_SETUP"
fi

# Activate the virtual environment
echo "Activating virtual environment from $VENV_DIR_SETUP..."
source "$VENV_DIR_SETUP/bin/activate"

# Ensure pip is up-to-date within the virtual environment
echo "Upgrading pip..."
pip install --upgrade pip

# Install the required dependencies
echo "Installing project dependencies (grpcio, protobuf, grpcio-tools, psutil)..."
pip install grpcio protobuf grpcio-tools psutil

echo "Dependencies installed in $VENV_DIR_SETUP."

echo "--- Compiling Protobuf file (replication.proto in $WORKER_DIR_SETUP) ---"
PROTO_FILE_REPLICATION="$WORKER_DIR_SETUP/replication.proto"
if [ ! -f "$PROTO_FILE_REPLICATION" ]; then
  echo "Error: Protobuf file $PROTO_FILE_REPLICATION not found."
  deactivate
  exit 1
fi
echo "Running protoc compiler on $PROTO_FILE_REPLICATION (from $WORKER_DIR_SETUP)..."
cd "$WORKER_DIR_SETUP"
python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. "replication.proto"
echo "replication.proto compilation successful."

echo "--- Compiling Protobuf file (master_health_interface.proto in $MASTER_DIR_SETUP) ---"
PROTO_FILE_MASTER_HEALTH_INTERFACE="$MASTER_DIR_SETUP/master_health_interface.proto"
if [ ! -f "$PROTO_FILE_MASTER_HEALTH_INTERFACE" ]; then
  echo "Error: Protobuf file $PROTO_FILE_MASTER_HEALTH_INTERFACE not found."
  deactivate
  exit 1
fi
echo "Running protoc compiler on $PROTO_FILE_MASTER_HEALTH_INTERFACE (from $MASTER_DIR_SETUP)..."
cd "$MASTER_DIR_SETUP"
python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. "master_health_interface.proto"
echo "master_health_interface.proto compilation successful."

# Go back to project root
cd "$PROJECT_ROOT_SETUP"

echo "Protobuf compilations successful."
echo "Generated files in $WORKER_DIR_SETUP: replication_pb2.py, replication_pb2_grpc.py, replication_pb2.pyi"
echo "Generated files in $MASTER_DIR_SETUP: master_health_interface_pb2.py, master_health_interface_pb2_grpc.py, master_health_interface_pb2.pyi"

echo "Deactivating virtual environment (setup complete)."
deactivate

echo "--- Setup and Compilation Complete ---"
echo "Virtual environment is located at: $VENV_DIR_SETUP"
echo "To use this virtual environment in a new terminal, navigate to $PROJECT_ROOT_SETUP and run: source ./venv/bin/activate"
echo "You can then run your server, worker, and client scripts."

