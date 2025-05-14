# Worker Node

This folder contains the `worker.py` script, which acts as a gRPC server responsible for processing video chunks received from the Master server.

## Functionality

- **gRPC Server**: Implements the `VideoProcessingService` defined in `replication.proto`.
    - `ProcessChunk` RPC: Receives a video chunk, its ID, index, and original filename from the Master.
        - Simulates video processing (e.g., encoding). In a real scenario, this is where actual video processing logic would reside.
        - Saves the processed "shard" (chunk) to a local directory (`./video_shards/` within the Worker directory).
        - Returns a success/failure status and the location of the processed shard to the Master.
    - `CheckHealth` RPC: Allows the Master to query the worker's health and load.
        - Reports if it's healthy.
        - Provides current CPU utilization (using `psutil`).
        - Reports the number of active tasks (simulated based on current processing).
    - `GetShard` RPC (Less used in the current master-driven workflow but available): Allows retrieval of a processed shard by its location.
- **Environment Setup**: Includes `setup_env.sh` to create a Python virtual environment (in the project root) and install dependencies, including gRPC tools for code generation from `replication.proto` (within this Worker directory).
- **Protocol Definition**: Contains `replication.proto`, the Protocol Buffers definition file for all gRPC services and messages used in the project (Client-Master and Master-Worker interactions).

## Setup

1.  Navigate to the `Worker` directory.
2.  Run the setup script to create/update the virtual environment in the project root (`video_encoding-main/venv/`) and install dependencies:
    ```bash
    chmod +x setup_env.sh
    ./setup_env.sh
    ```
    This will also generate the necessary Python gRPC files (`replication_pb2.py`, `replication_pb2_grpc.py`, `replication_pb2.pyi`) from `replication.proto` directly within this `Worker` folder.

## Usage

### Running `worker.py` directly (as a gRPC server):

1.  Navigate to the project root (`video_encoding-main/`).
2.  Activate the Python virtual environment:
    ```bash
    source ./venv/bin/activate
    ```
3.  Run the worker server script from the `Worker` directory (relative to project root):
    ```bash
    python Worker/worker.py --host <host_address> --port <port_number>
    ```
    Example (from project root, after activating venv):
    ```bash
    python Worker/worker.py --host localhost --port 50061
    ```
    Or, if you are inside the `Worker` directory (after activating venv from root):
    ```bash
    python worker.py --host localhost --port 50061
    ```
    You can run multiple instances of `worker.py` on different ports (and with different host/port combinations) to simulate multiple worker nodes.

### Using the Startup Script (`start_worker.sh`)

A startup script `start_worker.sh` is provided to simplify launching a worker instance.

1.  Navigate to the project root (`video_encoding-main/`).
2.  Activate the Python virtual environment: `source ./venv/bin/activate`
3.  From the `Worker` directory, make the script executable (if not already) and run it:
    ```bash
    # If in project root:
    # cd Worker/
    # chmod +x start_worker.sh
    # ./start_worker.sh worker1 50061
    # cd ..

    # Or directly from Worker directory (after activating venv from root):
    chmod +x start_worker.sh
    ./start_worker.sh worker1 50061
    ./start_worker.sh worker2 50062 localhost # Example with explicit host
    ```

### Test Scripts

To run test scripts, ensure the virtual environment is activated from the project root (`source ./venv/bin/activate`). Then, navigate to the `Worker` directory to execute them.

-   **`test_worker_client.py`**: A script that acts as a simple client to test the `ProcessChunk` functionality of a single worker. It can send a local video file (chunked) directly to a specified worker. This is useful for isolated worker testing.
    Example (from `Worker` directory, after activating venv from root):
    ```bash
    python test_worker_client.py --worker localhost:50061 --video_path ../sample_videos/file_example_MP4_1920_18MG.mp4
    ```
-   **`test_health_check.py`**: A script to test the `CheckHealth` RPC of one or more workers.
    Example (from `Worker` directory, after activating venv from root):
    ```bash
    python test_health_check.py --workers localhost:50061,localhost:50062
    ```

## Dependencies

- `grpcio`
- `protobuf`
- `psutil` (for system metrics like CPU)
- The generated gRPC Python files (`replication_pb2.py`, `replication_pb2_grpc.py`, `replication_pb2.pyi`) from `replication.proto`, located in this directory.

