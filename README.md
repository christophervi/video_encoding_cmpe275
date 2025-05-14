# Distributed Video Encoder Project

This project implements a distributed video encoding system consisting of a Client, a Master (now a dual-process system), and multiple Worker nodes. The system allows a client to upload a video, which is then chunked and distributed by the Master to available Worker nodes for simulated processing.

## Project Structure

```
video_encoding-main/
├── Client/                       # Client application for video upload
│   ├── client.py
│   ├── run_client_test.sh
│   └── README.md
├── Master/                       # Master components for managing workers and distributing tasks
│   ├── master_server_main.py     # Client-facing gRPC server
│   ├── master_health_monitor.py  # Worker health checking process
│   ├── master_health_interface.proto     # gRPC definitions for internal master communication
│   ├── master_health_interface_pb2.py    # Generated gRPC code
│   ├── master_health_interface_pb2_grpc.py # Generated gRPC code
│   ├── master_health_interface_pb2.pyi   # Generated gRPC code
│   ├── start_master.sh           # Script to start both master processes
│   └── README.md
├── Worker/                       # Worker nodes for processing video chunks
│   ├── worker.py
│   ├── replication.proto         # gRPC service and message definitions (Client-Master, Master-Worker)
│   ├── replication_pb2.py        # Generated gRPC Python code
│   ├── replication_pb2_grpc.py   # Generated gRPC Python code
│   ├── replication_pb2.pyi       # Generated gRPC Python code
│   ├── setup_env.sh              # Script to set up Python venv and install deps
│   ├── start_worker.sh
│   ├── test_worker_client.py     # Test client for individual worker
│   ├── test_health_check.py      # Test script for worker health checks
│   └── README.md
├── sample_videos/                # Sample video files for testing
│   └── file_example_MP4_1920_18MG.mp4
├── venv/                         # Python virtual environment for the project
└── README.md                     # This file: Project overview and general instructions
```

## Core Components

1.  **Client (`Client/client.py`)**
    *   Uploads a video file to the Master server (`master_server_main.py`) using gRPC.
    *   Streams the video in chunks (metadata first, then data).
    *   Receives and displays real-time status updates and benchmark metrics from the Master.

2.  **Master (Dual Process - `Master/`)**
    *   **`master_health_monitor.py`**: A dedicated process that monitors the health and status of all registered Worker nodes. It provides this information to the `master_server_main.py` via an internal gRPC service.
    *   **`master_server_main.py`**: The main client-facing gRPC server. It accepts video uploads from clients, queries the `master_health_monitor.py` for available workers, chunks the video, and distributes these chunks to the healthy Worker nodes. It also streams progress and status back to the uploading client.
    *   This separation was implemented to resolve stability issues (segmentation faults) encountered with a single-process master handling both client-facing and worker-monitoring gRPC roles concurrently.

3.  **Worker Node (`Worker/worker.py`)**
    *   A gRPC server that receives video chunks from the Master (`master_server_main.py`).
    *   Simulates video processing (e.g., encoding) and saves the processed "shard".
    *   Reports its health to the `master_health_monitor.py`.

## Overall Workflow

1.  **Setup & Start Servers**:
    *   Start one or more Worker nodes (`Worker/start_worker.sh <worker_id> <port>`).
    *   Start the Master components using `Master/start_master.sh`. This script will launch both `master_health_monitor.py` and `master_server_main.py`. You will be prompted for worker addresses for the health monitor.
2.  **Client Upload**:
    *   Run the Client application (`Client/run_client_test.sh` or `python Client/client.py --video_path ...`).
    *   The Client connects to the `master_server_main.py` and uploads the specified video file.
3.  **Master Processing & Distribution**:
    *   `master_server_main.py` receives the video.
    *   It queries `master_health_monitor.py` to get a list of healthy Worker nodes.
    *   It chunks the video and distributes the chunks to these Worker nodes.
    *   `master_server_main.py` sends status updates (upload progress, distribution status) back to the Client.
4.  **Worker Processing**:
    *   Worker nodes receive chunks, simulate processing, and store the resulting shards.
    *   `master_health_monitor.py` continuously checks worker health.
5.  **Completion**:
    *   `master_server_main.py` informs the Client upon completion or failure, providing final status and any benchmark metrics.

## Getting Started

1.  **Prerequisites**:
    *   Python 3.10+
    *   `pip` (Python package installer)

2.  **Setup Environment & Generate gRPC Code**:
    *   Navigate to the project root directory: `/path/to/video_encoding-main/`.
    *   Run the setup script from the project root:
        ```bash
        chmod +x setup_env.sh
        ./setup_env.sh
        ```
        This creates a Python virtual environment in the project root (`video_encoding-main/venv/`), installs dependencies, and generates gRPC Python stubs from `replication.proto` (in `Worker/`) and `master_health_interface.proto` (in `Master/`).

3.  **Start Worker Nodes**:
    *   Navigate to the project root: `cd /path/to/video_encoding-main/`
    *   Activate the virtual environment: `source ./venv/bin/activate`
    *   From the `Worker/` directory, start one or more worker instances. Each needs a unique ID and port.
        ```bash
        cd Worker/
        ./start_worker.sh worker1 50061
        # In a new terminal, activate venv, cd to Worker, start another:
        # ./start_worker.sh worker2 50062
        cd .. # Go back to project root
        ```

4.  **Start the Master Components**:
    *   Ensure the virtual environment is active (from project root): `source ./venv/bin/activate`.
    *   From the `Master/` directory, run the startup script:
        ```bash
        cd Master/
        chmod +x start_master.sh
        ./start_master.sh
        cd .. # Go back to project root
        ```
    *   When prompted, enter the comma-separated addresses of your running worker nodes (e.g., `localhost:50061,localhost:50062`) for the health monitor.

5.  **Run the Client to Upload a Video**:
    *   Ensure the virtual environment is active (from project root): `source ./venv/bin/activate`.
    *   From the `Client/` directory, run the test script:
        ```bash
        cd Client/
        chmod +x run_client_test.sh
        ./run_client_test.sh
        cd .. # Go back to project root
        ```
    *   Alternatively, run `client.py` directly (ensure correct paths from Client dir):
        ```bash
        python Client/client.py --master localhost:50050 --video_path ../sample_videos/file_example_MP4_1920_18MG.mp4
        ```

## Detailed Instructions

For more detailed instructions on each component, refer to the `README.md` files within the `Client/`, `Master/`, and `Worker/` folders. Ensure you adapt activation paths to use the root `venv`.

## Protocol Definitions

-   **`Worker/replication.proto`**: Defines gRPC services and messages for Client-Master and Master-Worker communication.
-   **`Master/master_health_interface.proto`**: Defines the internal gRPC service used for communication between `master_server_main.py` and `master_health_monitor.py`.


