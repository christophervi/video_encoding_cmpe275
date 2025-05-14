# Master Server (Dual Process)

This folder contains the components for the Master functionality, which is now split into two separate processes for stability and clarity:

1.  **`master_health_monitor.py`**: This process is responsible for:
    *   Periodically checking the health of all configured worker nodes (`worker.py`) using gRPC (`CheckHealth` RPC from `replication.proto`).
    *   Maintaining an internal, up-to-date status map of healthy worker nodes.
    *   Exposing an internal gRPC service (`InternalMasterService` defined in `internal_master.proto`) that the main Master Server can query to get a list of currently healthy workers.

2.  **`master_server_main.py`**: This process is the main client-facing gRPC server and is responsible for:
    *   Implementing the `ClientMasterService` defined in `replication.proto`.
    *   Handling video uploads from `client.py` via the `UploadVideo` RPC.
        *   Receiving video metadata and data chunks.
        *   Streaming status updates back to the client.
    *   Querying the `master_health_monitor.py` process via its internal gRPC service to get a list of healthy workers when a video needs to be distributed.
    *   Chunking the received video.
    *   Distributing these chunks to healthy worker nodes (obtained from the health monitor) using the `ProcessChunk` RPC.
    *   Managing temporary storage for uploaded videos in `/tmp/master_video_uploads/` and ensuring cleanup.

This separation was implemented to resolve segmentation faults caused by running gRPC server and client roles within the same asyncio event loop in the previous single-process `master.py`.

## Setup

1.  Ensure one or more Worker nodes (`worker.py` in the `Worker` folder) are running and their addresses are known.
2.  Make sure the Python virtual environment (now expected at `../venv` relative to this Master directory, or `video_encoding-main/venv` from the project root) is set up with all necessary dependencies (`grpcio`, `grpcio-tools`, `protobuf`, `asyncio`). You can set this up by running `setup_env.sh` in the `Worker` folder (which creates the venv in the project root).

## Usage

The `start_master.sh` script is the recommended way to launch both master components.

### Using the Startup Script (`start_master.sh`)

1.  Navigate to the project root (`video_encoding-main/`).
2.  Activate the Python virtual environment: `source ./venv/bin/activate`
3.  From the `Master` directory, make the script executable (if not already) and run it:
    ```bash
    # If in project root:
    # cd Master/
    # chmod +x start_master.sh
    # ./start_master.sh
    # cd ..

    # Or directly from Master directory (after activating venv from root):
    chmod +x start_master.sh
    ./start_master.sh
    ```
4.  The script will first prompt you to enter the comma-separated worker addresses for the **Health Monitor** (e.g., `localhost:50061,localhost:50062`).
5.  It will then start the `master_health_monitor.py` in the background on its default port (50071).
6.  After a brief pause, it will start `master_server_main.py` (the client-facing server) on its default port (50050). This server will be configured to connect to the health monitor.
7.  When you stop the main server (e.g., with Ctrl+C), the script will attempt to also stop the health monitor process.

### Running Components Manually (for debugging)

You can also run the components separately:

1.  **Start the Health Monitor**:
    Navigate to the project root, activate venv, then:
    ```bash
    python Master/master_health_monitor.py --port <health_monitor_port> --workers <worker1_address:port,...>
    ```
    Example: `python Master/master_health_monitor.py --port 50071 --workers localhost:50061,localhost:50062`

2.  **Start the Main Master Server** (in a new terminal):
    Navigate to the project root, activate venv, then:
    ```bash
    python Master/master_server_main.py --port <main_server_port> --health_monitor_target <health_monitor_address:port>
    ```
    Example: `python Master/master_server_main.py --port 50050 --health_monitor_target localhost:50071`

## Dependencies

- `grpcio`
- `protobuf`
- `asyncio`
- Generated gRPC Python files:
    - `replication_pb2.py`, `replication_pb2_grpc.py` (from `Worker` folder, for client/worker communication)
    - `internal_master_pb2.py`, `internal_master_pb2_grpc.py` (in `Master` folder, for internal master communication)

## Proto Files

-   `replication.proto`: (Located in `Worker` folder) Defines services and messages for client-master and master-worker communication.
-   `internal_master.proto`: (Located in `Master` folder) Defines the internal gRPC service used by `master_server_main.py` to query `master_health_monitor.py`.

