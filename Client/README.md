# Client

This folder contains the `client.py` script, which is responsible for uploading video files to the Master server for processing.

## Functionality

- Connects to the Master gRPC server.
- Takes a video file path as input.
- Streams the video to the Master in chunks:
    - First, it sends metadata about the video (filename, size, content type).
    - Then, it sends the video data chunk by chunk.
- Receives and displays real-time status updates from the Master, including:
    - Upload progress (bytes received by master).
    - Current processing status (e.g., received by master, distributing to workers, completed, failed).
    - Benchmark metrics upon completion (e.g., client upload duration, master distribution duration).

## Setup

1.  Ensure the Master server (`master.py` in the `Master` folder) is running.
2.  Ensure one or more Worker nodes (`worker.py` in the `Worker` folder) are running and the Master is configured with their addresses.
3.  Make sure the Python virtual environment (now expected at `../venv` relative to this Client directory, or `video_encoding-main/venv` from the project root) is set up with all necessary dependencies (`grpcio`, `grpcio-tools`, `protobuf`). You can set this up by running `setup_env.sh` in the `Worker` folder (which creates the venv in the project root).

## Usage

### Running `client.py` directly:

1.  Navigate to the project root (`video_encoding-main/`).
2.  Activate the Python virtual environment:
    ```bash
    source ./venv/bin/activate
    ```
3.  Run the client script from the `Client` directory (relative to project root):
    ```bash
    python Client/client.py --master <master_address:port> --video_path <path_to_your_video>
    ```
    Example (from project root, after activating venv):
    ```bash
    python Client/client.py --master localhost:50050 --video_path sample_videos/file_example_MP4_1920_18MG.mp4
    ```
    Or, if you are inside the `Client` directory (after activating venv from root):
    ```bash
    python client.py --master localhost:50050 --video_path ../sample_videos/file_example_MP4_1920_18MG.mp4
    ```

### Using the Test Script (`run_client_test.sh`)

A test script `run_client_test.sh` is provided to simplify uploading the sample video.

1.  Make sure the Master server and Worker nodes are running.
2.  Navigate to the project root (`video_encoding-main/`).
3.  Activate the Python virtual environment: `source ./venv/bin/activate`
4.  From the `Client` directory, make the script executable (if not already) and run it:
    ```bash
    # If in project root:
    # cd Client/
    # chmod +x run_client_test.sh
    # ./run_client_test.sh
    # cd ..

    # Or directly from Client directory (after activating venv from root):
    chmod +x run_client_test.sh
    ./run_client_test.sh
    ```
    This script will automatically use the sample video located in `../sample_videos/` (relative to Client dir) and attempt to connect to the master at `localhost:50050`.

## Dependencies

- `grpcio`
- `protobuf`
- The generated gRPC Python files (`replication_pb2.py`, `replication_pb2_grpc.py`) located in the `Worker` folder (client.py adds `../Worker` to its `sys.path`).

