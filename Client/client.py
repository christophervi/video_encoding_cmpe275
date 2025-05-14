import grpc
import argparse
import os
import sys
import time

# Add Worker directory to sys.path to find generated gRPC files
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
worker_dir = os.path.join(parent_dir, "Worker")
sys.path.append(worker_dir)

import replication_pb2
import replication_pb2_grpc

CHUNK_SIZE = 1024 * 1024  # 1MB

def generate_video_chunks(video_path):
    """Generates video chunks: first metadata, then data chunks."""
    try:
        filesize = os.path.getsize(video_path)
        filename = os.path.basename(video_path)
        content_type = "video/mp4" # Assuming MP4, can be made more dynamic

        # First, send metadata
        metadata = replication_pb2.VideoMetadata(
            filename=filename,
            filesize=filesize,
            content_type=content_type
        )
        yield replication_pb2.UploadVideoRequest(metadata=metadata)
        print(f"Client: Sent metadata for {filename} ({filesize} bytes)")

        # Then, send video data in chunks
        with open(video_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                yield replication_pb2.UploadVideoRequest(chunk_data=chunk)
                # print(f"Client: Sent chunk of size {len(chunk)} bytes") # Can be verbose
        print(f"Client: Finished sending all chunks for {filename}")
    except FileNotFoundError:
        print(f"Error: Video file {video_path} not found.")
        return
    except Exception as e:
        print(f"Error reading or chunking video file: {e}")
        return

def run_client(master_address, video_path):
    """Connects to the master, uploads video, and prints status updates."""
    if not os.path.exists(video_path):
        print(f"Error: Video file not found at {video_path}")
        return

    print(f"Client: Attempting to connect to master at {master_address}")
    try:
        with grpc.insecure_channel(master_address) as channel:
            stub = replication_pb2_grpc.ClientMasterServiceStub(channel)
            print(f"Client: Connected. Uploading {video_path}...")
            
            upload_start_time = time.time()
            responses = stub.UploadVideo(generate_video_chunks(video_path))
            
            for response in responses:
                print(f"\n--- Master Status Update ---")
                print(f"  Video ID: {response.video_id}")
                print(f"  Status: {replication_pb2.UploadStatusCode.Name(response.status_code)}")
                print(f"  Message: {response.message}")
                if response.total_video_size > 0:
                    print(f"  Bytes Received by Master: {response.bytes_received_by_master}/{response.total_video_size}")
                else:
                    print(f"  Bytes Received by Master: {response.bytes_received_by_master}")
                print(f"  Progress: {response.progress_percentage:.2f}%")

                if response.status_code == replication_pb2.UPLOAD_STATUS_COMPLETED or \
                   response.status_code == replication_pb2.UPLOAD_STATUS_FAILED:
                    if response.metrics:
                        print(f"  --- Benchmark Metrics ---")
                        print(f"    Client Upload Duration: {response.metrics.client_upload_duration_seconds:.2f}s")
                        print(f"    Master Chunking Duration: {response.metrics.master_chunking_duration_seconds:.2f}s")
                        print(f"    Master Distribution Duration: {response.metrics.master_distribution_duration_seconds:.2f}s")
                    break # Stop listening if upload is completed or failed
            
            upload_end_time = time.time()
            print(f"\nClient: Upload process finished. Total client-side time: {upload_end_time - upload_start_time:.2f}s")

    except grpc.RpcError as e:
        print(f"Client: gRPC Error - {e.code()}: {e.details()}")
    except Exception as e:
        print(f"Client: An unexpected error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client to upload video to the Master node.")
    parser.add_argument("--master", default="localhost:50050", help="Master node address (host:port)")
    parser.add_argument("--video_path", required=True, help="Path to the video file to upload.")
    
    args = parser.parse_args()
    
    run_client(args.master, args.video_path)

