import asyncio
import inspect
import logging
import time

# Mock replication_pb2 for status codes
class MockReplicationPb2:
    UPLOAD_STATUS_COMPLETED = 0
    UPLOAD_STATUS_FAILED = 1
    UPLOAD_STATUS_PROCESSING_BY_MASTER = 2
    UPLOAD_STATUS_DISTRIBUTING_TO_WORKERS = 3
    UPLOAD_STATUS_RECEIVED_BY_MASTER = 4

replication_pb2 = MockReplicationPb2()

# --- Simplified/Mocked version of _send_status_impl ---
async def _send_status_impl_mock(video_id, client_upload_duration, status_code, message, progress_percentage, **kwargs):
    master_dist_duration = kwargs.get("master_dist_duration")
    print(f"[{video_id}][MOCK _send_status_impl] CALLED with: status_code={status_code}, msg=\"{message}\", progress={progress_percentage}, client_upload_duration={client_upload_duration}, master_dist_duration={master_dist_duration}, other_kwargs={kwargs}")
    await asyncio.sleep(0.01)
    print(f"[{video_id}][MOCK _send_status_impl] FINISHED")
    return True

# --- Simplified/Mocked version of distribute_video_chunks_to_workers ---
async def distribute_video_chunks_to_workers_mock(video_id, status_callback):
    print(f"[{video_id}][MOCK distribute_chunks] ENTERED. Received status_callback: id={id(status_callback)}, type={type(status_callback)}, sig={inspect.signature(status_callback)}")
    
    distribution_duration = 5.5 # Example duration
    
    await asyncio.sleep(0.1)
    
    print(f"[{video_id}][MOCK distribute_chunks] ATTEMPTING to call status_callback with master_dist_duration.")
    try:
        # This is the call that mirrors the one causing TypeError in the main code
        await status_callback(
            replication_pb2.UPLOAD_STATUS_COMPLETED, 
            "Mock: All chunks distributed.", 
            100, 
            master_dist_duration=distribution_duration # Critical keyword argument
        )
        print(f"[{video_id}][MOCK distribute_chunks] status_callback call SUCCEEDED.")
    except TypeError as e:
        print(f"[{video_id}][MOCK distribute_chunks] status_callback call FAILED with TypeError: {e}")
        # raise # Optionally re-raise to halt execution
    except Exception as e:
        print(f"[{video_id}][MOCK distribute_chunks] status_callback call FAILED with other Exception: {e}")
        # raise
    
    print(f"[{video_id}][MOCK distribute_chunks] EXITED.")
    return {"status": "ok"}

# --- Simplified/Mocked version of UploadVideo (where the lambda is defined) ---
async def UploadVideo_mock():
    video_id = "test_video_123"
    client_upload_duration = 2.3 # Example duration
    
    # Define the lambda callback, mirroring the structure in master_server_main.py
    the_lambda_callback = lambda s, m, p, **kwargs: _send_status_impl_mock(
        video_id, 
        client_upload_duration, 
        s, m, p, 
        **kwargs
    )
    
    print(f"[{video_id}][MOCK UploadVideo] DEFINED the_lambda_callback: id={id(the_lambda_callback)}, type={type(the_lambda_callback)}, sig={inspect.signature(the_lambda_callback)}")
    
    print(f"[{video_id}][MOCK UploadVideo] PASSING the_lambda_callback to distribute_chunks_mock.")
    await distribute_video_chunks_to_workers_mock(video_id, the_lambda_callback)
    print(f"[{video_id}][MOCK UploadVideo] FINISHED.")

async def main():
    print("--- Starting Minimal Callback Test ---")
    await UploadVideo_mock()
    print("--- Minimal Callback Test Finished ---")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())

