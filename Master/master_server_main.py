import faulthandler
faulthandler.enable()

import grpc
import asyncio
import os
import sys
import argparse
import logging
import time
import uuid
import random # For simple worker selection
import inspect # For inspect.signature

# Add Worker directory to sys.path to find generated gRPC files for replication.proto
current_dir = os.path.dirname(os.path.abspath(__file__))
worker_dir = os.path.join(os.path.dirname(current_dir), "Worker")
sys.path.append(worker_dir)
# Add Master directory itself for internal_master_pb2
sys.path.append(current_dir)

import replication_pb2
import replication_pb2_grpc
import master_health_interface_pb2
import master_health_interface_pb2_grpc

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration
CHUNK_SIZE = 1024 * 1024  # 1MB chunks
TEMP_VIDEO_DIR = "/tmp/master_video_uploads"

# For persistent channels to workers when sending chunks
worker_chunk_channels_stubs = {}

async def get_or_create_worker_chunk_channel_stub(worker_address):
    if worker_address in worker_chunk_channels_stubs:
        channel = worker_chunk_channels_stubs[worker_address]["channel"]
        return channel, worker_chunk_channels_stubs[worker_address]["stub"]
    logger.info(f"Creating new chunk channel and stub for worker {worker_address}")
    channel = grpc.aio.insecure_channel(worker_address)
    stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
    worker_chunk_channels_stubs[worker_address] = {"channel": channel, "stub": stub}
    return channel, stub

async def close_all_worker_chunk_channels():
    logger.info("Closing all worker chunk channels...")
    for worker_address, data in list(worker_chunk_channels_stubs.items()):
        try:
            channel = data["channel"]
            if channel:
                await channel.close()
                logger.info(f"Closed chunk channel for {worker_address}")
        except Exception as e:
            logger.error(f"Error closing chunk channel for {worker_address}: {e}")
    worker_chunk_channels_stubs.clear()

async def send_chunk_to_worker(worker_address, video_id, chunk_index, chunk_data, original_filename):
    logger.debug(f"Sending chunk {chunk_index} of {video_id} to worker {worker_address}")
    channel, stub = await get_or_create_worker_chunk_channel_stub(worker_address)
    try:
        request = replication_pb2.ProcessChunkRequest(
            video_id=video_id,
            chunk_index=chunk_index,
            chunk_data=chunk_data,
            original_filename=original_filename
        )
        response = await stub.ProcessChunk(request, timeout=30.0)
        if response.success:
            logger.info(f"Chunk {chunk_index} for {video_id} (orig: {original_filename}) successfully processed by {worker_address}. Shard: {response.shard_location}")
            return True, response.shard_location
        else:
            logger.error(f"Worker {worker_address} failed to process chunk {chunk_index} for {video_id}: {response.message}")
            return False, None
    except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
        logger.error(f"Error sending chunk {chunk_index} to worker {worker_address}: {type(e).__name__} - {e}")
        if worker_address in worker_chunk_channels_stubs:
            try:
                ch = worker_chunk_channels_stubs[worker_address]["channel"]
                if ch:
                    await ch.close()
                    logger.info(f"Closed problematic chunk channel for {worker_address} after error.")
            except Exception as close_e:
                logger.error(f"Error closing problematic chunk channel for {worker_address}: {close_e}")
            del worker_chunk_channels_stubs[worker_address]
        return False, None
    except Exception as e:
        logger.error(f"Unexpected error sending chunk {chunk_index} to worker {worker_address}: {type(e).__name__} - {e}", exc_info=True)
        return False, None

async def distribute_video_chunks_to_workers(video_id, temp_video_path, original_filename, status_callback, health_monitor_stub):
    logger.info(f"[DISTRIBUTE_CHUNKS_BEGIN] video_id={video_id}, status_callback_id={id(status_callback)}, status_callback_type={type(status_callback)}, status_callback_sig={inspect.signature(status_callback)}")
    distribution_start_time = time.time()
    master_chunking_total_duration = 0.0

    if not os.path.exists(temp_video_path):
        logger.error(f"Temporary video file not found: {temp_video_path}")
        await status_callback(replication_pb2.UPLOAD_STATUS_FAILED, "Internal error: Temp video file missing", 0)
        return {}

    try:
        file_size = os.path.getsize(temp_video_path)
        if file_size == 0:
            logger.warning(f"Temporary video file {temp_video_path} is empty for video_id {video_id}.")
            # Handle empty file case appropriately, perhaps by sending a failed status
            await status_callback(replication_pb2.UPLOAD_STATUS_FAILED, "Video file is empty, nothing to distribute.", 0)
            return {}

        total_chunks_for_distribution = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        logger.info(f"[{video_id}] Total chunks to distribute: {total_chunks_for_distribution} from file size {file_size}")


    except FileNotFoundError: # Should be caught by os.path.exists, but good to have
        logger.error(f"Error: Video file not found at {temp_video_path} (during size check)")
        await status_callback(replication_pb2.UPLOAD_STATUS_FAILED, "Internal error: Temp video file disappeared.", 0)
        return {}
    except Exception as e_size: # Catch other potential errors from getsize or calculation
        logger.error(f"Error calculating total chunks for {temp_video_path}: {e_size}")
        await status_callback(replication_pb2.UPLOAD_STATUS_FAILED, f"Internal error: Could not determine video size for distribution: {e_size}", 0)
        return {}


    chunk_index = 0
    results = {}
    no_worker_retry_delay = 5
    total_chunks_sent = 0
    successful_chunks = 0
    recently_assigned_tasks = {}

    try:
        with open(temp_video_path, "rb") as f:
            while True:
                read_start_time = time.time()
                chunk_data = f.read(CHUNK_SIZE)
                read_end_time = time.time()
                master_chunking_total_duration += (read_end_time - read_start_time)

                if not chunk_data:
                    break
                
                selected_worker = None
                retries = 3
                
                try:
                    if not health_monitor_stub:
                        logger.error("Health monitor stub is not available. Cannot get healthy workers.")
                        selected_worker = None
                    else:
                        health_response = await health_monitor_stub.GetHealthyWorkers(...)
                        available_workers_from_monitor = list(health_response.workers_with_load)

                        # Adjust load based on recently assigned tasks
                        candidate_workers = []
                        for w_info in available_workers_from_monitor:
                            optimistic_tasks = w_info.active_tasks + recently_assigned_tasks.get(w_info.address, 0)
                            candidate_workers.append({
                                'address': w_info.address,
                                'active_tasks': optimistic_tasks,
                                'cpu_utilization': w_info.cpu_utilization
                            })

                        if candidate_workers:
                            candidate_workers.sort(key=lambda w: (w['active_tasks'], w['cpu_utilization']))
                            selected_worker_details = candidate_workers[0]
                            selected_worker = selected_worker_details['address']

                            # Increment the count for the selected worker
                            recently_assigned_tasks[selected_worker] = recently_assigned_tasks.get(selected_worker, 0) + 1

                            logger.info(f"Selected worker {selected_worker} (Optimistic Tasks: {selected_worker_details['active_tasks']}, CPU: {selected_worker_details['cpu_utilization']:.2f}%)")
                        else:
                            logger.warning(f"Health monitor returned no healthy workers. Response: {health_response} for chunk {chunk_index}")
                            selected_worker = None
                except grpc.aio.AioRpcError as e:
                    logger.error(f"Failed to get healthy workers from monitor (gRPC Error) for chunk {chunk_index}: {e.code()} - {e.details()}", exc_info=True)
                    selected_worker = None
                except asyncio.TimeoutError as e:
                    logger.error(f"Timeout getting healthy workers from monitor for chunk {chunk_index}: {e}", exc_info=True)
                    selected_worker = None
                except Exception as e:
                    logger.error(f"Failed to get healthy workers from monitor (Exception) for chunk {chunk_index}: {e}", exc_info=True)
                    selected_worker = None

                if selected_worker is None:
                    logger.warning(f"No suitable worker for chunk {chunk_index} of {video_id}. Waiting {no_worker_retry_delay}s (retries left: {retries-1})...")
                    dist_waiting_progress = (successful_chunks * 100.0 / total_chunks_for_distribution) if total_chunks_for_distribution > 0 else 0
                    await status_callback(replication_pb2.UPLOAD_STATUS_DISTRIBUTING_TO_WORKERS, f"Waiting for available worker for chunk {chunk_index}", dist_waiting_progress)
                    await asyncio.sleep(no_worker_retry_delay)
                    retries -=1
                    continue # Skip to next retry or break if retries exhausted for this chunk
                
                if selected_worker is None: # This condition means retries were exhausted
                    logger.error(f"Could not find a worker for chunk {chunk_index} of {video_id} after multiple retries. Aborting distribution for this video.")
                    final_dist_progress = (successful_chunks * 100.0 / total_chunks_for_distribution) if total_chunks_for_distribution > 0 else 0
                    await status_callback(replication_pb2.UPLOAD_STATUS_FAILED, f"Failed to find worker for chunk {chunk_index}", final_dist_progress, master_chunking_duration=master_chunking_total_duration)
                    break # Break the main chunk reading loop

                # Calculate distribution progress before sending the chunk
                # Progress reflects how many chunks we *are about to send* or *have initiated sending for*.
                # Using (chunk_index + 1) as we are processing the current chunk_index.
                current_distribution_progress = ((chunk_index + 1) * 100.0 / total_chunks_for_distribution) if total_chunks_for_distribution > 0 else 0
                current_distribution_progress = min(current_distribution_progress, 100.0) # Cap at 100%

                logger.info(f"Sending chunk {chunk_index} of {video_id} (Progress: {current_distribution_progress:.2f}%) to selected worker {selected_worker}")
                await status_callback(
                    replication_pb2.UPLOAD_STATUS_DISTRIBUTING_TO_WORKERS, 
                    f"Sending chunk {chunk_index} to {selected_worker}", 
                    current_distribution_progress # Pass calculated percentage
                )
                                
                success, shard_location = await send_chunk_to_worker(selected_worker, video_id, chunk_index, chunk_data, original_filename)
                total_chunks_sent +=1 # Increment regardless of success, as an attempt was made
                if success:
                    successful_chunks +=1
                
                results[f"{video_id}_chunk_{chunk_index}"] = {
                    "worker": selected_worker,
                    "status": "success" if success else "failed",
                    "shard_location": shard_location if success else None
                }

                if not success:
                    logger.warning(f"Failed to process chunk {chunk_index} on worker {selected_worker}.")
                    # Note: if a single chunk fails, might decide to abort distribution.
                    # For now, it continues trying other chunks.
                
                chunk_index += 1
        
        distribution_duration = time.time() - distribution_start_time
        final_message_progress = (successful_chunks * 100.0 / total_chunks_for_distribution) if total_chunks_for_distribution > 0 else 0
        final_message_progress = min(final_message_progress, 100.0)

        logger.info(f"Finished distributing video \'{video_id}\'. Total Chunks in file: {total_chunks_for_distribution}. Attempted to send: {total_chunks_sent}. Successful: {successful_chunks}. Duration: {distribution_duration:.2f}s. Master Chunking Time: {master_chunking_total_duration:.2f}s")
        
        if successful_chunks == total_chunks_for_distribution and total_chunks_for_distribution > 0 : # Ensure all expected chunks were successfully processed
            await status_callback(replication_pb2.UPLOAD_STATUS_COMPLETED, "All chunks distributed and processed successfully.", 100.0, master_dist_duration=distribution_duration, master_chunking_duration=master_chunking_total_duration)
        elif successful_chunks > 0: # Partial success
            # Ensure UPLOAD_STATUS_COMPLETED_PARTIAL is defined in .proto if decide to use it
            # For now, let's assume it would be a type of FAILED or a specific COMPLETED_PARTIAL status
            status_to_send = getattr(replication_pb2, "UPLOAD_STATUS_COMPLETED_PARTIAL", replication_pb2.UPLOAD_STATUS_FAILED) # Fallback to FAILED
            message_to_send = f"Partial success: {successful_chunks}/{total_chunks_for_distribution} chunks processed successfully."
            if status_to_send == replication_pb2.UPLOAD_STATUS_FAILED and successful_chunks == total_chunks_sent :
                 message_to_send = f"All chunks sent, but only {successful_chunks}/{total_chunks_for_distribution} processed successfully by workers."
            elif status_to_send == replication_pb2.UPLOAD_STATUS_FAILED :
                  message_to_send = f"Distribution failed: {successful_chunks}/{total_chunks_for_distribution} chunks processed successfully."


            await status_callback(status_to_send, message_to_send, final_message_progress, master_dist_duration=distribution_duration, master_chunking_duration=master_chunking_total_duration)
        else: # Complete failure to process any chunk
            await status_callback(replication_pb2.UPLOAD_STATUS_FAILED, "Failed to process any chunks successfully.", 0.0, master_dist_duration=distribution_duration, master_chunking_duration=master_chunking_total_duration)

    except FileNotFoundError: # This catch block might be redundant if initial check handles it
        logger.error(f"Error: Video file not found at {temp_video_path}")
        await status_callback(replication_pb2.UPLOAD_STATUS_FAILED, "Internal error: Temp video file missing after upload.", 0, master_chunking_duration=master_chunking_total_duration)
    except Exception as e:
        logger.error(f"An error occurred during video chunk distribution for {video_id}: {e}", exc_info=True)
        await status_callback(replication_pb2.UPLOAD_STATUS_FAILED, f"Internal error during distribution: {str(e)}", 0, master_chunking_duration=master_chunking_total_duration)
    finally:
        if os.path.exists(temp_video_path):
            try:
                os.remove(temp_video_path)
                logger.info(f"Cleaned up temporary video file: {temp_video_path}")
            except OSError as e:
                logger.error(f"Error deleting temporary video file {temp_video_path}: {e}")
    return results

class ClientMasterServicer(replication_pb2_grpc.ClientMasterServiceServicer):
    def __init__(self, health_monitor_channel):
        self.health_monitor_channel = health_monitor_channel
        self.health_monitor_stub = None
        if self.health_monitor_channel:
            self.health_monitor_stub = master_health_interface_pb2_grpc.InternalMasterServiceStub(health_monitor_channel)
        
        if not os.path.exists(TEMP_VIDEO_DIR):
            os.makedirs(TEMP_VIDEO_DIR)
        health_monitor_status = "created" if self.health_monitor_stub else "not created"
        logger.info(f"ClientMasterServicer initialized. Temp dir: {TEMP_VIDEO_DIR}. Health monitor stub {health_monitor_status}.")

    async def UploadVideo(self, request_iterator, context):
        video_id = str(uuid.uuid4())
        logger.info(f"[{video_id}] New video upload request received.")
        
        metadata = None
        temp_video_path = None
        video_file = None 
        bytes_received = 0
        upload_start_time = time.time()
        client_upload_duration = None

        # This is the actual function that gets called by the lambda
        async def _send_status_impl(status_code, message, progress_percentage, **kwargs):
            nonlocal client_upload_duration # Allow modification of outer scope variable
            client_upload_duration_val = kwargs.get("client_upload_duration_val")
            master_dist_duration = kwargs.get("master_dist_duration")
            master_chunk_dur = kwargs.get("master_chunking_duration") # Get the new metric

            logger.debug(f"[_SEND_STATUS_IMPL CALLED] video_id={video_id}, status_code={status_code}, message=\"{message}\", progress={progress_percentage}, kwargs={kwargs}")
            if client_upload_duration_val is not None:
                 client_upload_duration = client_upload_duration_val

            metrics = None
            # Check if any metric is available before creating the BenchmarkMetrics object
            if client_upload_duration is not None or \
               master_dist_duration is not None or \
               master_chunk_dur is not None:
                metrics = replication_pb2.BenchmarkMetrics(
                    client_upload_duration_seconds=client_upload_duration if client_upload_duration is not None else 0,
                    master_chunking_duration_seconds=master_chunk_dur if master_chunk_dur is not None else 0, # Use the new metric
                    master_distribution_duration_seconds=master_dist_duration if master_dist_duration is not None else 0
                )
            
            total_size = metadata.filesize if metadata else 0
            effective_progress = progress_percentage
            if progress_percentage == -1: # Calculate progress if -1 is passed
                 effective_progress = min(bytes_received * 100.0 / total_size, 99.9) if total_size > 0 else 50

            response_metrics = replication_pb2.UploadVideoStatusResponse(
                video_id=video_id,
                status_code=status_code,
                message=message,
                progress_percentage=effective_progress,
                bytes_received_by_master=bytes_received,
                total_video_size=total_size,
                metrics=metrics
            )
            try:
                await context.write(response_metrics)
                logger.debug(f"[{video_id}] Status sent to client: {replication_pb2.UploadStatusCode.Name(status_code)} - {message} - Progress: {effective_progress:.2f}%")
            except grpc.aio.AioRpcError as e:
                logger.warning(f"[{video_id}] Failed to send status to client (gRPC error): {e.code()} - {e.details()}")

        # The lambda that will be passed as status_callback
        # This lambda captures client_upload_duration from its outer scope (UploadVideo)
        # and passes it to _send_status_impl along with other arguments.
        the_lambda_callback = lambda s, m, p, **kwargs: _send_status_impl(s, m, p, **kwargs)
        logger.debug(f"[UPLOAD_VIDEO_LAMBDA_DEF] video_id={video_id}, defined the_lambda_callback_id={id(the_lambda_callback)}, type={type(the_lambda_callback)}, signature={inspect.signature(the_lambda_callback)}")

        try:
            async for request_chunk in request_iterator:
                if request_chunk.HasField("metadata"):
                    metadata = request_chunk.metadata
                    original_filename = metadata.filename if metadata.filename else f"{video_id}.mp4"
                    temp_video_path = os.path.join(TEMP_VIDEO_DIR, f"{video_id}_{original_filename}")
                    logger.info(f"[{video_id}] Received metadata: Filename={metadata.filename}, Size={metadata.filesize}, Type={metadata.content_type}. Temp path: {temp_video_path}")
                    logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_IN_PROGRESS}, \"Metadata received, awaiting video data.\", 0)")
                    await the_lambda_callback(replication_pb2.UPLOAD_STATUS_IN_PROGRESS, "Metadata received, awaiting video data.", 0)
                    try:
                        video_file = open(temp_video_path, "wb")
                    except IOError as e:
                        logger.error(f"[{video_id}] Failed to open temp file {temp_video_path} for writing: {e}")
                        logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_FAILED}, f\"Internal server error: could not save video data.\", 0)")
                        await the_lambda_callback(replication_pb2.UPLOAD_STATUS_FAILED, f"Internal server error: could not save video data.", 0)
                        return
                elif request_chunk.HasField("chunk_data"):
                    if metadata is None or temp_video_path is None or video_file is None:
                        logger.error(f"[{video_id}] Received chunk data before metadata or temp file was ready.")
                        logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_FAILED}, \"Chunk data received before metadata.\", 0)")
                        await the_lambda_callback(replication_pb2.UPLOAD_STATUS_FAILED, "Chunk data received before metadata.", 0)
                        return
                    try:
                        video_file.write(request_chunk.chunk_data)
                        bytes_received += len(request_chunk.chunk_data)
                        # Send progress update, using -1 to indicate calculation based on bytes_received
                        if bytes_received % (CHUNK_SIZE * 5) == 0 or bytes_received == metadata.filesize: # Update every 5MB or on completion
                            logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_IN_PROGRESS}, \"Video data receiving in progress.\", -1)")
                            await the_lambda_callback(replication_pb2.UPLOAD_STATUS_IN_PROGRESS, "Video data receiving in progress.", -1)
                    except IOError as e:
                        logger.error(f"[{video_id}] Failed to write to temp file {temp_video_path}: {e}")
                        logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_FAILED}, f\"Internal server error: could not write video data.\", 0)")
                        await the_lambda_callback(replication_pb2.UPLOAD_STATUS_FAILED, f"Internal server error: could not write video data.", 0)
                        return
                else:
                    logger.warning(f"[{video_id}] Received empty or unknown request part.")
            
            if video_file:
                video_file.close()
                video_file = None
            
            if metadata and bytes_received == metadata.filesize:
                current_client_upload_duration = time.time() - upload_start_time
                logger.info(f"[{video_id}] Video data fully received by master. Total bytes: {bytes_received}. Client upload duration: {current_client_upload_duration:.2f}s")
                logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_RECEIVED_BY_MASTER}, \"Video data fully received by master.\", 100, client_upload_duration_val={current_client_upload_duration})")
                await the_lambda_callback(replication_pb2.UPLOAD_STATUS_RECEIVED_BY_MASTER, "Video data fully received by master.", 100, client_upload_duration_val=current_client_upload_duration)
                
                logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_PROCESSING_BY_MASTER}, \"Master is preparing video for distribution.\", 0)")
                await the_lambda_callback(replication_pb2.UPLOAD_STATUS_PROCESSING_BY_MASTER, "Master is preparing video for distribution.", 0)
                
                logger.debug(f"[UPLOAD_VIDEO_PASS_LAMBDA_TO_DISTRIBUTE] Passing the_lambda_callback (id={id(the_lambda_callback)}) to distribute_video_chunks_to_workers")
                await distribute_video_chunks_to_workers(video_id, temp_video_path, metadata.filename, the_lambda_callback, self.health_monitor_stub)
            elif metadata:
                logger.warning(f"[{video_id}] Video upload incomplete. Expected {metadata.filesize} bytes, received {bytes_received}.")
                logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_FAILED}, \"Video upload incomplete.\", -1)")
                await the_lambda_callback(replication_pb2.UPLOAD_STATUS_FAILED, "Video upload incomplete.", -1)
            else:
                logger.error(f"[{video_id}] No metadata received, cannot process video.")
                logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_FAILED}, \"No metadata received.\", 0)")
                await the_lambda_callback(replication_pb2.UPLOAD_STATUS_FAILED, "No metadata received.", 0)

        except grpc.aio.AioRpcError as e:
            logger.error(f"[{video_id}] gRPC error during UploadVideo: {e.code()} - {e.details()}", exc_info=True)
            try:
                logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA_GRPC_ERROR] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_FAILED}, f\"gRPC error: {e.details()}\", 0)")
                await the_lambda_callback(replication_pb2.UPLOAD_STATUS_FAILED, f"gRPC error: {e.details()}", 0)
            except Exception as final_e:
                logger.error(f"[{video_id}] Failed to send final error status: {final_e}")
        except Exception as e:
            logger.error(f"[{video_id}] Unexpected error during UploadVideo: {e}", exc_info=True)
            try:
                logger.debug(f"[UPLOAD_VIDEO_INVOKE_LAMBDA_UNEXPECTED_ERROR] Invoking the_lambda_callback (id={id(the_lambda_callback)}) with args: ({replication_pb2.UPLOAD_STATUS_FAILED}, f\"Unexpected server error: {str(e)}\", 0)")
                await the_lambda_callback(replication_pb2.UPLOAD_STATUS_FAILED, f"Unexpected server error: {str(e)}", 0)
            except Exception as final_e:
                logger.error(f"[{video_id}] Failed to send final error status after unexpected error: {final_e}")
        finally:
            if video_file:
                try:
                    video_file.close()
                except Exception as e_close:
                    logger.error(f"[{video_id}] Error closing video file in finally block: {e_close}")
            # Temp file is cleaned up by distribute_video_chunks_to_workers or if it fails before calling it.
            # However, if UploadVideo itself fails before calling distribute, ensure cleanup.
            if temp_video_path and os.path.exists(temp_video_path) and not (metadata and bytes_received == metadata.filesize): # Only if not passed to distribute
                try:
                    os.remove(temp_video_path)
                    logger.info(f"[{video_id}] Cleaned up temp video file {temp_video_path} in UploadVideo finally block.")
                except OSError as e_os:
                    logger.error(f"[{video_id}] Error deleting temp video file {temp_video_path} in UploadVideo finally: {e_os}")
            logger.info(f"[{video_id}] UploadVideo handler finishing.")

async def serve(port, health_monitor_target):
    server = grpc.aio.server()
    health_monitor_channel = None
    if health_monitor_target:
        health_monitor_channel = grpc.aio.insecure_channel(health_monitor_target)
    
    replication_pb2_grpc.add_ClientMasterServiceServicer_to_server(ClientMasterServicer(health_monitor_channel), server)
    server.add_insecure_port(f"[::]:{port}")
    logger.info(f"Master server starting on port {port}...")
    await server.start()
    logger.info(f"Master server started. Listening on port {port}.")
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Master server stopping due to KeyboardInterrupt...")
    finally:
        await server.stop(grace=None) # Allow ongoing RPCs to complete
        logger.info("Master server stopped.")
        if health_monitor_channel:
            await health_monitor_channel.close()
            logger.info("Closed health monitor channel.")
        await close_all_worker_chunk_channels()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client-facing Master Server for Video Encoding")
    parser.add_argument("--port", type=int, default=50050, help="Port for the master server to listen on")
    parser.add_argument("--health_monitor_target", type=str, default="localhost:50071", help="Address of the health monitor service (e.g., localhost:50071)")
    args = parser.parse_args()
    asyncio.run(serve(args.port, args.health_monitor_target))

