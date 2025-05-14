import grpc
import asyncio
import os
import sys
import argparse
import logging
import time
import faulthandler

faulthandler.enable()

# Add Worker directory to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
worker_dir = os.path.join(os.path.dirname(current_dir), "Worker")
sys.path.append(worker_dir)

import replication_pb2
import replication_pb2_grpc
import master_health_interface_pb2
import master_health_interface_pb2_grpc

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

HEALTH_CHECK_INTERVAL = 5  # seconds
MASTER_ID = "master-health-monitor-01"

worker_status_map = {}
worker_channels_stubs = {} # To store persistent channels and stubs
worker_status_lock = asyncio.Lock()

async def get_or_create_channel_stub(worker_address):
    if worker_address not in worker_channels_stubs or worker_channels_stubs[worker_address]["channel"].is_closed():
        logger.info(f"Creating new channel and stub for {worker_address}")
        channel = grpc.aio.insecure_channel(worker_address)
        stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
        worker_channels_stubs[worker_address] = {"channel": channel, "stub": stub}
        return channel, stub
    return worker_channels_stubs[worker_address]["channel"], worker_channels_stubs[worker_address]["stub"]

async def close_all_worker_channels():
    logger.info("Closing all worker channels...")
    for worker_address, data in list(worker_channels_stubs.items()): # list() for safe iteration if del were inside loop for this specific entry
        try:
            if data["channel"]:
                await data["channel"].close()
                logger.info(f"Closed channel for {worker_address}")
        except Exception as e:
            logger.error(f"Error closing channel for {worker_address}: {e}")
    worker_channels_stubs.clear()

async def get_worker_health(worker_address):
    logger.debug(f"Attempting health check for worker: {worker_address}")
    channel, stub = await get_or_create_channel_stub(worker_address)
    try:
        request = replication_pb2.HealthCheckRequest(master_id=MASTER_ID)
        response = await stub.CheckHealth(request, timeout=5.0)
        logger.debug(f"Health check response from {worker_address}: Healthy={response.is_healthy}, Msg=\"{response.message}\"")
        async with worker_status_lock:
            if worker_address not in worker_status_map:
                worker_status_map[worker_address] = {}

            status_entry = worker_status_map[worker_address]
            status_entry["healthy"] = response.is_healthy
            status_entry["last_seen"] = time.monotonic()
            status_entry["failed_checks"] = 0
            status_entry["message"] = response.message

            if response.is_healthy:
                status_entry["worker_id"] = response.worker_id
                status_entry["cpu_utilization"] = response.cpu_utilization
                status_entry["memory_usage_bytes"] = response.memory_usage_bytes
                status_entry["active_tasks"] = response.active_tasks
            else: # Clear or reset metrics if unhealthy
                status_entry.pop("worker_id", None)
                status_entry.pop("cpu_utilization", None)
                status_entry.pop("memory_usage_bytes", None)
                status_entry.pop("active_tasks", None)
            # logger.debug(f"Updated worker_status_map for {worker_address}: {status_entry}")
        if not response.is_healthy:
            logger.warning(f"Worker {worker_address} reported unhealthy: {response.message}")
        return True
    except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
        logger.warning(f"Health check failed for worker {worker_address}: {type(e).__name__} - {e}")
        async with worker_status_lock:
            if worker_address not in worker_status_map:
                 worker_status_map[worker_address] = {"healthy": False, "last_seen": 0, "failed_checks": 0, "message": "Connection failed"}
            worker_status_map[worker_address]["healthy"] = False
            worker_status_map[worker_address]["failed_checks"] = worker_status_map[worker_address].get("failed_checks", 0) + 1
            logger.debug(f"Marked {worker_address} as unhealthy due to gRPC/Timeout error. Status: {worker_status_map[worker_address]}")
        # Attempt to close and remove potentially problematic channels/stubs
        if worker_address in worker_channels_stubs:
            try:
                ch = worker_channels_stubs[worker_address]["channel"]
                if ch: # Just check if the channel object exists
                    await ch.close()
                    logger.info(f"Closed channel for {worker_address} after error.")
            except Exception as close_e:
                logger.error(f"Error closing channel for {worker_address} after error: {close_e}")
            del worker_channels_stubs[worker_address]
        return False
    except Exception as e:
        logger.error(f"Unexpected error during health check for {worker_address}: {type(e).__name__} - {e}", exc_info=True)
        async with worker_status_lock:
            if worker_address not in worker_status_map:
                 worker_status_map[worker_address] = {"healthy": False, "last_seen": 0, "failed_checks": 0, "message": "Unexpected error"}
            worker_status_map[worker_address]["healthy"] = False
            worker_status_map[worker_address]["failed_checks"] = worker_status_map[worker_address].get("failed_checks", 0) + 1
            logger.debug(f"Marked {worker_address} as unhealthy due to unexpected error. Status: {worker_status_map[worker_address]}")
        return False

async def update_all_worker_statuses_periodically(worker_addresses):
    logger.info("Starting periodic worker health checks...")
    while True:
        if not worker_addresses:
            logger.warning("No worker addresses configured for health checks.")
            await asyncio.sleep(HEALTH_CHECK_INTERVAL) 
            continue
        logger.debug(f"Performing periodic health check for workers: {worker_addresses}")
        tasks = [get_worker_health(addr) for addr in worker_addresses]
        await asyncio.gather(*tasks)
        logger.debug(f"Current worker_status_map after periodic check: {worker_status_map}")
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)

class InternalMasterServicer(master_health_interface_pb2_grpc.InternalMasterServiceServicer):
    async def GetHealthyWorkers(self, request, context):
        # logger.info("InternalMasterServicer received GetHealthyWorkers request")
        workers_with_load_list = []
        async with worker_status_lock:
            for addr, status_data in worker_status_map.items():
                if status_data.get("healthy", False):
                    load_info = master_health_interface_pb2.WorkerLoadInfo(
                        address=addr,
                        cpu_utilization=status_data.get("cpu_utilization", 100.0), # Default to high if missing
                        active_tasks=status_data.get("active_tasks", float('inf')) # Default to high if missing
                    )
                    workers_with_load_list.append(load_info)
        # logger.info(f"Returning healthy workers with load info: {workers_with_load_list}")
        return master_health_interface_pb2.GetHealthyWorkersResponse(workers_with_load=workers_with_load_list)

async def serve(port, worker_addresses_str):
    server = grpc.aio.server()
    master_health_interface_pb2_grpc.add_InternalMasterServiceServicer_to_server(InternalMasterServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    logger.info(f"Master Health Monitor server starting on port {port}...")
    logger.info(f"Master Health Monitor server started. Listening on port {port}.")
    
    worker_addrs_list = [addr.strip() for addr in worker_addresses_str.split(",") if addr.strip()]
    if not worker_addrs_list:
        logger.warning("No worker addresses provided to health monitor. Health checks will not run effectively.")
    else:
        logger.info(f"Health monitor configured to check workers: {worker_addrs_list}")

    health_check_task = asyncio.create_task(update_all_worker_statuses_periodically(worker_addrs_list))

    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Master Health Monitor server shutting down...")
    finally:
        health_check_task.cancel()
        try:
            await health_check_task
        except asyncio.CancelledError:
            logger.info("Health check task cancelled.")
        await close_all_worker_channels() # Ensure all persistent channels are closed
        await server.stop(0)
        logger.info("Master Health Monitor server stopped.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Master Health Monitor Server")
    parser.add_argument("--port", type=int, default=50071, help="Port for the health monitor server to listen on")
    parser.add_argument("--workers", type=str, required=True, help="Comma-separated list of worker addresses to monitor (e.g., localhost:50061,localhost:50062)")
    args = parser.parse_args()

    try:
        asyncio.run(serve(args.port, args.workers))
    except KeyboardInterrupt:
        logger.info("Master Health Monitor process terminated by user.")
    except Exception as e:
        logger.critical(f"Master Health Monitor failed to run: {e}", exc_info=True)

