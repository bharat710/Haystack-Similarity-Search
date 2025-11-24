import pika
import threading
import time
import redis
import requests # Keeping requests for existing upload_photo endpoint, but using httpx for new read API
import httpx
import asyncio
import logging
import logging_loki
from fastapi import FastAPI, HTTPException, UploadFile, File, Header
from fastapi.responses import JSONResponse # Import JSONResponse
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware


# --- Setup ---
handler = logging_loki.LokiHandler(
    url="http://loki:3100/loki/api/v1/push",
    tags={"application": "api-gateway"},
    version="1",
)
logger = logging.getLogger("api-gateway-logger")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # or ["*"] in dev
    allow_credentials=False,
    allow_methods=["*"],            # important: lets OPTIONS, POST, GET, etc. through
    allow_headers=["*"],            # allows Idempotency-Key, Content-Type, etc.
)


# --- Globals & Config ---
REDIS_HOST = "redis"
REDIS_PORT = 6379
RABBITMQ_HOST = "rabbitmq"
PHOTO_QUEUE = "photo_processing_queue" # This queue name must match ai-worker's config
HTTP_CLIENT_TIMEOUT = 30
IDEMPOTENCY_KEY_EXPIRY_SECONDS = 24 * 60 * 60 # 24 hours

redis_client = None
rabbitmq_channel = None
rabbitmq_connection = None

# --- Service Discovery & Dependencies ---
def connect_to_rabbitmq():
    """Connects to RabbitMQ with retry logic."""
    global rabbitmq_channel, rabbitmq_connection
    while True:
        try:
            logger.info("Connecting to RabbitMQ...")
            rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            rabbitmq_channel = rabbitmq_connection.channel()
            rabbitmq_channel.queue_declare(queue=PHOTO_QUEUE, durable=True)
            logger.info("Successfully connected to RabbitMQ and declared queue.")
            break
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"RabbitMQ connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"An unexpected error occurred during RabbitMQ connection: {e}. Retrying in 5 seconds...")
            time.sleep(5)

@app.on_event("startup")
def startup_event():
    """Connect to Redis and RabbitMQ on startup."""
    global redis_client
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        redis_client.ping()
        logger.info("Successfully connected to Redis.")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Could not connect to Redis: {e}")
        # The application can run, but idempotency and service discovery will fail.
        redis_client = None
    
    connect_to_rabbitmq()
    # Start RabbitMQ connection in a separate thread to avoid blocking startup
    # rabbitmq_thread = threading.Thread(target=connect_to_rabbitmq, daemon=True)
    # rabbitmq_thread.start()

def get_service_url(service_name: str) -> str:
    """Retrieves the URL of a service from Redis service discovery."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Service discovery (Redis) unavailable")
    
    service_address = redis_client.get(f"service:{service_name}")
    if not service_address:
        raise HTTPException(status_code=503, detail=f"Service '{service_name}' not found")
    
    return f"http://{service_address}"

# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"status": "API Gateway is running"}

async def upload_to_node(client: httpx.AsyncClient, node_url: str, volume_id: str, photo_id: str, content: bytes, filename: str):
    """Uploads photo content to a single storage node."""
    upload_url = f"http://{node_url}/upload/{volume_id}/{photo_id}"
    files = {'file': (filename, content)}
    try:
        response = await client.post(upload_url, files=files)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error uploading to {node_url}: {e.response.status_code} - {e.response.text}")
        raise HTTPException(status_code=502, detail=f"Failed to upload to storage node {node_url}")
    except httpx.RequestError as e:
        logger.error(f"Request error uploading to {node_url}: {e}")
        raise HTTPException(status_code=502, detail=f"Error connecting to storage node {node_url}")

@app.post("/upload_photo/")
async def upload_photo(
    idempotency_key: Optional[str] = Header(None),
    file: UploadFile = File(...)
):
    """
    Handles photo upload using a two-phase commit and idempotency.
    1. Requires an `Idempotency-Key` header for safe retries.
    2. Phase 1: Requests an upload location from the Directory Service.
    3. Uploads the file to all storage replicas.
    4. Phase 2: Commits the upload metadata to the Directory Service.
    """
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required.")
    if not redis_client:
        raise HTTPException(status_code=503, detail="Backend service (Redis) unavailable for idempotency check.")

    # Idempotency Check
    idempotency_redis_key = f"idempotency:{idempotency_key}"
    stored_result = redis_client.get(idempotency_redis_key)
    if stored_result:
        logger.info(f"Idempotency key {idempotency_key} already processed. Returning stored result.")
        return {"status": "success (from retry)", "photo_id": stored_result}

    try:
        # Read file content and get size
        content = await file.read()
        photo_size = len(content)

        # --- Phase 1: Request Upload Location ---
        directory_service_url = get_service_url("directory-service")
        try:
            dir_response = requests.post(
                f"{directory_service_url}/request_upload_location",
                json={"photo_size_bytes": photo_size}
            )
            dir_response.raise_for_status()
            upload_info = dir_response.json()
            photo_id = upload_info["photo_id"]
            volume_id = upload_info["volume_id"]
            storage_node_urls = upload_info["storage_node_urls"]
        except requests.exceptions.RequestException as e:
            logger.error(f"Could not get upload location from Directory Service: {e}")
            raise HTTPException(status_code=502, detail="Directory Service unavailable")

        if not storage_node_urls:
            raise HTTPException(status_code=503, detail="No storage nodes available for upload")

        # --- Upload to all replicas ---
        async with httpx.AsyncClient(timeout=HTTP_CLIENT_TIMEOUT) as client:
            upload_tasks = [
                upload_to_node(client, node_url, volume_id, photo_id, content, file.filename)
                for node_url in storage_node_urls
            ]
            results = await asyncio.gather(*upload_tasks, return_exceptions=True)

        failed_uploads = [res for res in results if isinstance(res, Exception)]
        if failed_uploads:
            logger.error(f"Failed to upload photo {photo_id} to some nodes: {failed_uploads}")
            raise HTTPException(status_code=500, detail="Failed to save photo to all required replicas.")
        
        logger.info(f"Successfully uploaded photo {photo_id} to all {len(storage_node_urls)} replicas.")

        # --- Phase 2: Commit Upload ---
        try:
            commit_response = requests.post(
                f"{directory_service_url}/commit_upload",
                json={"photo_id": photo_id, "volume_id": volume_id, "size_bytes": photo_size}
            )
            commit_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to commit upload for photo {photo_id}: {e}")
            # In a real system, you'd have a cleanup/retry mechanism for this failed commit.
            raise HTTPException(status_code=500, detail="Failed to finalize upload. Please try again.")

        # If everything is successful, store the result for idempotency
        redis_client.set(idempotency_redis_key, photo_id, ex=IDEMPOTENCY_KEY_EXPIRY_SECONDS)

        # Publish message to RabbitMQ for AI processing
        if rabbitmq_channel:
            try:
                rabbitmq_channel.basic_publish(
                    exchange='',
                    routing_key=PHOTO_QUEUE,
                    body=f"{volume_id}/{photo_id}",
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    )
                )
                logger.info(f"Published photo_id {photo_id} to RabbitMQ queue '{PHOTO_QUEUE}'")
            except Exception as e:
                logger.error(f"Failed to publish photo_id {photo_id} to RabbitMQ: {e}")
        else:
            logger.error(f"RabbitMQ channel not available to publish photo_id {photo_id}")
        
        logger.info(f"Successfully committed photo {photo_id}.")
        return {"status": "upload successful", "photo_id": photo_id}

    except HTTPException as e:
        raise e # Re-raise known HTTP exceptions
    except Exception as e:
        logger.error(f"An unexpected error occurred during photo upload: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected internal error occurred.")


@app.get("/photos/{photo_id}")
async def get_photo(photo_id: str):
    """
    Retrieves a photo by its ID from the Haystack system.
    Orchestrates calls to Directory Service and Cache Service.
    """
    directory_service_url = get_service_url("directory-service")
    cache_service_url = get_service_url("cache-service")

    async with httpx.AsyncClient(timeout=HTTP_CLIENT_TIMEOUT) as client:
        try:
            # 1. Query Directory Service for photo location
            directory_response = await client.get(f"{directory_service_url}/lookup/{photo_id}")
            directory_response.raise_for_status()
            location_info = directory_response.json()
            
            logical_volume_id = location_info.get("logical_volume_id")
            storage_nodes = location_info.get("storage_nodes")

            if not logical_volume_id or not storage_nodes:
                logger.error(f"Directory service returned incomplete location info for {photo_id}: {location_info}")
                raise HTTPException(status_code=500, detail="Incomplete location information from directory service")

            # 2. Query Cache Service to get the photo
            cache_request_body = {
                "photo_id": photo_id,
                "logical_volume_id": logical_volume_id,
                "storage_nodes": storage_nodes
            }
            cache_response = await client.post(f"{cache_service_url}/cache/lookup", json=cache_request_body)
            cache_response.raise_for_status()

            photo_data = cache_response.json() # Expecting JSON with photo_id and data (Base64)
            
            logger.info(f"Successfully retrieved photo {photo_id} via cache service.")
            return JSONResponse(content=photo_data)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"Photo {photo_id} not found in directory or cache service. Details: {e.response.text}")
                raise HTTPException(status_code=404, detail="Photo not found")
            else:
                logger.error(f"HTTP error during photo retrieval for {photo_id}: {e.response.status_code} - {e.response.text}")
                raise HTTPException(status_code=502, detail=f"Error communicating with backend service: {e.response.text}")
        except httpx.RequestError as e:
            logger.error(f"Network error during photo retrieval for {photo_id}: {e}")
            raise HTTPException(status_code=503, detail="Backend service unreachable")
        except Exception as e:
            logger.error(f"An unexpected error occurred during photo retrieval for {photo_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="An unexpected internal error occurred.")


@app.delete("/photos/{photo_id}")
async def delete_photo(photo_id: str):
    """
    Deletes a photo by its ID from the Haystack system.
    Orchestrates calls to Directory Service, Cache Service, and Storage Nodes.
    """
    directory_service_url = get_service_url("directory-service")
    cache_service_url = get_service_url("cache-service")

    async with httpx.AsyncClient(timeout=HTTP_CLIENT_TIMEOUT) as client:
        try:
            # 1. Query Directory Service for photo location
            directory_response = await client.get(f"{directory_service_url}/lookup/{photo_id}")
            directory_response.raise_for_status()
            location_info = directory_response.json()
            
            storage_nodes = location_info.get("storage_nodes")

            if not storage_nodes:
                logger.error(f"Directory service returned incomplete location info for {photo_id}: {location_info}")
                raise HTTPException(status_code=500, detail="Incomplete location information from directory service")

            # 2. Delete from Cache Service and Storage Nodes in parallel
            tasks = []
            
            # Task to delete from cache
            tasks.append(client.delete(f"{cache_service_url}/cache/{photo_id}"))
            
            # Tasks to delete from storage nodes
            for node_url in storage_nodes:
                tasks.append(client.delete(f"http://{node_url}/photos/{photo_id}"))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # --- Error Handling ---
            failed_tasks = [res for res in results if isinstance(res, Exception)]
            if failed_tasks:
                # Log errors for each failed request
                for i, res in enumerate(results):
                    if isinstance(res, Exception):
                        task_description = f"Cache deletion for {photo_id}" if i == 0 else f"Storage node deletion for {photo_id} at {storage_nodes[i-1]}"
                        
                        if isinstance(res, httpx.HTTPStatusError):
                            # Handle HTTP errors (e.g., 404, 500)
                            logger.error(f"{task_description} failed with status {res.response.status_code}: {res.response.text}")
                        elif isinstance(res, httpx.RequestError):
                            # Handle network/connection errors
                            logger.error(f"{task_description} failed with request error: {res}")
                        else:
                            # Handle other unexpected exceptions
                            logger.error(f"An unexpected error occurred during {task_description}: {res}", exc_info=True)
                
                # Optionally, attempt to rollback or queue for retry here
                
                # Check if all failed, or just some
                if len(failed_tasks) == len(tasks):
                    raise HTTPException(status_code=502, detail="Failed to delete photo from all services.")
                else:
                    # Partial success - might be okay, but needs monitoring
                    logger.warning(f"Photo {photo_id} was partially deleted. Some services failed.")
                    # Return a 207 Multi-Status or a 200 OK depending on desired behavior
                    return JSONResponse(status_code=207, content={"message": "Photo deletion partially successful. Some services failed."})


            # --- Mark as deleted in Directory Service ---
            # This is an important step to prevent the photo from being "resurrected"
            # if a lookup happens before compaction.
            commit_response = await client.post(f"{directory_service_url}/mark_deleted/{photo_id}")
            commit_response.raise_for_status()


            logger.info(f"Successfully initiated deletion for photo {photo_id}")
            return {"status": "deletion initiated"}

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"Photo {photo_id} not found in directory service for deletion.")
                raise HTTPException(status_code=404, detail="Photo not found")
            else:
                logger.error(f"HTTP error during photo deletion for {photo_id}: {e.response.status_code} - {e.response.text}")
                raise HTTPException(status_code=502, detail=f"Error communicating with backend service: {e.response.text}")
        except httpx.RequestError as e:
            logger.error(f"Network error during photo deletion for {photo_id}: {e}")
            raise HTTPException(status_code=503, detail="Backend service unreachable")
        except Exception as e:
            logger.error(f"An unexpected error occurred during photo deletion for {photo_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="An unexpected internal error occurred.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
