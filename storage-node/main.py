import os
import uuid
import redis
import aiofiles
import requests
import time
import asyncio
import threading
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
import logging
import logging_loki
import base64
import json
import pika

# --- Setup ---
handler = logging_loki.LokiHandler(
    url="http://loki:3100/loki/api/v1/push",
    tags={"application": "storage-node"},
    version="1",
)
logger = logging.getLogger("storage-node-logger")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

app = FastAPI()

# --- Globals & Config ---
REDIS_HOST = "redis"
REDIS_PORT = 6379
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
STORAGE_PATH = os.environ.get("STORAGE_PATH", "/data")
DIRECTORY_SERVICE_URL = "http://directory-service:8000"
storage_node_id = os.environ.get("HOSTNAME", "storage-node") + ":8080"
MY_URL = f"http://{storage_node_id}"
INDEX_FILE = os.path.join(STORAGE_PATH, "index.jsonl")
COMPACTION_FRAGMENTATION_THRESHOLD = 0.2  # 20%

# RabbitMQ Exchanges
COMPACTION_TRIGGER_EXCHANGE = 'compaction_trigger_exchange'
COMPACTION_START_EXCHANGE = 'compaction_start_exchange'
COMPACTION_COMPLETE_EXCHANGE = 'compaction_complete_exchange'

volume_locks = {}
redis_client = None
main_event_loop = None

import random

def pick_instance(service_name: str):
    nodes = redis_client.smembers(f"service:{service_name}")
    if not nodes:
        raise Exception(f"No live instances of {service_name}")
    return random.choice(list(nodes))   # or apply RR, weights, etc


def get_service_url(service_name: str) -> str:
    """Retrieves the URL of a service from Redis service discovery."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Service discovery (Redis) unavailable")
    
    service_address = pick_instance(service_name)
    if not service_address:
        raise HTTPException(status_code=503, detail=f"Service '{service_name}' not found")
    
    return f"http://{service_address}"


async def append_to_index(photo_id: str, metadata: dict):
    """Appends a metadata entry to the index file."""
    try:
        record = {"photo_id": photo_id, **metadata}
        async with aiofiles.open(INDEX_FILE, "a") as f:
            await f.write(json.dumps(record) + "\n")
    except Exception as e:
        logger.error(f"Failed to append to index file for photo {photo_id}: {e}")

# --- RabbitMQ Functions ---

def get_rabbitmq_connection():
    """Creates and returns a new RabbitMQ connection."""
    return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))

def publish_event(exchange, routing_key, message):
    """Publishes an event to a RabbitMQ exchange."""
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, exchange_type='direct', durable=True)
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
        )
        connection.close()
        logger.info(f"Published event to exchange '{exchange}' with routing key '{routing_key}': {message}")
    except pika.exceptions.AMQPError as e:
        logger.error(f"Failed to publish RabbitMQ event to exchange '{exchange}': {e}")

def on_compaction_started(ch, method, properties, body):
    """Callback for when a COMPACTION_STARTED event is received."""
    try:
        data = json.loads(body)
        volume_id = data.get('volume_id')
        round_id = data.get('round_id')
        if not all([volume_id, round_id]):
            logger.error(f"Invalid COMPACTION_STARTED message: {data}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        logger.info(f"Received COMPACTION_STARTED for volume {volume_id}, round {round_id}")
        volume_path = os.path.join(STORAGE_PATH, f"{volume_id}.dat")
        if os.path.exists(volume_path):
            logger.info(f"Volume {volume_id} exists locally. Scheduling compaction.")
            asyncio.run_coroutine_threadsafe(run_compaction(volume_id, round_id), main_event_loop)
        else:
            logger.debug(f"Volume {volume_id} not on this node. Ignoring compaction request.")

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing COMPACTION_STARTED message: {e}", exc_info=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def rabbitmq_consumer_thread():
    """The main thread for the RabbitMQ consumer."""
    while True:
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            channel.exchange_declare(exchange=COMPACTION_START_EXCHANGE, exchange_type='fanout', durable=True)
            
            # Declare an exclusive queue to receive broadcast messages
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            channel.queue_bind(exchange=COMPACTION_START_EXCHANGE, queue=queue_name)

            channel.basic_consume(queue=queue_name, on_message_callback=on_compaction_started)
            logger.info("RabbitMQ consumer is waiting for compaction messages.")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"RabbitMQ connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"RabbitMQ consumer error: {e}. Restarting...", exc_info=True)
            if 'channel' in locals() and channel.is_open:
                channel.close()
            if 'connection' in locals() and connection.is_open:
                connection.close()
            time.sleep(5)

# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"status": "Storage Node is running"}

@app.get("/photos/{logical_volume_id}/{photo_id}")
async def get_photo(logical_volume_id: str, photo_id: str):
    """Retrieves a photo from a volume by its ID."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Storage metadata service (Redis) unavailable")

    metadata = redis_client.hgetall(f"photo_metadata:{photo_id}")
    if not metadata:
        logger.warning(f"Metadata for photo {photo_id} not found.")
        raise HTTPException(status_code=404, detail="Photo not found")

    try:
        if metadata.get('status') == 'deleted':
            logger.warning(f"Attempted to access a deleted photo: {photo_id}")
            raise HTTPException(status_code=404, detail="Photo has been deleted")

        stored_volume_id = metadata.get('volume_id')
        if stored_volume_id != logical_volume_id:
            logger.warning(f"Requested logical_volume_id {logical_volume_id} does not match stored volume_id {stored_volume_id} for photo {photo_id}.")
            raise HTTPException(status_code=404, detail="Photo not found in specified logical volume")
        
        offset = int(metadata['offset'])
        size = int(metadata['size'])
        
        volume_path = os.path.join(STORAGE_PATH, f"{logical_volume_id}.dat")
        if not os.path.exists(volume_path):
            logger.error(f"Volume file {volume_path} not found for photo {photo_id}.")
            raise HTTPException(status_code=500, detail="Volume file not found on storage node")

        async with aiofiles.open(volume_path, "rb") as f:
            await f.seek(offset)
            photo_data = await f.read(size)
        
        encoded_photo_data = base64.b64encode(photo_data).decode('utf-8')
        return JSONResponse(content={"photo_id": photo_id, "data": encoded_photo_data})
    
    except KeyError:
        logger.error(f"Incomplete metadata for photo {photo_id}: {metadata}")
        raise HTTPException(status_code=500, detail="Incomplete metadata for photo.")
    except Exception as e:
        logger.error(f"Error retrieving photo {photo_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve photo: {e}")

@app.post("/create_volume")
async def create_volume():
    """Creates a new, empty volume file and returns its ID."""
    try:
        new_volume_id = str(uuid.uuid4())
        volume_path = os.path.join(STORAGE_PATH, f"{new_volume_id}.dat")
        async with aiofiles.open(volume_path, "wb") as f:
            await f.write(b"") 
        
        logger.info(f"Created new volume: {new_volume_id} at {volume_path}")
        return {"volume_id": new_volume_id}
    except Exception as e:
        logger.error(f"Failed to create new volume: {e}")
        raise HTTPException(status_code=500, detail="Failed to create new volume file.")

@app.post("/upload/{volume_id}/{photo_id}")
async def upload_photo(volume_id: str, photo_id: str, file: UploadFile = File(...)):
    """Appends photo to a volume file, records metadata, and updates volume stats."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Storage metadata service (Redis) unavailable")

    if redis_client.exists(f"photo_metadata:{photo_id}"):
        logger.warning(f"Photo {photo_id} already exists. Skipping upload.")
        metadata = redis_client.hgetall(f"photo_metadata:{photo_id}")
        return {"status": "already exists", "photo_id": photo_id, **metadata}

    volume_path = os.path.join(STORAGE_PATH, f"{volume_id}.dat")
    os.makedirs(STORAGE_PATH, exist_ok=True)
    
    if volume_id not in volume_locks:
        volume_locks[volume_id] = asyncio.Lock()
    lock = volume_locks[volume_id]

    async with lock:
        if redis_client.exists(f"photo_metadata:{photo_id}"):
            logger.warning(f"Photo {photo_id} already exists (race condition averted). Skipping upload.")
            metadata = redis_client.hgetall(f"photo_metadata:{photo_id}")
            return {"status": "already exists", "photo_id": photo_id, **metadata}

        try:
            content = await file.read()
            size = len(content)

            async with aiofiles.open(volume_path, "ab") as f:
                offset = await f.tell()
                await f.write(content)

            metadata_to_save = {
                "volume_id": volume_id, "offset": offset, "size": size, "status": "active"
            }
            # Use a pipeline to ensure atomicity
            pipeline = redis_client.pipeline()
            pipeline.hset(f"photo_metadata:{photo_id}", mapping=metadata_to_save)
            pipeline.hincrby(f"volume_stats:{volume_id}", "total_photos", 1)
            pipeline.execute()
            
            await append_to_index(photo_id, metadata_to_save)
            logger.info(f"Saved photo {photo_id} to {volume_path} at offset {offset}, size {size}")
            return {"status": "saved", "photo_id": photo_id, **metadata_to_save}
        except Exception as e:
            logger.error(f"Error saving photo {photo_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to save photo: {e}")

@app.delete("/photos/{photo_id}")
async def delete_photo(photo_id: str):
    """Marks a photo as 'deleted' and triggers compaction check."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Storage metadata service (Redis) unavailable")

    metadata_key = f"photo_metadata:{photo_id}"
    if not redis_client.exists(metadata_key):
        logger.warning(f"Delete request for non-existent photo {photo_id}")
        raise HTTPException(status_code=404, detail="Photo not found")
        
    try:
        volume_id = redis_client.hget(metadata_key, "volume_id")
        
        # Use a pipeline for atomicity
        pipeline = redis_client.pipeline()
        pipeline.hset(metadata_key, "status", "deleted")
        pipeline.hincrby(f"volume_stats:{volume_id}", "deleted_photos", 1)
        results = pipeline.execute()

        await append_to_index(photo_id, {"status": "deleted"})
        logger.info(f"Marked photo {photo_id} in volume {volume_id} as deleted.")

        # --- Compaction Trigger Logic ---
        stats = redis_client.hgetall(f"volume_stats:{volume_id}")
        total_photos = int(stats.get("total_photos", 0))
        deleted_photos = int(stats.get("deleted_photos", 0))

        if total_photos > 0 and (deleted_photos / total_photos) > COMPACTION_FRAGMENTATION_THRESHOLD:
            logger.info(f"Volume {volume_id} fragmentation ({deleted_photos / total_photos:.2%}) exceeds threshold. Triggering compaction.")
            publish_event(
                exchange=COMPACTION_TRIGGER_EXCHANGE,
                routing_key='compaction.trigger',
                message={"volume_id": volume_id}
            )

        return {"status": "marked for deletion", "photo_id": photo_id}
    except Exception as e:
        logger.error(f"Error marking photo {photo_id} as deleted: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to mark photo for deletion: {e}")

# --- Compaction Logic ---

async def run_compaction(volume_id: str, round_id: str):
    """
    Performs compaction on a volume, copies active needles to a new file,
    and notifies the directory service via RabbitMQ. Runs as a background task.
    """
    if not redis_client:
        logger.error(f"Compaction failed for {volume_id}: Redis unavailable.")
        return

    volume_path = os.path.join(STORAGE_PATH, f"{volume_id}.dat")
    new_volume_path = os.path.join(STORAGE_PATH, f"{volume_id}.new")

    if volume_id not in volume_locks:
        volume_locks[volume_id] = asyncio.Lock()
    lock = volume_locks[volume_id]

    async with lock:
        logger.info(f"Starting compaction for volume {volume_id}, round {round_id}...")
        try:
            new_offset = 0
            active_photos_in_volume = 0
            
            async with aiofiles.open(new_volume_path, "wb") as new_f:
                for key in redis_client.scan_iter("photo_metadata:*"):
                    metadata = redis_client.hgetall(key)
                    if metadata.get('volume_id') == volume_id and metadata.get('status') == 'active':
                        photo_id = key.split(":")[-1]
                        try:
                            original_offset = int(metadata['offset'])
                            original_size = int(metadata['size'])
                            
                            async with aiofiles.open(volume_path, "rb") as old_f:
                                await old_f.seek(original_offset)
                                needle_data = await old_f.read(original_size)
                            
                            await new_f.write(needle_data)
                            
                            # Update metadata with the new offset
                            pipeline = redis_client.pipeline()
                            pipeline.hset(key, "offset", new_offset)
                            pipeline.execute()
                            await append_to_index(photo_id, {"offset": new_offset})
                            
                            logger.debug(f"Copied {photo_id} to new volume. New offset: {new_offset}")
                            new_offset += original_size
                            active_photos_in_volume += 1
                        except (KeyError, ValueError, TypeError) as e:
                            logger.error(f"Skipping photo {photo_id} during compaction due to corrupt metadata: {e}")

            os.rename(new_volume_path, volume_path)
            
            # Reset volume stats in Redis
            pipeline = redis_client.pipeline()
            pipeline.hset(f"volume_stats:{volume_id}", "total_photos", active_photos_in_volume)
            pipeline.hset(f"volume_stats:{volume_id}", "deleted_photos", 0)
            pipeline.execute()
            
            logger.info(f"Compaction successful for volume {volume_id}. New size: {new_offset} bytes.")

            # Publish completion event
            publish_event(
                exchange=COMPACTION_COMPLETE_EXCHANGE,
                routing_key='compaction.completed',
                message={
                    "volume_id": volume_id,
                    "round_id": round_id,
                    "storage_node_id": storage_node_id,
                    "new_size": new_offset
                }
            )
        except Exception as e:
            logger.error(f"Compaction failed for volume {volume_id}: {e}", exc_info=True)
            if os.path.exists(new_volume_path):
                os.remove(new_volume_path)

# --- Startup & System Functions ---

def connect_to_redis():
    """Connects to Redis, with retry logic."""
    global redis_client
    while True:
        try:
            redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            redis_client.ping()
            logger.info("Successfully connected to Redis.")
            break
        except redis.exceptions.ConnectionError as e:
            logger.warning(f"Could not connect to Redis: {e}. Retrying in 5s...")
            time.sleep(5)

def register_with_directory():
    """Registers this node with the directory service, with retry logic."""
    while True:
        try:
            DIRECTORY_SERVICE_URL = get_service_url("directory-service")
            response = requests.post(f"{DIRECTORY_SERVICE_URL}/register_storage_node", json={"url": storage_node_id})
            response.raise_for_status()
            logger.info("Successfully registered with Directory Service.")
            break
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to register with Directory Service: {e}. Retrying in 5s...")
            time.sleep(5)

def recover_metadata_from_index():
    """Reads the index file and repopulates Redis with metadata and volume stats."""
    if not os.path.exists(INDEX_FILE):
        logger.info("Index file not found. Skipping recovery.")
        return

    logger.info("Starting metadata recovery from index file...")
    volume_stats = {}
    try:
        with open(INDEX_FILE, "r") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    photo_id = record.pop("photo_id")
                    
                    metadata_key = f"photo_metadata:{photo_id}"
                    redis_client.hset(metadata_key, mapping=record)
                    
                    # Re-calculate volume stats during recovery
                    if 'volume_id' in record and record.get('status') != 'deleted':
                        vol_id = record['volume_id']
                        stats = volume_stats.setdefault(vol_id, {'total': 0, 'deleted': 0})
                        stats['total'] += 1
                    if record.get('status') == 'deleted':
                        vol_id = redis_client.hget(metadata_key, "volume_id") # Get vol_id for deleted photo
                        if vol_id:
                           stats = volume_stats.setdefault(vol_id, {'total': 0, 'deleted': 0})
                           stats['deleted'] += 1
                    
                    logger.debug(f"Recovered metadata for photo {photo_id}")
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning(f"Skipping corrupt line/record in index file: {line.strip()} ({e})")
        
        # Save calculated stats to Redis
        for vol_id, stats in volume_stats.items():
            redis_client.hset(f"volume_stats:{vol_id}", "total_photos", stats['total'])
            redis_client.hset(f"volume_stats:{vol_id}", "deleted_photos", stats['deleted'])
        logger.info("Metadata and volume stats recovery complete.")
    except Exception as e:
        logger.error(f"Failed to read or process index file: {e}")

def heartbeat_to_directory():
    """Periodically sends a heartbeat to the directory service."""
    HEARTBEAT_INTERVAL_SECONDS = 30 
    while True:
        try:
            DIRECTORY_SERVICE_URL = get_service_url("directory-service")
            response = requests.post(f"{DIRECTORY_SERVICE_URL}/heartbeat", json={"url": storage_node_id})
            response.raise_for_status()
            logger.info("Successfully sent heartbeat to Directory Service.")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to send heartbeat to Directory Service: {e}.")
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)

@app.on_event("startup")
def startup_event():
    """On startup, connect to dependencies, start background threads, and register."""
    global main_event_loop
    main_event_loop = asyncio.get_running_loop()

    connect_to_redis()
    recover_metadata_from_index()
    register_with_directory()
    
    # Start background threads
    threading.Thread(target=heartbeat_to_directory, daemon=True).start()
    threading.Thread(target=rabbitmq_consumer_thread, daemon=True).start()
    logger.info("Background threads (Heartbeat, RabbitMQ Consumer) started.")

    os.makedirs(STORAGE_PATH, exist_ok=True)
    logger.info(f"Storage path '{STORAGE_PATH}' ensured.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
