import os
import uuid
import redis
import aiofiles
import requests
import time
import asyncio
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse # Import JSONResponse
import logging
import logging_loki
import base64 # Import base64

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
STORAGE_PATH = os.environ.get("STORAGE_PATH", "/data")
DIRECTORY_SERVICE_URL = "http://directory-service:8000"
storage_node_id = os.environ.get("HOSTNAME","storage-node")+":8080"
MY_URL = f"http://{storage_node_id}" # How this node is reachable from other services

volume_locks = {}
redis_client = None

# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"status": "Storage Node is running"}

@app.get("/photos/{logical_volume_id}/{photo_id}") # Modified endpoint signature
async def get_photo(logical_volume_id: str, photo_id: str): # Modified function signature
    """Retrieves a photo from a volume by its ID."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Storage metadata service (Redis) unavailable")

    metadata = redis_client.hgetall(f"photo_metadata:{photo_id}")
    if not metadata:
        logger.warning(f"Metadata for photo {photo_id} not found.")
        raise HTTPException(status_code=404, detail="Photo not found")

    try:
        # Check if photo is marked as deleted
        if metadata.get('status') == 'deleted':
            logger.warning(f"Attempted to access a deleted photo: {photo_id}")
            raise HTTPException(status_code=404, detail="Photo has been deleted")

        # Validate that the requested logical_volume_id matches the one in metadata
        stored_volume_id = metadata.get('volume_id')
        if stored_volume_id != logical_volume_id:
            logger.warning(f"Requested logical_volume_id {logical_volume_id} does not match stored volume_id {stored_volume_id} for photo {photo_id}.")
            raise HTTPException(status_code=404, detail="Photo not found in specified logical volume")
        
        offset = int(metadata['offset'])
        size = int(metadata['size'])
        
        volume_path = os.path.join(STORAGE_PATH, f"{logical_volume_id}.dat") # Use logical_volume_id for file path

        if not os.path.exists(volume_path):
            logger.error(f"Volume file {volume_path} not found for photo {photo_id}.")
            raise HTTPException(status_code=500, detail="Volume file not found on storage node")

        async with aiofiles.open(volume_path, "rb") as f:
            await f.seek(offset)
            photo_data = await f.read(size)
        
        # Encode binary data to Base64
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
        
        # Create the file
        async with aiofiles.open(volume_path, "wb") as f:
            await f.write(b"") 
        
        logger.info(f"Created new volume: {new_volume_id} at {volume_path}")
        return {"volume_id": new_volume_id}
    except Exception as e:
        logger.error(f"Failed to create new volume: {e}")
        raise HTTPException(status_code=500, detail="Failed to create new volume file.")

@app.post("/upload/{volume_id}/{photo_id}")
async def upload_photo(volume_id: str, photo_id: str, file: UploadFile = File(...)):
    """Appends photo to a volume file; records metadata in Redis. Idempotent."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Storage metadata service (Redis) unavailable")

    # Idempotency check
    if redis_client.exists(f"photo_metadata:{photo_id}"):
        logger.warning(f"Photo {photo_id} already exists. Skipping upload.")
        # Return existing metadata to ensure consistency
        metadata = redis_client.hgetall(f"photo_metadata:{photo_id}")
        return {"status": "already exists", "photo_id": photo_id, **metadata}

    volume_path = os.path.join(STORAGE_PATH, f"{volume_id}.dat")
    os.makedirs(STORAGE_PATH, exist_ok=True)
    
    if volume_id not in volume_locks:
        volume_locks[volume_id] = asyncio.Lock()
    lock = volume_locks[volume_id]

    async with lock:
        # Second check inside the lock to prevent race conditions
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
                "volume_id": volume_id,
                "offset": offset,
                "size": size,
                "status": "active" # New status field
            }
            redis_client.hset(f"photo_metadata:{photo_id}", mapping=metadata_to_save)

            logger.info(f"Saved photo {photo_id} to {volume_path} at offset {offset}, size {size}")
            return {"status": "saved", "photo_id": photo_id, **metadata_to_save}
        except Exception as e:
            logger.error(f"Error saving photo {photo_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to save photo: {e}")



@app.delete("/photos/{photo_id}")
async def delete_photo(photo_id: str):
    """Marks a photo as 'deleted' in its metadata."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Storage metadata service (Redis) unavailable")

    metadata_key = f"photo_metadata:{photo_id}"
    
    # Check if the photo exists
    if not redis_client.exists(metadata_key):
        logger.warning(f"Delete request for non-existent photo {photo_id}")
        raise HTTPException(status_code=404, detail="Photo not found")
        
    try:
        # Mark the photo as deleted
        redis_client.hset(metadata_key, "status", "deleted")
        
        # We can also retrieve the volume_id to potentially trigger
        # a check for compaction later
        volume_id = redis_client.hget(metadata_key, "volume_id")

        logger.info(f"Marked photo {photo_id} in volume {volume_id} as deleted.")
        return {"status": "marked for deletion", "photo_id": photo_id}
    except Exception as e:
        logger.error(f"Error marking photo {photo_id} as deleted: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to mark photo for deletion: {e}")


@app.post("/compact/{volume_id}")
async def compact_volume(volume_id: str):
    """
    Performs compaction on a volume: copies active needles to a new file 
    and atomically swaps it with the old one.
    """
    if not redis_client:
        raise HTTPException(status_code=503, detail="Storage metadata service (Redis) unavailable")

    volume_path = os.path.join(STORAGE_PATH, f"{volume_id}.dat")
    if not os.path.exists(volume_path):
        raise HTTPException(status_code=404, detail=f"Volume {volume_id} not found.")

    # Define paths for new and temp files
    new_volume_path = os.path.join(STORAGE_PATH, f"{volume_id}.new")
    
    # Get a lock for the volume to prevent writes during compaction
    if volume_id not in volume_locks:
        volume_locks[volume_id] = asyncio.Lock()
    lock = volume_locks[volume_id]

    async with lock:
        logger.info(f"Starting compaction for volume {volume_id}...")
        try:
            new_offset = 0
            # Open the new file to write to and old file to read from
            async with aiofiles.open(new_volume_path, "wb") as new_f:
                # Scan all photo metadata keys in Redis
                for key in redis_client.scan_iter("photo_metadata:*"):
                    photo_id = key.split(":")[-1]
                    metadata = redis_client.hgetall(key)
                    
                    # Check if the photo belongs to the volume being compacted and is not deleted
                    if metadata.get('volume_id') == volume_id and metadata.get('status') != 'deleted':
                        try:
                            original_offset = int(metadata['offset'])
                            original_size = int(metadata['size'])
                            
                            # Read the needle from the old volume
                            async with aiofiles.open(volume_path, "rb") as old_f:
                                await old_f.seek(original_offset)
                                needle_data = await old_f.read(original_size)
                            
                            # Write the needle to the new volume
                            await new_f.write(needle_data)
                            
                            # Update metadata in Redis with the new offset
                            # Use a pipeline for atomic update
                            pipeline = redis_client.pipeline()
                            pipeline.hset(key, "offset", new_offset)
                            # You might also want to update the volume_id if you change it, but here we don't.
                            pipeline.execute()

                            logger.debug(f"Copied {photo_id} to new volume. New offset: {new_offset}")
                            
                            new_offset += original_size
                        except (KeyError, ValueError, TypeError) as e:
                            logger.error(f"Skipping photo {photo_id} due to corrupt metadata: {e}")
                            continue

            # --- Atomic Swap ---
            # This is the critical step. os.rename is atomic on POSIX systems.
            os.rename(new_volume_path, volume_path)
            
            logger.info(f"Compaction successful for volume {volume_id}. New size: {new_offset} bytes.")
            return {"status": "compaction complete", "volume_id": volume_id, "new_size": new_offset}

        except Exception as e:
            logger.error(f"Compaction failed for volume {volume_id}: {e}", exc_info=True)
            # Cleanup the .new file if it exists
            if os.path.exists(new_volume_path):
                os.remove(new_volume_path)
            raise HTTPException(status_code=500, detail=f"Compaction failed: {e}")


# --- Startup ---
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
            response = requests.post(f"{DIRECTORY_SERVICE_URL}/register_storage_node", json={"url": storage_node_id})
            response.raise_for_status()
            logger.info("Successfully registered with Directory Service.")
            break
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to register with Directory Service: {e}. Retrying in 5s...")
            time.sleep(5)

@app.on_event("startup")
def startup_event():
    """On startup, connect to dependencies and register service."""
    connect_to_redis()
    
    # Also register with the directory service
    register_with_directory()

    # Old service discovery via Redis (can be a fallback or removed)
    redis_client.set(f"service:storage-node", MY_URL)

    os.makedirs(STORAGE_PATH, exist_ok=True)
    logger.info(f"Storage path '{STORAGE_PATH}' ensured.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
