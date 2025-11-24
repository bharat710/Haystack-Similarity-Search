import redis
import logging
import logging_loki
import httpx # Import httpx
from pydantic import BaseModel # Import BaseModel
from cachetools import TTLCache # Import TTLCache
import asyncio # Import asyncio
import json # Import json

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse # Ensure JSONResponse is imported

# --- Models ---
class CacheLookupRequest(BaseModel):
    photo_id: str
    logical_volume_id: str
    storage_nodes: list[str]

handler = logging_loki.LokiHandler(
    url="http://loki:3100/loki/api/v1/push",
    tags={"application": "cache-service"},
    version="1",
)

logger = logging.getLogger("cache-service-logger")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

app = FastAPI()

# --- Globals ---
REDIS_HOST = "redis"
REDIS_PORT = 6379

# In-memory cache using cachetools
# Cache photos for 5 minutes (300 seconds), max 1000 entries
in_memory_cache = TTLCache(maxsize=1000, ttl=300) 

# This redis client is for the cache itself (Image_Blob_Map)
try:
    image_cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1) # Use a different DB for the cache
    image_cache.ping()
    logger.info("Successfully connected to Redis for image caching.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis for image caching: {e}")
    image_cache = None

# This redis client is for service discovery
try:
    discovery_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    discovery_client.ping()
    logger.info("Successfully connected to Redis for service discovery.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis for service discovery: {e}")
    discovery_client = None


# --- API Endpoints ---

@app.get("/")
def read_root():
    """Root endpoint for health check."""
    return {"status": "Cache Service is running"}


@app.post("/cache/lookup")
async def cache_lookup(request: CacheLookupRequest):
    """
    Looks up a photo in the cache. If not found, fetches from storage nodes, caches it, and returns.
    """
    photo_id = request.photo_id
    logical_volume_id = request.logical_volume_id
    storage_nodes = request.storage_nodes

    # 1. Check in-memory cache
    if photo_id in in_memory_cache:
        logger.info(f"IN-MEMORY CACHE HIT for {photo_id}")
        return JSONResponse(content=in_memory_cache[photo_id])

    # 2. Check Redis cache
    if image_cache:
        cached_data = image_cache.get(photo_id)
        if cached_data:
            logger.info(f"REDIS CACHE HIT for {photo_id}")
            # Redis stores bytes, so decode and load as JSON
            photo_json = json.loads(cached_data.decode('utf-8'))
            in_memory_cache[photo_id] = photo_json # Populate in-memory cache
            return JSONResponse(content=photo_json)

    logger.info(f"CACHE MISS for {photo_id}")

    # 3. Fetch from storage nodes if cache miss
    async with httpx.AsyncClient() as client:
        for node_url in storage_nodes:
            storage_node_photo_url = f"http://{node_url}/photos/{logical_volume_id}/{photo_id}"
            try:
                # Use a short timeout for storage node requests to fail fast if a node is down
                response = await client.get(storage_node_photo_url, timeout=3.0) 
                response.raise_for_status() # Raise an exception for 4xx or 5xx responses

                photo_data = response.json() # Expecting JSON with photo_id and data (Base64)
                
                # 4. Cache the response
                if image_cache:
                    # Store as JSON string in Redis
                    image_cache.set(photo_id, json.dumps(photo_data), ex=3600) # Cache for 1 hour
                in_memory_cache[photo_id] = photo_data # Populate in-memory cache

                logger.info(f"Successfully fetched {photo_id} from {node_url} and cached.")
                return JSONResponse(content=photo_data)

            except httpx.RequestError as e:
                logger.warning(f"Error fetching {photo_id} from {node_url}: {e}")
                continue # Try next storage node
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning(f"Photo {photo_id} not found on {node_url}.")
                else:
                    logger.error(f"HTTP error fetching {photo_id} from {node_url}: {e}")
                continue # Try next storage node
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON response from {node_url}: {e}")
                continue # Try next storage node

    # If all storage nodes fail
    logger.error(f"Photo {photo_id} not found on any of the provided storage nodes.")
    raise HTTPException(status_code=404, detail="Photo not found in cache or on storage nodes")


@app.delete("/cache/{photo_id}")
def delete_from_cache(photo_id: str):
    """
    Deletes a photo from both the in-memory and Redis caches.
    This is an idempotent operation.
    """
    in_memory_deleted = False
    redis_deleted_count = 0
    
    # 1. Delete from in-memory cache
    if photo_id in in_memory_cache:
        try:
            del in_memory_cache[photo_id]
            in_memory_deleted = True
            logger.info(f"Deleted {photo_id} from in-memory cache.")
        except KeyError:
            # This could happen in a race condition, it's fine.
            logger.info(f"{photo_id} was already removed from in-memory cache, likely by another process.")
            pass # Already deleted

    # 2. Delete from Redis cache
    if image_cache:
        try:
            # The `delete` method returns the number of keys deleted.
            redis_deleted_count = image_cache.delete(photo_id)
            if redis_deleted_count > 0:
                logger.info(f"Deleted {photo_id} from Redis cache.")
        except redis.exceptions.RedisError as e:
            # Log the error but don't fail the request, as the primary goal is deletion
            # and one of the caches might have succeeded.
            logger.error(f"Error deleting {photo_id} from Redis cache: {e}")
            # Depending on the desired robustness, you might want to raise an HTTPException here
            raise HTTPException(status_code=500, detail=f"Failed to delete key from Redis: {e}")


    # 3. Report result
    if in_memory_deleted or redis_deleted_count > 0:
        return {"status": "success", "message": f"Photo {photo_id} removed from cache."}
    else:
        # If it wasn't in either cache, it's not an error. The state is consistent.
        logger.info(f"Attempted to delete {photo_id}, but it was not found in any cache.")
        raise HTTPException(status_code=404, detail=f"Photo {photo_id} not found in cache.")



# --- Startup ---

@app.on_event("startup")
def startup_event():
    """On startup, register with service discovery."""
    if discovery_client:
        discovery_client.set("service:cache-service", "cache-service:8080")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
