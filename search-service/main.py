import os
import redis
import psycopg2
import faiss
import numpy as np
from fastapi import FastAPI, HTTPException
from psycopg2.extras import DictCursor
import logging
import logging_loki
from pydantic import BaseModel

class AddVectorRequest(BaseModel):
    photo_uuid: str
    vector: list[float]


handler = logging_loki.LokiHandler(
    url="http://loki:3100/loki/api/v1/push",
    tags={"application": "search-service"},
    version="1",
)

logger = logging.getLogger("search-service-logger")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

app = FastAPI()

# --- Globals ---
DATABASE_URL = os.environ.get("DATABASE_URL")
REDIS_HOST = "redis"
REDIS_PORT = 6379

# Placeholder for the FAISS index
# In a real system, you would load this from a file.
INDEX_DIMENSION = 512  # Example dimension
faiss_index = faiss.IndexFlatL2(INDEX_DIMENSION)
INDEX_FILE = "faiss_index.bin"

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    redis_client.ping()
    logger.info("Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis: {e}")
    redis_client = None

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("Successfully connected to the search database.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to the database: {e}")
        return None

# --- API Endpoints ---

@app.get("/")
def read_root():
    """Root endpoint for health check."""
    return {"status": "Search Service is running"}

@app.post("/add_vector/")
def add_vector(payload: AddVectorRequest):
    """Adds a vector to the FAISS index and maps it to a photo UUID."""
    photo_uuid = payload.photo_uuid
    vector = payload.vector
    """Adds a vector to the FAISS index and maps it to a photo UUID."""
    if len(vector) != INDEX_DIMENSION:
        raise HTTPException(status_code=400, detail=f"Vector must have dimension {INDEX_DIMENSION}")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=503, detail="Database connection unavailable")

    try:
        # Add to FAISS index
        vector_np = np.array([vector]).astype('float32')
        faiss_index.add(vector_np)
        internal_id = faiss_index.ntotal - 1

        # Add mapping to database
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO vector_map (internal_id, photo_uuid) VALUES (%s, %s)",
                (internal_id, photo_uuid)
            )
        conn.commit()

        return {"status": "vector added", "internal_id": internal_id, "photo_uuid": photo_uuid}

    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# --- Startup ---

def load_faiss_index():
    """Loads the FAISS index from disk if it exists."""
    global faiss_index
    if os.path.exists(INDEX_FILE):
        logger.info(f"Loading FAISS index from '{INDEX_FILE}'...")
        faiss_index = faiss.read_index(INDEX_FILE)
        logger.info(f"Index loaded successfully. It contains {faiss_index.ntotal} vectors.")
    else:
        logger.info(f"FAISS index file '{INDEX_FILE}' not found. Starting with a new index.")

@app.on_event("startup")
def startup_event():
    """On startup, connect to services and initialize database/index."""
    if redis_client:
        redis_client.set("service:search-service", "search-service:8000")

    load_faiss_index()

    conn = get_db_connection()
    if conn:
        with conn.cursor() as cur:
            # Create the vector mapping table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vector_map (
                    internal_id BIGINT PRIMARY KEY,
                    photo_uuid VARCHAR(255) NOT NULL UNIQUE
                );
            """)
        conn.commit()
        conn.close()
        logger.info("Database schema initialized.")

@app.on_event("shutdown")
def shutdown_event():
    """On shutdown, save the FAISS index to disk."""
    logger.info(f"Saving FAISS index with {faiss_index.ntotal} vectors to '{INDEX_FILE}'...")
    faiss.write_index(faiss_index, INDEX_FILE)
    logger.info("FAISS index saved successfully.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
