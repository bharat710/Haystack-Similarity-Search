import os
import uuid
import redis
import psycopg2
import requests
from fastapi import FastAPI, HTTPException, Depends
from psycopg2.extras import DictCursor
from pydantic import BaseModel
import logging
import logging_loki

# --- Models ---
class UploadRequest(BaseModel):
    photo_size_bytes: int

class CommitRequest(BaseModel):
    photo_id: str
    volume_id: str
    size_bytes: int

class NodeRegistryRequest(BaseModel):
    url: str


# --- Setup ---
handler = logging_loki.LokiHandler(
    url="http://loki:3100/loki/api/v1/push",
    tags={"application": "directory-service"},
    version="1",
)
logger = logging.getLogger("directory-service-logger")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

app = FastAPI()

# --- Globals & Config ---
DATABASE_URL = os.environ.get("DATABASE_URL")
REDIS_HOST = "redis"
REDIS_PORT = 6379
VOLUME_MAX_SIZE_BYTES = 100 * 1024 * 1024  # 100MB

# --- DB Connection ---
def get_db_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False # Ensure we can manage transactions
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Could not connect to the database: {e}")
        raise HTTPException(status_code=503, detail="Database connection unavailable")

# Dependency for FastAPI endpoints
def get_db():
    db = get_db_connection()
    try:
        yield db
    finally:
        db.close()


# --- Database Schema Management ---
def create_db_tables(conn):
    """Creates necessary database tables if they do not exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS storage_nodes (
                    id SERIAL PRIMARY KEY,
                    url VARCHAR(255) UNIQUE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS logical_volumes (
                    id SERIAL PRIMARY KEY,
                    volume_id VARCHAR(255) UNIQUE NOT NULL,
                    current_size_bytes BIGINT DEFAULT 0,
                    status VARCHAR(20) DEFAULT 'writable' NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS volume_replicas (
                    id SERIAL PRIMARY KEY,
                    volume_id VARCHAR(255) REFERENCES logical_volumes(volume_id),
                    storage_node_id INT REFERENCES storage_nodes(id),
                    UNIQUE (volume_id, storage_node_id)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS photos (
                    photo_id VARCHAR(255) PRIMARY KEY,
                    volume_id VARCHAR(255) REFERENCES logical_volumes(volume_id),
                    size_bytes BIGINT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            conn.commit()
            logger.info("Database tables verified/created successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating database tables: {e}")
        raise

# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"status": "Directory Service is running"}

@app.post("/register_storage_node")
def register_storage_node(req: NodeRegistryRequest, db: psycopg2.extensions.connection = Depends(get_db)):
    """Allows storage nodes to register themselves with the directory."""
    with db.cursor() as cur:
        try:
            cur.execute("INSERT INTO storage_nodes (url) VALUES (%s) ON CONFLICT (url) DO NOTHING", (req.url,))
            db.commit()
            logger.info(f"Registered or updated storage node: {req.url}")
            return {"status": "success", "url": req.url}
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to register storage node {req.url}: {e}")
            raise HTTPException(status_code=500, detail="Failed to register node")

@app.post("/request_upload_location")
def request_upload_location(req: UploadRequest, db: psycopg2.extensions.connection = Depends(get_db)):
    """
    Phase 1 of upload: Find a writable volume or create one, then return an ephemeral photo_id.
    """
    with db.cursor(cursor_factory=DictCursor) as cur:
        # Find a writable volume with enough space
        cur.execute(
            "SELECT volume_id FROM logical_volumes WHERE status = 'writable' AND current_size_bytes + %s <= %s ORDER BY current_size_bytes DESC LIMIT 1",
            (req.photo_size_bytes, VOLUME_MAX_SIZE_BYTES)
        )
        volume = cur.fetchone()

        volume_id = None
        if volume:
            volume_id = volume['volume_id']
        else:
            # No suitable volume found, create a new one
            logger.info("No suitable volume found. Attempting to create a new one.")
            
            # Find the least-burdened storage node
            cur.execute("""
                SELECT s.id, s.url, COUNT(vr.volume_id) as volume_count
                FROM storage_nodes s
                LEFT JOIN volume_replicas vr ON s.id = vr.storage_node_id
                GROUP BY s.id, s.url
                ORDER BY volume_count ASC
                LIMIT 1;
            """)
            target_node = cur.fetchone()

            if not target_node:
                raise HTTPException(status_code=503, detail="No storage nodes available to create a new volume")
            
            try:
                # Ask the storage node to create a new volume
                res = requests.post(f"http://{target_node['url']}/create_volume")
                res.raise_for_status()
                new_volume_id = res.json()["volume_id"]

                # Register the new volume in our database
                cur.execute(
                    "INSERT INTO logical_volumes (volume_id) VALUES (%s)", (new_volume_id,)
                )
                cur.execute(
                    "INSERT INTO volume_replicas (volume_id, storage_node_id) VALUES (%s, %s)",
                    (new_volume_id, target_node['id'])
                )
                db.commit()
                volume_id = new_volume_id
                logger.info(f"Created new volume {volume_id} on node {target_node['url']}")
            except Exception as e:
                db.rollback()
                logger.error(f"Failed to create and register new volume: {e}")
                raise HTTPException(status_code=500, detail="Could not provision a new storage volume")

        # Get all replicas for the chosen volume
        cur.execute("""
            SELECT sn.url
            FROM volume_replicas vr
            JOIN storage_nodes sn ON vr.storage_node_id = sn.id
            WHERE vr.volume_id = %s
        """, (volume_id,))
        replicas = [row['url'] for row in cur.fetchall()]
        
        photo_id = str(uuid.uuid4())
        logger.info(f"Reserved photo_id {photo_id} for volume {volume_id}")
        
        return {"photo_id": photo_id, "volume_id": volume_id, "storage_node_urls": replicas}


@app.post("/commit_upload")
def commit_upload(req: CommitRequest, db: psycopg2.extensions.connection = Depends(get_db)):
    """Phase 2 of upload: Commit the photo metadata to the database and update volume stats."""
    with db.cursor() as cur:
        try:
            # 1. Insert the photo record
            cur.execute(
                "INSERT INTO photos (photo_id, volume_id, size_bytes) VALUES (%s, %s, %s)",
                (req.photo_id, req.volume_id, req.size_bytes)
            )

            # 2. Update the volume size
            cur.execute(
                "UPDATE logical_volumes SET current_size_bytes = current_size_bytes + %s WHERE volume_id = %s RETURNING current_size_bytes",
                (req.size_bytes, req.volume_id)
            )
            updated_size = cur.fetchone()[0]

            # 3. Check if the volume is now full
            if updated_size >= VOLUME_MAX_SIZE_BYTES:
                cur.execute(
                    "UPDATE logical_volumes SET status = 'readonly' WHERE volume_id = %s",
                    (req.volume_id,)
                )
                logger.info(f"Volume {req.volume_id} is now full and set to readonly.")
            
            db.commit()
            logger.info(f"Committed photo {req.photo_id} to volume {req.volume_id}")
            return {"status": "success", "photo_id": req.photo_id}
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to commit photo {req.photo_id}: {e}")
            raise HTTPException(status_code=500, detail="Failed to commit upload metadata")

@app.get("/lookup/{photo_id}")
async def lookup_photo(photo_id: str, db: psycopg2.extensions.connection = Depends(get_db)):
    """
    Looks up the logical volume and storage nodes for a given photo_id.
    """
    with db.cursor(cursor_factory=DictCursor) as cur:
        # 1. Get volume_id for the photo_id
        cur.execute("SELECT volume_id FROM photos WHERE photo_id = %s", (photo_id,))
        photo_record = cur.fetchone()

        if not photo_record:
            logger.warning(f"Photo ID {photo_id} not found in directory service.")
            raise HTTPException(status_code=404, detail="Photo mapping not found")

        volume_id = photo_record['volume_id']

        # 2. Get all replica storage node URLs for the volume
        cur.execute("""
            SELECT sn.url
            FROM volume_replicas vr
            JOIN storage_nodes sn ON vr.storage_node_id = sn.id
            WHERE vr.volume_id = %s
        """, (volume_id,))
        
        storage_node_urls = [row['url'] for row in cur.fetchall()]

        if not storage_node_urls:
            logger.error(f"No storage nodes found for volume {volume_id} (photo ID: {photo_id}). Data inconsistency.")
            raise HTTPException(status_code=500, detail="No storage nodes found for this photo's volume")

        logger.info(f"Lookup successful for photo_id {photo_id}: volume {volume_id}, nodes {storage_node_urls}")
        return {
            "logical_volume_id": volume_id,
            "storage_nodes": storage_node_urls
        }


# --- Startup ---
@app.on_event("startup")
def startup_event():
    """On startup, create DB tables and register service."""
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        redis_client.ping()
        redis_client.set("service:directory-service", "directory-service:8000")
        logger.info("Successfully connected to Redis and registered service.")
    except redis.exceptions.ConnectionError as e:
        logger.warning(f"Could not connect to Redis: {e}. Service discovery will be unavailable.")

    try:
        conn = get_db_connection()
        create_db_tables(conn)
        conn.close()
    except Exception as e:
        logger.critical(f"FATAL: Could not initialize database. Shutting down. Error: {e}")
        # In a real containerized setup, this might cause the service to crash-loop,
        # which is desirable until the DB is available.
        exit(1)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
