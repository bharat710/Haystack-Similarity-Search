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
import threading
import time

# --- Models ---
class UploadRequest(BaseModel):
    photo_size_bytes: int

class CommitRequest(BaseModel):
    photo_id: str
    volume_id: str
    size_bytes: int

class NodeRegistryRequest(BaseModel):
    url: str

#-FIXED-
class NodeHeartbeatRequest(BaseModel):
    url: str



# --- Setup ---
HOSTNAME = os.environ.get("HOSTNAME")
handler = logging_loki.LokiHandler(
    url="http://loki:3100/loki/api/v1/push",
    tags={"application": f"{HOSTNAME}"},
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
HEARTBEAT_TTL=60

# This redis client is for service discovery
try:
    discovery_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    discovery_client.ping()
    logger.info("Successfully connected to Redis for service discovery.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis for service discovery: {e}")
    discovery_client = None

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
                    status VARCHAR(20) DEFAULT 'unverified' NOT NULL, -- e.g., 'active', 'offline', 'unverified'
                    last_heartbeat TIMESTAMP WITH TIME ZONE,
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
                    status VARCHAR(20) DEFAULT 'active' NOT NULL,
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

#-FIXED-
@app.post("/register_storage_node")
def register_storage_node(req: NodeRegistryRequest, db: psycopg2.extensions.connection = Depends(get_db)):
    """
    Allows storage nodes to register themselves.
    On conflict (if URL already exists), it updates the last_heartbeat and status.
    """
    with db.cursor() as cur:
        try:
            cur.execute("""
                INSERT INTO storage_nodes (url, status, last_heartbeat)
                VALUES (%s, 'active', NOW())
                ON CONFLICT (url) DO UPDATE SET
                    status = 'active',
                    last_heartbeat = NOW();
            """, (req.url,))
            db.commit()
            logger.info(f"Registered or updated storage node: {req.url}")
            return {"status": "success", "url": req.url}
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to register storage node {req.url}: {e}")
            raise HTTPException(status_code=500, detail="Failed to register node")

#-FIXED-
@app.post("/heartbeat")
def storage_node_heartbeat(req: NodeHeartbeatRequest, db: psycopg2.extensions.connection = Depends(get_db)):
    """Receives a heartbeat from a storage node."""
    with db.cursor() as cur:
        try:
            cur.execute(
                "UPDATE storage_nodes SET last_heartbeat = NOW(), status = 'active' WHERE url = %s",
                (req.url,)
            )
            # Check if any row was updated
            if cur.rowcount == 0:
                logger.warning(f"Heartbeat received from unregistered node: {req.url}. Registering it now.")
                # If the node isn't registered, register it
                cur.execute("""
                    INSERT INTO storage_nodes (url, status, last_heartbeat)
                    VALUES (%s, 'active', NOW())
                    ON CONFLICT (url) DO UPDATE SET
                        status = 'active',
                        last_heartbeat = NOW();
                """, (req.url,))
            db.commit()
            logger.info(f"Heartbeat received from {req.url}")
            return {"status": "heartbeat received"}
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to process heartbeat from {req.url}: {e}")
            raise HTTPException(status_code=500, detail="Failed to process heartbeat")


#-FIXED-
@app.post("/request_upload_location")
async def request_upload_location(req: UploadRequest, db: psycopg2.extensions.connection = Depends(get_db)):
    """
    Phase 1 of upload: Find a writable volume or create one, then return an ephemeral photo_id
    and the selected storage nodes for replication.
    """
    with db.cursor(cursor_factory=DictCursor) as cur:
        volume_id = None
        selected_node_ids = []
        selected_node_urls = []

        # 1. Try to find an existing writable volume with enough space
        cur.execute(
            "SELECT lv.volume_id, sn.url, sn.id "
            "FROM logical_volumes lv "
            "JOIN volume_replicas vr ON lv.volume_id = vr.volume_id "
            "JOIN storage_nodes sn ON vr.storage_node_id = sn.id "
            "WHERE lv.status = 'writable' "
            "AND lv.current_size_bytes + %s <= %s "
            "AND sn.status = 'active' "
            "ORDER BY lv.current_size_bytes DESC LIMIT 1",
            (req.photo_size_bytes, VOLUME_MAX_SIZE_BYTES)
        )
        existing_volume_info = cur.fetchone()

        if existing_volume_info:
            volume_id = existing_volume_info['volume_id']
            # Get all active replicas for this existing volume
            cur.execute("""
                SELECT sn.id, sn.url
                FROM volume_replicas vr
                JOIN storage_nodes sn ON vr.storage_node_id = sn.id
                WHERE vr.volume_id = %s AND sn.status = 'active'
            """, (volume_id,))
            replicas_for_volume = cur.fetchall()
            if not replicas_for_volume:
                logger.warning(f"Found suitable volume {volume_id} but no active replicas. Attempting to create new volume.")
                volume_id = None # Force new volume creation
            else:
                selected_node_ids = [r['id'] for r in replicas_for_volume]
                selected_node_urls = [r['url'] for r in replicas_for_volume]
                logger.info(f"Using existing volume {volume_id} with active replicas: {selected_node_urls}")

        # 2. If no suitable existing volume or active replicas, create a new one
        if not volume_id:
            logger.info("No suitable existing volume found or no active replicas. Attempting to select nodes for a new volume.")
            
            # Select 2 (or 1 if less than 2 available) least-burdened active storage nodes
            cur.execute("""
                SELECT s.id, s.url, COUNT(vr.volume_id) as volume_count
                FROM storage_nodes s
                LEFT JOIN volume_replicas vr ON s.id = vr.storage_node_id
                WHERE s.status = 'active'
                GROUP BY s.id, s.url
                ORDER BY volume_count ASC, s.id ASC  -- s.id for consistent tie-breaking
                LIMIT 2;
            """)
            target_nodes = cur.fetchall()

            if not target_nodes:
                raise HTTPException(status_code=503, detail="No active storage nodes available to create a new volume")
            
            # Generate a new logical volume ID
            new_volume_id = str(uuid.uuid4())
            
            # Register the new logical volume in our database
            cur.execute(
                "INSERT INTO logical_volumes (volume_id) VALUES (%s)", (new_volume_id,)
            )
            volume_id = new_volume_id

            # Create volumes on the selected physical nodes and register replicas
            for node in target_nodes:
                try:
                    # Ask the storage node to create a new volume
                    # The storage node generates its own physical volume ID, we just need to ensure it's created
                    res = requests.post(f"http://{node['url']}/create_volume")
                    res.raise_for_status()
                    # We don't use the returned volume_id from storage node here,
                    # as our logical_volume_id is canonical.
                    
                    # Register this node as a replica for the new logical volume
                    cur.execute(
                        "INSERT INTO volume_replicas (volume_id, storage_node_id) VALUES (%s, %s)",
                        (volume_id, node['id'])
                    )
                    selected_node_ids.append(node['id'])
                    selected_node_urls.append(node['url'])
                    logger.info(f"Created new logical volume {volume_id} on physical node {node['url']}")
                except Exception as e:
                    logger.error(f"Failed to create volume {volume_id} on node {node['url']}: {e}")
                    # If one node fails to create a volume, we should ideally rollback or
                    # try to find another node. For simplicity, we'll continue with available nodes
                    # but log the error. The API Gateway will then deal with fewer replicas.

            if not selected_node_urls:
                db.rollback()
                raise HTTPException(status_code=500, detail="Failed to provision new logical volume on any selected storage node.")
            
            db.commit() # Commit volume and replica registrations
            logger.info(f"New logical volume {volume_id} provisioned with replicas: {selected_node_urls}")
        
        photo_id = str(uuid.uuid4())
        logger.info(f"Reserved photo_id {photo_id} for volume {volume_id} on nodes {selected_node_urls}")
        
        return {"photo_id": photo_id, "volume_id": volume_id, "storage_node_urls": selected_node_urls}


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

#-FIXED-
@app.get("/lookup/{photo_id}")
async def lookup_photo(photo_id: str, db: psycopg2.extensions.connection = Depends(get_db)):
    """
    Looks up the logical volume and storage nodes for a given photo_id.
    """
    with db.cursor(cursor_factory=DictCursor) as cur:
        # 1. Get volume_id for the photo_id, ensuring it's not marked as deleted
        cur.execute("SELECT volume_id, status FROM photos WHERE photo_id = %s", (photo_id,))
        photo_record = cur.fetchone()

        if not photo_record:
            logger.warning(f"Photo ID {photo_id} not found in directory service.")
            raise HTTPException(status_code=404, detail="Photo mapping not found")

        if photo_record['status'] == 'deleted':
            logger.warning(f"Attempt to look up a deleted photo: {photo_id}")
            raise HTTPException(status_code=404, detail="Photo has been deleted")

        volume_id = photo_record['volume_id']

        # 2. Get all *active* replica storage node URLs for the volume
        cur.execute("""
            SELECT sn.url
            FROM volume_replicas vr
            JOIN storage_nodes sn ON vr.storage_node_id = sn.id
            WHERE vr.volume_id = %s AND sn.status = 'active'
        """, (volume_id,))
        
        storage_node_urls = [row['url'] for row in cur.fetchall()]

        if not storage_node_urls:
            logger.error(f"No *active* storage nodes found for volume {volume_id} (photo ID: {photo_id}). All replicas may be offline.")
            raise HTTPException(status_code=503, detail="No active storage nodes are available for this photo's volume")

        logger.info(f"Lookup successful for photo_id {photo_id}: volume {volume_id}, nodes {storage_node_urls}")
        return {
            "logical_volume_id": volume_id,
            "storage_nodes": storage_node_urls
        }


@app.post("/mark_deleted/{photo_id}")
def mark_deleted(photo_id: str, db: psycopg2.extensions.connection = Depends(get_db)):
    """Marks a photo as 'deleted' in the directory service."""
    with db.cursor() as cur:
        try:
            # Check if the photo exists first
            cur.execute("SELECT status FROM photos WHERE photo_id = %s", (photo_id,))
            record = cur.fetchone()

            if not record:
                raise HTTPException(status_code=404, detail="Photo not found")
            
            if record[0] == 'deleted':
                # Idempotency: if already deleted, just return success
                logger.info(f"Photo {photo_id} is already marked as deleted.")
                return {"status": "success", "message": "Photo already marked as deleted."}

            # Update the status to 'deleted'
            cur.execute(
                "UPDATE photos SET status = 'deleted' WHERE photo_id = %s",
                (photo_id,)
            )
            db.commit()
            
            # Optionally, you can also update the volume size here to reclaim space logically
            # but that might be better handled by a separate background job
            
            logger.info(f"Successfully marked photo {photo_id} as deleted.")
            return {"status": "success", "message": f"Photo {photo_id} marked as deleted."}
        
        except HTTPException:
            # Re-raise HTTP exceptions to let FastAPI handle them
            raise
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to mark photo {photo_id} as deleted: {e}")
            raise HTTPException(status_code=500, detail="Failed to update photo status")



#-FIXED-
#-FIXED-
# --- Background Tasks ---
def check_stale_nodes():
    """Periodically checks for stale storage nodes and marks them as offline."""
    # Wait a bit for services to start up
    time.sleep(30)
    
    while True:
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                # Mark nodes as 'offline' if they haven't sent a heartbeat in over 2x the TTL
                # The node is expected to send a heartbeat every 30 seconds, so 65 is a safe buffer
                inactive_threshold_seconds = HEARTBEAT_TTL * 2 
                cur.execute("""
                    UPDATE storage_nodes
                    SET status = 'offline'
                    WHERE status = 'active' AND last_heartbeat < NOW() - INTERVAL '%s seconds'
                """, (inactive_threshold_seconds,))
                updated_rows = cur.rowcount
                conn.commit()
                if updated_rows > 0:
                    logger.info(f"Marked {updated_rows} stale storage node(s) as offline.")
            conn.close()
        except Exception as e:
            logger.error(f"Error in check_stale_nodes background task: {e}")
        
        # Check every 30 seconds
        time.sleep(30)


# --- Startup ---
def heartbeat():
    """Sends a heartbeat to Redis every HEARTBEAT_TTL/2 seconds."""
    while True:
        try:
            if discovery_client:
                service_key = "service:directory-service"
                service_address = f"{HOSTNAME}:8000"
                heartbeat_key = f"heartbeat:{service_address}"

                discovery_client.sadd(service_key, service_address)
                discovery_client.set(heartbeat_key, "1", ex=HEARTBEAT_TTL)
                logger.info(f"Sent heartbeat for {service_address}")
        except redis.exceptions.RedisError as e:
            logger.error(f"Error sending heartbeat: {e}")
        time.sleep(HEARTBEAT_TTL / 2)


@app.on_event("startup")
def startup_event():
    """On startup, create DB tables and start background tasks."""
    # Start the service heartbeat thread
    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
    heartbeat_thread.start()
    logger.info("Service heartbeat thread started.")

    # Start the thread to check for stale storage nodes
    stale_node_checker_thread = threading.Thread(target=check_stale_nodes, daemon=True)
    stale_node_checker_thread.start()
    logger.info("Stale node checker thread started.")

    # Initialize the database
    try:
        conn = get_db_connection()
        create_db_tables(conn)
        conn.close()
    except Exception as e:
        logger.critical(f"FATAL: Could not initialize database. Shutting down. Error: {e}")
        exit(1)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
