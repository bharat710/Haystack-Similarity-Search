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
import pika
import json

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
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
VOLUME_MAX_SIZE_BYTES = 100 * 1024 * 1024  # 100MB
HOSTNAME = os.environ.get("HOSTNAME")
HEARTBEAT_TTL=60

# RabbitMQ Exchanges
COMPACTION_TRIGGER_EXCHANGE = 'compaction_trigger_exchange'
COMPACTION_START_EXCHANGE = 'compaction_start_exchange'
COMPACTION_COMPLETE_EXCHANGE = 'compaction_complete_exchange'

# This redis client is for service discovery
try:
    discovery_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    discovery_client.ping()
    logger.info("Successfully connected to Redis for service discovery.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis for service discovery: {e}")
    discovery_client = None

# This redis client is for compaction tracking
try:
    compaction_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1, decode_responses=True)
    compaction_client.ping()
    logger.info("Successfully connected to Redis for compaction tracking.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis for compaction tracking: {e}")
    compaction_client = None


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
# --- RabbitMQ Functions ---
def get_rabbitmq_connection():
    """Creates and returns a new RabbitMQ connection."""
    return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))

def publish_event(exchange, routing_key, message, exchange_type='direct'):
    """Publishes an event to a RabbitMQ exchange."""
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=True)
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()
        logger.info(f"Published event to exchange '{exchange}' with routing key '{routing_key}': {message}")
    except pika.exceptions.AMQPError as e:
        logger.error(f"Failed to publish RabbitMQ event to exchange '{exchange}': {e}")


def on_compaction_trigger(ch, method, properties, body):
    """
    Handles the COMPACTION_TRIGGER event.
    This function is the entry point for starting a volume compaction cycle.
    """
    try:
        data = json.loads(body)
        volume_id = data.get('volume_id')
        if not volume_id:
            logger.error(f"Invalid COMPACTION_TRIGGER message: {data}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        logger.info(f"Received COMPACTION_TRIGGER for volume {volume_id}")

        # Use a Redis lock to prevent race conditions where multiple triggers
        # for the same volume are received at once.
        lock_key = f"compaction_lock:{volume_id}"
        compaction_lock = compaction_client.lock(lock_key, timeout=60)
        if not compaction_lock.acquire(blocking=False):
            logger.warning(f"Compaction for volume {volume_id} is already in progress. Ignoring trigger.")
            ch.basic_ack(delivery_tag=method.delivery_tag) # Acknowledge message to remove from queue
            return
        
        db_conn = None
        try:
            db_conn = get_db_connection()
            with db_conn.cursor(cursor_factory=DictCursor) as cur:
                # 1. Check volume status and get replica count
                cur.execute(
                    "SELECT status FROM logical_volumes WHERE volume_id = %s FOR UPDATE",
                    (volume_id,)
                )
                volume_status = cur.fetchone()
                if not volume_status or volume_status['status'] != 'writable':
                    logger.warning(f"Volume {volume_id} is not in a writable state. Current status: {volume_status['status'] if volume_status else 'not found'}. Aborting compaction.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    db_conn.commit() # Release row lock
                    return

                cur.execute("SELECT COUNT(id) FROM volume_replicas WHERE volume_id = %s", (volume_id,))
                replica_count = cur.fetchone()[0]

                if replica_count == 0:
                    logger.warning(f"No replicas found for volume {volume_id}. Nothing to compact.")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    db_conn.commit()
                    return

                # 2. Update volume status to readonly
                cur.execute(
                    "UPDATE logical_volumes SET status = 'readonly' WHERE volume_id = %s",
                    (volume_id,)
                )
                logger.info(f"Marked volume {volume_id} as readonly.")
                
                # 3. Generate a round_id and set up tracking in Redis
                round_id = str(uuid.uuid4())
                compaction_client.set(f"compaction:{round_id}:pending_replicas", replica_count)
                
                # 4. Broadcast the compaction start event
                publish_event(
                    exchange=COMPACTION_START_EXCHANGE,
                    routing_key='', # Routing key is ignored for fanout exchanges
                    message={"volume_id": volume_id, "round_id": round_id},
                    exchange_type='fanout'
                )
                logger.info(f"Broadcasted COMPACTION_START for volume {volume_id} with round_id {round_id}")
            
            db_conn.commit()
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logger.error(f"Error during compaction trigger for volume {volume_id}: {e}", exc_info=True)
            if db_conn:
                db_conn.rollback()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True) # Requeue on failure
        finally:
            if db_conn:
                db_conn.close()
            compaction_lock.release()

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode RabbitMQ message body: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        logger.error(f"Unexpected error in on_compaction_trigger: {e}", exc_info=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def on_compaction_complete(ch, method, properties, body):
    """
    Handles the COMPACTION_COMPLETE event.
    Tracks the progress of compaction across all replicas.
    """
    try:
        data = json.loads(body)
        volume_id = data.get('volume_id')
        round_id = data.get('round_id')
        storage_node_id = data.get('storage_node_id')
        new_size = data.get('new_size')

        if not all([volume_id, round_id, storage_node_id, new_size is not None]):
            logger.error(f"Invalid COMPACTION_COMPLETE message: {data}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        logger.info(f"Received COMPACTION_COMPLETE from {storage_node_id} for volume {volume_id}, round {round_id}")

        # Check if this compaction round is still active
        pending_replicas_key = f"compaction:{round_id}:pending_replicas"
        if not compaction_client.exists(pending_replicas_key):
            logger.warning(f"Compaction round {round_id} no longer active. Ignoring completion message from {storage_node_id}.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Store the new size reported by the node
        sizes_key = f"compaction:{round_id}:sizes"
        compaction_client.hset(sizes_key, storage_node_id, new_size)

        # Decrement the counter for pending replicas
        remaining_replicas = compaction_client.decr(pending_replicas_key)

        if remaining_replicas == 0:
            logger.info(f"All replicas for volume {volume_id} (round {round_id}) have completed compaction. Finalizing...")
            # Use a thread to avoid blocking the consumer
            finalization_thread = threading.Thread(
                target=finalize_compaction,
                args=(volume_id, round_id)
            )
            finalization_thread.start()
        
        elif remaining_replicas < 0:
             logger.warning(f"Compaction counter for round {round_id} is negative. This may indicate duplicate 'complete' messages.")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode RabbitMQ message body: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        logger.error(f"Unexpected error in on_compaction_complete: {e}", exc_info=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def finalize_compaction(volume_id, round_id):
    """
    Finalizes the compaction process for a volume.
    - Deletes 'deleted' photo entries from the database.
    - Updates the volume's size.
    - Sets the volume back to 'writable'.
    - Cleans up tracking keys in Redis.
    """
    db_conn = None
    lock_key = f"compaction_lock:{volume_id}"
    compaction_lock = compaction_client.lock(lock_key, timeout=60)
    
    try:
        # Re-acquire lock to ensure no other process interferes during finalization
        acquired = compaction_lock.acquire(blocking=True, blocking_timeout=30)
        if not acquired:
            logger.error(f"Could not acquire lock for finalizing compaction on volume {volume_id}. Aborting.")
            return

        db_conn = get_db_connection()
        with db_conn.cursor() as cur:
            # 1. Get the new canonical size from the max of all reported sizes
            sizes_key = f"compaction:{round_id}:sizes"
            reported_sizes = compaction_client.hvals(sizes_key)
            if not reported_sizes:
                raise ValueError("No new sizes were reported for this compaction round.")
            
            new_volume_size = max(int(s) for s in reported_sizes)

            # 2. Delete photo entries marked as 'deleted' for this volume
            cur.execute(
                "DELETE FROM photos WHERE volume_id = %s AND status = 'deleted'",
                (volume_id,)
            )
            deleted_rows = cur.rowcount
            logger.info(f"Deleted {deleted_rows} photo entries for volume {volume_id}.")

            # 3. Update the volume's size and status
            cur.execute(
                "UPDATE logical_volumes SET current_size_bytes = %s, status = 'writable' WHERE volume_id = %s",
                (new_volume_size, volume_id)
            )
            logger.info(f"Volume {volume_id} is now writable with new size {new_volume_size} bytes.")
        
        db_conn.commit()
        logger.info(f"Successfully finalized compaction for volume {volume_id}, round {round_id}.")

    except Exception as e:
        logger.error(f"Failed to finalize compaction for volume {volume_id}: {e}", exc_info=True)
        if db_conn:
            db_conn.rollback()
    finally:
        # 4. Clean up all Redis keys for this compaction round
        if compaction_client:
            pending_replicas_key = f"compaction:{round_id}:pending_replicas"
            sizes_key = f"compaction:{round_id}:sizes"
            compaction_client.delete(pending_replicas_key, sizes_key)
            logger.info(f"Cleaned up Redis tracking keys for round {round_id}.")
        
        # only release if we actually acquired it
        if acquired:
            try:
                compaction_lock.release()
            except redis.exceptions.LockError:
                logger.warning(f"Tried to release lock for volume {volume_id}, but it was not owned.")
        if db_conn:
            db_conn.close()


def rabbitmq_consumer_thread():
    """The main thread for the RabbitMQ consumer."""
    while True:
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()

            # Declare exchanges
            channel.exchange_declare(exchange=COMPACTION_TRIGGER_EXCHANGE, exchange_type='direct', durable=True)
            channel.exchange_declare(exchange=COMPACTION_COMPLETE_EXCHANGE, exchange_type='direct', durable=True)

            # Declare queues
            trigger_queue = channel.queue_declare(queue='compaction_trigger_queue', durable=True)
            complete_queue = channel.queue_declare(queue='compaction_complete_queue', durable=True)

            # Bind queues
            channel.queue_bind(exchange=COMPACTION_TRIGGER_EXCHANGE, queue=trigger_queue.method.queue, routing_key='compaction.trigger')
            channel.queue_bind(exchange=COMPACTION_COMPLETE_EXCHANGE, queue=complete_queue.method.queue, routing_key='compaction.completed')

            # Set up consumers
            channel.basic_consume(queue=trigger_queue.method.queue, on_message_callback=on_compaction_trigger, auto_ack=False)
            channel.basic_consume(queue=complete_queue.method.queue, on_message_callback=on_compaction_complete, auto_ack=False)

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

    # Start the RabbitMQ consumer thread
    rabbitmq_thread = threading.Thread(target=rabbitmq_consumer_thread, daemon=True)
    rabbitmq_thread.start()
    logger.info("RabbitMQ consumer thread started.")

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
