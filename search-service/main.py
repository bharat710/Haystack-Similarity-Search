from typing import List, Optional, Dict
import logging
import redis
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import Birch
import numpy as np
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from pgvector.psycopg2 import register_vector
import psycopg2
from logging_loki import LokiHandler
import os


# --- LOGGING ---
LOKI_URL = os.environ.get("LOKI_URL", "http://loki:3100/loki/api/v1/push")
handler = LokiHandler(
    url=LOKI_URL,
    tags={"application": "search-service"},
    version="1",
)

logger = logging.getLogger("search-service")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# --- CONFIGURATION ---
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://user:password@localhost:5433/search")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
SERVICE_NAME = "search-service"
SERVICE_URL = f"http://{SERVICE_NAME}:8002"

# --- DATABASE ---


def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    register_vector(conn)
    return conn


def create_embeddings_table():
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS embeddings (
                photo_id VARCHAR(255) PRIMARY KEY,
                embedding VECTOR(512),
                cluster_id INTEGER,
                is_leader BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
    conn.close()

# --- MODELS ---


class UpdateSummaryPayload(BaseModel):
    photo_ids: List[str]
    embeddings: List[List[float]]
    subcluster_centroids: List[List[float]]
    worker_id: Optional[str] = None

import time

# --- GLOBAL STATE ---


app = FastAPI()
# This dict will store the latest set of centroids from each worker
worker_centroids: Dict[str, np.ndarray] = {}
redis_client: Optional[redis.Redis] = None

# --- EVENTS ---


@app.on_event("startup")
def startup_event():
    global redis_client
    logger.info("Search service starting up...")
    # Connect and create extension BEFORE any other DB calls that might use the vector type
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        conn.commit()
    conn.close()

    create_embeddings_table()
    logger.info("Database table checked/created.")

    # Register with Redis (with retry)
    while True:
        try:
            client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
            client.ping()
            redis_client = client
            logger.info("Successfully connected to Redis.")
            redis_client.sadd(f"service:{SERVICE_NAME}", f"{SERVICE_NAME}:8002")
            logger.info(
                f"Registered {SERVICE_NAME} with Redis at {SERVICE_URL}")
            break  # Exit loop on success
        except redis.exceptions.ConnectionError as e:
            logger.error(
                f"Could not connect to Redis: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# --- ENDPOINTS ---


@app.get("/")
def read_root():
    return {"service": "search-service"}


@app.post("/internal/update-summary")
async def update_summary(payload: UpdateSummaryPayload):
    """
    Receives embedding and centroid data from an ai-worker.
    Stores the embeddings and updates the worker's centroid contribution.
    """
    logger.info(
        f"Received summary update with {len(payload.photo_ids)} photos from worker {payload.worker_id or 'unknown'}.")

    # --- Store Embeddings in DB ---
    conn = get_db_connection()
    with conn.cursor() as cur:
        for photo_id, embedding in zip(payload.photo_ids, payload.embeddings):
            cur.execute(
                "INSERT INTO embeddings (photo_id, embedding) VALUES (%s, %s) ON CONFLICT (photo_id) DO NOTHING;",
                (photo_id, np.array(embedding))
            )
    conn.commit()
    conn.close()
    logger.info(
        f"Stored {len(payload.photo_ids)} new embeddings in the database.")

    # --- Update Centroid Contributions ---
    if payload.worker_id and payload.subcluster_centroids:
        worker_centroids[payload.worker_id] = np.array(
            payload.subcluster_centroids)
        logger.info(
            f"Updated centroids for worker '{payload.worker_id}' with {len(payload.subcluster_centroids)} centroids.")

    return {"status": "summary updated"}


def compute_cluster_leaders(emb, photo_ids, labels, unique_labels):
    """
    For each cluster, compute its centroid and choose the member closest
    to the centroid as the 'leader' photo.
    """
    leaders = {}
    for label in unique_labels:
        if label == -1:
            continue  # Skip noise points

        idxs = np.where(labels == label)[0]
        if len(idxs) == 0:
            continue

        cluster_emb = emb[idxs]
        centroid = cluster_emb.mean(axis=0, keepdims=True)
        sims = cosine_similarity(cluster_emb, centroid)

        best_local_idx = np.argmax(sims)
        best_photo_id = photo_ids[idxs[best_local_idx]]
        leaders[int(label)] = best_photo_id

    return leaders


@app.post("/recluster")
async def recluster_photos():
    """
    Performs a fast, two-level recluster.
    1. Clusters the aggregated centroids from all workers.
    2. Uses the resulting model to predict cluster labels for all photos.
    3. Updates leaders and database assignments.
    """
    logger.info("Starting two-level recluster process...")

    # 1. Aggregate centroids from all workers
    if not worker_centroids:
        raise HTTPException(
            status_code=404, detail="No worker centroids available to perform clustering.")

    all_centroids = np.vstack(list(worker_centroids.values()))
    logger.info(
        f"Aggregated {len(all_centroids)} centroids from {len(worker_centroids)} workers.")

    if len(all_centroids) == 0:
        raise HTTPException(
            status_code=404, detail="Centroid list is empty.")

    # 2. Fit global BIRCH model on the aggregated centroids
    logger.info("Fitting global BIRCH model on aggregated centroids...")
    global_model = Birch(threshold=0.05, n_clusters=None)
    global_model.fit(all_centroids)
    logger.info("Global model fitting complete.")

    # 3. Fetch all photo embeddings from the database
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT photo_id, embedding FROM embeddings;")
        rows = cur.fetchall()
    conn.close()

    if not rows:
        raise HTTPException(
            status_code=404, detail="No embeddings found in database to cluster.")

    photo_ids = [row[0] for row in rows]
    embeddings = np.array([row[1] for row in rows])
    logger.info(
        f"Loaded {len(photo_ids)} embeddings from database for labeling.")

    # 4. Predict global cluster labels for each photo embedding
    logger.info("Predicting global cluster labels for all photos...")
    # This works because predict() finds the closest leaf in the fitted tree.
    # Since the tree was built from centroids, this effectively finds the
    # cluster of the nearest centroid for each photo.
    labels = global_model.predict(embeddings)
    unique_labels = sorted(list(set(labels)))
    num_clusters = len(unique_labels) - \
        (1 if -1 in unique_labels else 0)
    logger.info(
        f"Prediction complete. Found {num_clusters} final clusters.")

    # 5. Determine cluster leaders
    logger.info("Computing cluster leaders...")
    leaders = compute_cluster_leaders(
        embeddings, photo_ids, labels, unique_labels)

    # 6. Update database with new assignments
    logger.info("Updating database with new cluster assignments...")
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("UPDATE embeddings SET is_leader = FALSE;")
        for i, photo_id in enumerate(photo_ids):
            cluster_id = int(labels[i])
            is_leader = leaders.get(cluster_id) == photo_id
            cur.execute(
                "UPDATE embeddings SET cluster_id = %s, is_leader = %s WHERE photo_id = %s;",
                (cluster_id, is_leader, photo_id)
            )
    conn.commit()
    conn.close()

    logger.info("Recluster process finished successfully.")
    return {
        "status": "reclustering complete",
        "total_clusters": num_clusters,
        "total_photos_processed": len(photo_ids)
    }


@app.get("/clusters")
async def get_clusters():
    """
    Returns a list of all clusters with their representative photo.
    """
    logger.info("Fetching cluster leaders...")
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cluster_id, photo_id FROM embeddings WHERE is_leader = TRUE AND cluster_id IS NOT NULL ORDER BY cluster_id;"
        )
        rows = cur.fetchall()
    conn.close()

    clusters = [{"cluster_id": row[0], "representative_photo_id": row[1]}
                for row in rows]
    logger.info(f"Found {len(clusters)} cluster leaders.")
    return clusters


@app.get("/cluster/{cluster_id}")
async def get_cluster_members(cluster_id: int):
    """
    Returns a list of all photo IDs for a given cluster.
    """
    logger.info(f"Fetching members for cluster {cluster_id}...")
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT photo_id FROM embeddings WHERE cluster_id = %s ORDER BY created_at DESC;",
            (cluster_id,)
        )
        rows = cur.fetchall()
    conn.close()

    members = [row[0] for row in rows]
    logger.info(f"Found {len(members)} members in cluster {cluster_id}.")
    if not members:
        raise HTTPException(
            status_code=404, detail=f"Cluster {cluster_id} not found or is empty.")

    return {"cluster_id": cluster_id, "members": members}
