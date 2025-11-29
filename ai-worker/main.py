import pika
import time
import os
import sys
import torch
import open_clip
from PIL import Image
import requests
from io import BytesIO
import numpy as np
from sklearn.cluster import Birch
import logging
from logging_loki import LokiHandler
import json
import base64

# --- LOGGING ---
LOKI_URL = os.environ.get("LOKI_URL", "http://loki:3100/loki/api/v1/push")
handler = LokiHandler(
    url=LOKI_URL,
    tags={"application": "ai-worker"},
    version="1",
)

logger = logging.getLogger("ai-worker")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# --- CONFIGURATION ---
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
PHOTO_QUEUE = "photo_processing_queue"
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
MODEL_NAME = "ViT-B-32"
PRETRAINED = "laion2b_s34b_b79k"
SEARCH_SERVICE_URL = os.environ.get("SEARCH_SERVICE_URL", "http://localhost:8002")
BATCH_SIZE = 10
CF_TREE_DUMP_PATH = "cf_tree.dump"
WORKER_ID = os.environ.get("HOSTNAME", f"ai-worker-{os.getpid()}")


# ---  MODEL LOADING ---
def load_model_and_preprocess():
    logger.info(f"Loading OpenCLIP model {MODEL_NAME}...")
    model, _, preprocess = open_clip.create_model_and_transforms(
        MODEL_NAME,
        pretrained=PRETRAINED
    )
    model.eval()
    model.to(DEVICE)
    logger.info(f"Model loaded on device: {DEVICE}")
    return model, preprocess

model, preprocess = load_model_and_preprocess()

def get_photo_id_from_path(path:str):
    return path.split('/')[-1]

def generate_embedding(photo_path: str):
    """
    Downloads a photo, preprocesses it, and generates a 512-dim embedding.
    """
    try:
        logger.info(f"Generating embedding for: {photo_path}")
        response = requests.get(photo_path, timeout=10)
        response.raise_for_status()
        
        # Decode the JSON response from the storage node
        json_response = response.json()
        encoded_data = json_response['data']
        photo_data = base64.b64decode(encoded_data)
        
        img = Image.open(BytesIO(photo_data)).convert("RGB")
        
        with torch.no_grad():
            x = preprocess(img).unsqueeze(0).to(DEVICE)
            embedding = model.encode_image(x)
            embedding /= embedding.norm(dim=-1, keepdim=True)
            
        return embedding.cpu().numpy().astype("float32")[0]

    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading photo {photo_path}: {e}")
    except (KeyError, json.JSONDecodeError) as e:
        logger.error(f"Error parsing response from storage node for {photo_path}: {e}")
    except Exception as e:
        logger.error(f"Error generating embedding for {photo_path}: {e}")
    return None

# --- RABBITMQ & CLUSTERING ---
class AIWorker:
    def __init__(self):
        self.local_birch = Birch(n_clusters=None, threshold=0.5, branching_factor=50)
        self.photo_embeddings = []
        self.photo_ids = []
        self.connection = None

    def connect_to_rabbitmq(self):
        while not self.connection:
            try:
                logger.info(f"Connecting to RabbitMQ at {RABBITMQ_HOST}...")
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            except pika.exceptions.AMQPConnectionError:
                logger.error("Connection to RabbitMQ failed. Retrying in 5 seconds...")
                time.sleep(5)
    
    def start_consuming(self):
        self.connect_to_rabbitmq()
        channel = self.connection.channel()
        channel.queue_declare(queue=PHOTO_QUEUE, durable=True)
        logger.info("Successfully connected to RabbitMQ. Waiting for messages.")

        channel.basic_consume(queue=PHOTO_QUEUE, on_message_callback=self.process_message)
        channel.start_consuming()

    def process_message(self, ch, method, properties, body):
        photo_path = body.decode()
        photo_id = get_photo_id_from_path(photo_path)
        logger.info(f"\nReceived photo: {photo_path}")
        
        embedding = generate_embedding(photo_path)
        
        if embedding is not None:
            logger.info(f"Generated embedding for photo_id {photo_id}")
            
            # Add to local data
            self.photo_embeddings.append(embedding)
            self.photo_ids.append(photo_id)
            
            # Update local BIRCH model
            self.local_birch.partial_fit(np.array([embedding]))
            logger.info(f"Added to local BIRCH model. Batch size is now {len(self.photo_ids)}.")
            
            # Check if batch is ready
            if len(self.photo_ids) >= BATCH_SIZE:
                self.send_batch_to_search_service()

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def send_batch_to_search_service(self):
        logger.info(f"Batch of {len(self.photo_ids)} reached. Sending to search service...")

        if not self.photo_ids:
            logger.warning("Batch is empty, skipping send.")
            return
        
        # We need to send both the raw embeddings for storage and the
        # subcluster centroids for the global BIRCH model.
        
        # Extract subcluster centroids from the leaf nodes of the local tree
        subcluster_centers = self.local_birch.root_.subclusters_
        leaf_centroids = [sc.centroid_ for sc in subcluster_centers]

        payload = {
            "worker_id": WORKER_ID,
            "photo_ids": self.photo_ids,
            "embeddings": [emb.tolist() for emb in self.photo_embeddings],
            "subcluster_centroids": [centroid.tolist() for centroid in leaf_centroids]
        }

        try:
            response = requests.post(f"{SEARCH_SERVICE_URL}/internal/update-summary", json=payload, timeout=30)
            response.raise_for_status()
            logger.info("Successfully sent batch to search service.")
            
            # Clear local batch after successful send
            self.photo_embeddings = []
            self.photo_ids = []
            # The local_birch model is now stateful and is NOT reset.

        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending batch to search service: {e}")
            # In a real system, you'd add retry logic or a dead-letter queue here.


def main():
    worker = AIWorker()
    worker.start_consuming()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
