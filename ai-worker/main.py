import os
import pika
import requests
import torch
import open_clip
import numpy as np
from PIL import Image
from io import BytesIO
import logging
import threading
import time
from fastapi import FastAPI
from dotenv import load_dotenv
import base64

load_dotenv()

# --- Setup ---
# Using standard logging for now
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ai-worker-logger")

app = FastAPI()

# --- Globals & Config ---
# RabbitMQ Config
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
PHOTO_QUEUE = os.environ.get("PHOTO_QUEUE", "photo_processing_queue")

# Service URLs
STORAGE_NODE_URL = os.environ.get("STORAGE_NODE_URL", "http://storage-node:8080")
SEARCH_SERVICE_URL = os.environ.get("SEARCH_SERVICE_URL", "http://search-service:8000")

# AI Model
MODEL_NAME = 'ViT-B-32-quickgelu'
PRETRAINED_DATASET = 'laion400m_e32'
model = None
preprocess = None


# --- AI Model Loading ---
def load_model():
    """Loads the OpenCLIP model and preprocessing transforms."""
    global model, preprocess
    try:
        logger.info(f"Loading AI model '{MODEL_NAME}' with '{PRETRAINED_DATASET}' weights...")
        model, _, preprocess = open_clip.create_model_and_transforms(
            MODEL_NAME, pretrained=PRETRAINED_DATASET, device="cpu"
        )
        model.eval() # Set model to evaluation mode
        logger.info("AI model loaded successfully.")
    except Exception as e:
        logger.error(f"Fatal error: Could not load AI model. {e}")
        # If the model can't load, the service is useless.
        # In a real system, this might trigger a restart or alert.
        raise

# --- Worker Logic ---
def process_photo_callback(ch, method, properties, body):
    """The core function to process a photo from the queue."""
    photo_id = body.decode('utf-8')
    logical_id = photo_id.split('/')[0]
    photo_id = photo_id.split('/')[1]
    logger.info(f"Received job for photo_id: {photo_id}")

    # 1. Download image from Storage Node
    try:
        response = requests.get(f"{STORAGE_NODE_URL}/photos/{logical_id}/{photo_id}", timeout=30)
        response.raise_for_status()
        payload = response.json()
        b64_data = payload["data"]
        image_bytes = base64.b64decode(b64_data)
        image = Image.open(BytesIO(image_bytes)).convert("RGB")
        logger.info(f"Successfully downloaded image for photo_id: {photo_id}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download image for {photo_id}: {e}. Re-queueing.")
        # Negative Acknowledgement: re-queue the message
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    # 2. Generate feature vector
    try:
        with torch.no_grad():
            image_tensor = preprocess(image).unsqueeze(0).to("cpu")
            vector = model.encode_image(image_tensor)
            vector /= vector.norm(dim=-1, keepdim=True) # Normalize the vector
            vector_list = vector.cpu().numpy().flatten().tolist()
        logger.info(f"Successfully generated vector for photo_id: {photo_id}")
    except Exception as e:
        logger.error(f"Failed to generate vector for {photo_id}: {e}. Discarding message.")
        # Acknowledge the message to discard it, as retrying will likely fail again.
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # 3. Send vector to Search Service
    try:
        payload = {"photo_uuid": photo_id, "vector": vector_list}
        resp = requests.post(f"{SEARCH_SERVICE_URL}/add_vector/", json=payload, timeout=30)

        if 400 <= resp.status_code < 500:
            logger.error(
                f"Permanent error {resp.status_code} for {photo_id}: {resp.text}. "
                "Discarding message."
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        resp.raise_for_status()
        logger.info(f"Successfully sent vector for {photo_id} to search service.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send vector for {photo_id}: {e}. Re-queueing.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return


    # 4. Acknowledge the RabbitMQ message (Job Done)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logger.info(f"Successfully completed job for photo_id: {photo_id}")


def start_rabbitmq_consumer():
    """Connects to RabbitMQ and starts consuming messages."""
    while True:
        try:
            logger.info("Connecting to RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            # Ensure the queue exists
            channel.queue_declare(queue=PHOTO_QUEUE, durable=True)
            # Don't dispatch a new message to a worker until it has processed and acknowledged the previous one
            channel.basic_qos(prefetch_count=1)
            
            channel.basic_consume(queue=PHOTO_QUEUE, on_message_callback=process_photo_callback)

            logger.info(f"[*] Waiting for messages on queue '{PHOTO_QUEUE}'. To exit press CTRL+C")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            logger.warning(f"RabbitMQ connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"An unexpected error occurred in RabbitMQ consumer: {e}. Restarting...")
            time.sleep(5)


# --- FastAPI Endpoints & Events ---
@app.on_event("startup")
def startup_event():
    """On startup, load the model and start the RabbitMQ consumer in a background thread."""
    load_model()
    
    # Run the consumer in a separate thread to not block the FastAPI server
    consumer_thread = threading.Thread(target=start_rabbitmq_consumer, daemon=True)
    consumer_thread.start()

@app.get("/")
def read_root():
    return {"status": "AI Worker is running"}

# The uvicorn command in the Dockerfile will run this.
