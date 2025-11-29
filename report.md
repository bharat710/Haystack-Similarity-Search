### Stateful Clustering Design: End-to-End Flow

I have fixed the Redis connection issue. The service now uses a robust retry loop to ensure it connects successfully on startup.

Here is the verified flow of the stateful two-level clustering design that you requested:

#### 1. Photo Upload & Message Queuing
- **Action**: A user uploads a photo. The `api-gateway` routes this request to a `storage-node`.
- **Storage**: The `storage-node` saves the raw photo data to a volume file (`.dat`) on its disk.
- **Notification**: After saving, the `storage-node` publishes a message to the `photo_processing_queue` in RabbitMQ. This message contains the URL where the photo can be retrieved (e.g., `http://storage-node-1:8080/photos/...`).

#### 2. `ai-worker`: Stateful Embedding and Local Clustering
- **Consumption**: An `ai-worker` instance picks up the photo URL message from the RabbitMQ queue.
- **Embedding Generation**: It calls the URL to download the photo data, then passes it through the OpenCLIP model to generate a 512-dimension vector embedding.
- **Stateful Local Clustering**: The worker feeds this new embedding into its **persistent local BIRCH model**.
  - Unlike before, this model is **never reset**. Its internal CF (Clustering Feature) Tree grows and refines itself with every photo it processes.
  - This allows the worker to build a continuously improving, high-quality summary of the data distribution it has seen over its entire lifetime.
- **Batching**: The worker adds the new photo's ID and raw embedding to a temporary batch list.

#### 3. `ai-worker` to `search-service`: Sending the Summary
- **Trigger**: Once the temporary batch is full, the worker prepares to send its summary.
- **Payload Creation**: It creates a JSON payload containing:
  1.  Its unique `worker_id` (e.g., `ai-worker-1`).
  2.  The batch of raw `photo_ids` and `embeddings` (for database storage).
  3.  The full list of **subcluster centroids** from its persistent, stateful `local_birch` model. These are high-quality centroids representing all data seen by the worker.
- **Transmission**: The payload is sent via an HTTP POST request to the `/internal/update-summary` endpoint on the `search-service`.

#### 4. `search-service`: Aggregating Summaries
- **Ingestion**: The `/internal/update-summary` endpoint receives the data.
- **DB Storage**: It saves the batch of raw photo IDs and embeddings into the `embeddings` table in the PostgreSQL database. This ensures the raw data is always available.
- **Centroid Aggregation**: It takes the list of centroids and the `worker_id` from the payload and updates a global dictionary, `worker_centroids`. This dictionary holds the most recent cluster summaries from every active `ai-worker`.

#### 5. `search-service`: Fast, Two-Level Global Clustering
- **Trigger**: The `/recluster` endpoint is called (e.g., by a periodic automated job or an administrator).
- **Global Model Fitting**:
  - The service aggregates the centroids from all workers stored in the `worker_centroids` dictionary into a single large list.
  - It fits a new `global_model` (BIRCH) on this list of **centroids**, not on the millions of raw embeddings. This step is extremely fast.
- **Global Label Prediction**:
  - It fetches all raw photo embeddings from the database.
  - It uses the `global_model.predict()` method on these raw embeddings. Because the model's tree was built from centroids, this efficiently finds the appropriate global cluster for each individual photo by identifying its nearest centroid's cluster.
- **Database Update**: The service calculates leader photos for each global cluster and updates the `cluster_id` and `is_leader` columns for every photo in the database.

This completes the implementation. The system now uses a more accurate and vastly more efficient online clustering approach, driven by stateful workers providing high-quality, real-time summaries.
