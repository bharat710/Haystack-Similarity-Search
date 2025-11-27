from pathlib import Path

DATASET_ROOT = Path("/Users/bharat/Downloads/faiss_cluster_dynamic copy/Uploaded_Images")
OUTPUT_DIR = Path("artifacts")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EMBEDDINGS_FILE = OUTPUT_DIR / "embeddings.npy"
PHOTO_IDS_FILE = OUTPUT_DIR / "photo_ids.txt"
FAISS_INDEX_FILE = OUTPUT_DIR / "faiss.index"

CLUSTER_LABELS_FILE = OUTPUT_DIR / "cluster_labels.npy"
CLUSTER_LEADERS_FILE = OUTPUT_DIR / "cluster_leaders.json"
CLUSTER_MEMBERS_FILE = OUTPUT_DIR / "cluster.json"