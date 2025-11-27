# faiss_cluster.py

import json
import numpy as np
import faiss
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity


from config import (
    EMBEDDINGS_FILE,
    PHOTO_IDS_FILE,
    FAISS_INDEX_FILE,
    CLUSTER_LABELS_FILE,
    CLUSTER_LEADERS_FILE,
    CLUSTER_MEMBERS_FILE,
)


# ------------------------- utils ------------------------- #

def load_embeddings_and_ids():
    if not EMBEDDINGS_FILE.exists():
        raise FileNotFoundError(
            f"Embeddings not found: {EMBEDDINGS_FILE}. "
            "Run: python3 prepare_embeddings.py"
        )
    if not PHOTO_IDS_FILE.exists():
        raise FileNotFoundError(
            f"Photo IDs not found: {PHOTO_IDS_FILE}. "
            "Run: python3 prepare_embeddings.py"
        )

    emb = np.load(EMBEDDINGS_FILE).astype("float32")
    with open(PHOTO_IDS_FILE) as f:
        ids = [l.strip() for l in f]

    if emb.shape[0] != len(ids):
        raise RuntimeError(
            f"Mismatch: {emb.shape[0]} embeddings vs {len(ids)} IDs"
        )

    return emb, ids


def build_faiss_index(emb):
    n, d = emb.shape
    print(f"[FAISS] Building IndexFlatL2: n={n}, d={d}")
    index = faiss.IndexFlatL2(d)
    index.add(emb)
    faiss.write_index(index, str(FAISS_INDEX_FILE))
    print(f"[FAISS] Saved index to {FAISS_INDEX_FILE}")
    return index



def run_hac(emb, cosine_threshold=0.35):
    """
    HAC using PRECOMPUTED cosine distances.
    Splits clusters correctly (cats, dogs, etc.)
    """

    n = emb.shape[0]
    if n == 1:
        print("[HAC] Only one image → cluster 0")
        return np.array([0]), [0]

    # Normalize embeddings
    emb = emb / np.linalg.norm(emb, axis=1, keepdims=True)

    # Build cosine distance matrix: D = 1 - cosine_similarity
    sim = cosine_similarity(emb)
    dist_matrix = 1 - sim

    # Agglomerative clustering using precomputed distances
    model = AgglomerativeClustering(
        metric='precomputed',       # IMPORTANT
        linkage='average',          # avoids single-cluster collapse
        distance_threshold=cosine_threshold,
        n_clusters=None
    )

    labels = model.fit_predict(dist_matrix)
    unique = sorted(set(labels))

    print(f"[HAC] Distance threshold={cosine_threshold} → clusters={len(unique)} → {unique}")
    return labels, unique

def compute_cluster_leaders(emb, photo_ids, labels, unique_labels):
    """
    For each cluster, compute its centroid and choose the member closest
    to the centroid as the 'leader' photo.
    """
    leaders = {}
    for label in unique_labels:
        idxs = np.where(labels == label)[0]
        cluster_emb = emb[idxs]        # (Nc, d)

        # centroid in embedding space
        centroid = cluster_emb.mean(axis=0, keepdims=True).astype("float32")

        # normalize centroid and members for cosine similarity
        centroid_norm = centroid / (np.linalg.norm(centroid, axis=1, keepdims=True) + 1e-8)
        members_norm = cluster_emb / (np.linalg.norm(cluster_emb, axis=1, keepdims=True) + 1e-8)

        sims = members_norm @ centroid_norm.T    # (Nc, 1)
        best_local = int(np.argmax(sims))
        best_idx = idxs[best_local]

        leaders[int(label)] = photo_ids[best_idx]

    with open(CLUSTER_LEADERS_FILE, "w") as f:
        json.dump(leaders, f, indent=2)

    print(f"[Leaders] Saved cluster leaders to {CLUSTER_LEADERS_FILE}")
    return leaders


def save_cluster_members(photo_ids, labels, unique_labels):
    """
    Save a JSON mapping: cluster_id -> [list of image names]
    """
    from pathlib import Path
    
    members = {}
    for label in unique_labels:
        idxs = np.where(labels == label)[0]
        # Extract just the filename from the full path
        members[str(label)] = [Path(photo_ids[i]).name for i in idxs]

    with open(CLUSTER_MEMBERS_FILE, "w") as f:
        json.dump(members, f, indent=2)

    print(f"[Members] Saved cluster members to {CLUSTER_MEMBERS_FILE}")



def main():
    print("\n=== STEP 1: Load embeddings ===")
    emb, photo_ids = load_embeddings_and_ids()

    print("\n=== STEP 2: Build FAISS index (optional) ===")
    build_faiss_index(emb)

    print("\n=== STEP 3: Run HAC clustering ===")
    labels, unique_labels = run_hac(emb, cosine_threshold=0.5)

    print("\n=== STEP 4: Leaders per cluster ===")
    leaders = compute_cluster_leaders(emb, photo_ids, labels, unique_labels)
    save_cluster_members(photo_ids, labels, unique_labels)

    print("\n=== SUMMARY ===")
    print(f"Total images: {emb.shape[0]}")
    print(f"Total clusters: {len(unique_labels)}")
    for label in unique_labels:
        print(f"Cluster {label}: leader = {leaders[int(label)]}")


if __name__ == "__main__":
    main()