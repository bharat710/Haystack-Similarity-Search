# incremental_update.py

import numpy as np
from pathlib import Path
from shutil import copy2

from get_similar_images import embed_image as embed_external
from faiss_cluster import run_hac, compute_cluster_leaders, build_faiss_index, save_cluster_members
from config import (
    DATASET_ROOT,
    EMBEDDINGS_FILE,
    PHOTO_IDS_FILE,
    CLUSTER_LABELS_FILE,
)

def load_state():
    if EMBEDDINGS_FILE.exists():
        emb = np.load(EMBEDDINGS_FILE).astype("float32")
    else:
        emb = np.empty((0, 512), dtype="float32")

    if PHOTO_IDS_FILE.exists():
        with open(PHOTO_IDS_FILE) as f:
            ids = [l.strip() for l in f]
    else:
        ids = []

    return emb, ids


def save_state(emb, ids):
    np.save(EMBEDDINGS_FILE, emb)
    with open(PHOTO_IDS_FILE, "w") as f:
        for pid in ids:
            f.write(pid + "\n")


def process_new_image(src_path: Path, dest_folder: Path = DATASET_ROOT):
    """
    Incremental pipeline:
    1. Copy image
    2. Embed
    3. Update embeddings
    4. Re-run HAC
    5. Compute leaders
    6. Rebuild FAISS
    """
    dest_folder.mkdir(parents=True, exist_ok=True)
    dest_path = dest_folder / src_path.name

    copy2(src_path, dest_path)
    print(f"\n[Upload] Copied --> {dest_path}")

    # Load state
    emb_all, ids_all = load_state()

    # Embed new image
    new_emb = embed_external(dest_path)

    if emb_all.size == 0:
        emb_all = new_emb
    else:
        emb_all = np.vstack([emb_all, new_emb])

    ids_all.append(str(dest_path))
    print(f"[Incremental] Total images = {len(ids_all)}")

    # Save updated state
    save_state(emb_all, ids_all)

    print("[HAC] Updating clusters...")
    labels, uniq = run_hac(emb_all, cosine_threshold=0.5)
    np.save(CLUSTER_LABELS_FILE, labels)

    compute_cluster_leaders(emb_all, ids_all, labels, uniq)
    save_cluster_members(ids_all, labels, uniq)
    build_faiss_index(emb_all)

    print(f"[Incremental] Updated: clusters={len(uniq)}, images={len(ids_all)}")
    return dest_path