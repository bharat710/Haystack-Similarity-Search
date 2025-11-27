# get_similar_images.py

from typing import Optional
from pathlib import Path

import numpy as np
from PIL import Image, ImageFile

import torch
import open_clip

from config import EMBEDDINGS_FILE, PHOTO_IDS_FILE, CLUSTER_LABELS_FILE

ImageFile.LOAD_TRUNCATED_IMAGES = True

device = "cuda" if torch.cuda.is_available() else "cpu"


def load_model_and_preprocess():
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-B-32",
        pretrained="laion2b_s34b_b79k"
    )
    model.eval()
    model.to(device)
    return model, preprocess


MODEL, PREPROCESS = load_model_and_preprocess()


def embed_image(path: Path) -> np.ndarray:
    img = Image.open(path)
    img.load()
    img = img.convert("RGB")
    x = PREPROCESS(img).unsqueeze(0).to(device)

    with torch.no_grad():
        feat = MODEL.encode_image(x)
        feat = feat / feat.norm(dim=-1, keepdim=True)
        feat = feat.cpu().numpy().astype("float32")
    return feat  # (1, d)


def load_embeddings_ids_labels():
    if not EMBEDDINGS_FILE.exists():
        raise FileNotFoundError(
            f"Missing embeddings: {EMBEDDINGS_FILE}"
        )
    if not PHOTO_IDS_FILE.exists():
        raise FileNotFoundError(
            f"Missing photo IDs: {PHOTO_IDS_FILE}"
        )
    if not CLUSTER_LABELS_FILE.exists():
        raise FileNotFoundError(
            f"Missing cluster labels: {CLUSTER_LABELS_FILE}"
        )

    emb = np.load(EMBEDDINGS_FILE).astype("float32")
    with open(PHOTO_IDS_FILE) as f:
        ids = [l.strip() for l in f]
    labels = np.load(CLUSTER_LABELS_FILE).astype("int64")

    if emb.shape[0] != len(ids) or emb.shape[0] != labels.shape[0]:
        raise RuntimeError(
            f"Shape mismatch: emb={emb.shape[0]}, ids={len(ids)}, labels={labels.shape[0]}"
        )

    id_to_index = {pid: i for i, pid in enumerate(ids)}
    return emb, ids, labels, id_to_index


def get_similar_images(input_image_path: str, top_k: Optional[int] = None):
    """
    Cluster-aware similar images:

    - If input_image_path is one of the uploaded images:
        * find its index
        * get its HAC cluster label
        * return all images in that cluster, sorted by cosine distance

    - If external:
        * embed it
        * find nearest neighbor in embedding space
        * use that neighbor's cluster label
        * return cluster members sorted by cosine distance
    """
    input_path = str(input_image_path)
    q_path = Path(input_path)

    emb_all, photo_ids, labels, id_to_index = load_embeddings_ids_labels()
    n, d = emb_all.shape

    # 1) Get query embedding + cluster label
    if input_path in id_to_index:
        q_idx = id_to_index[input_path]
        q_vec = emb_all[q_idx : q_idx + 1]  # (1, d)
        label = int(labels[q_idx])
        print(f"[Similar] Known photo; index={q_idx}, cluster={label}")
    else:
        print(f"[Similar] External photo; embedding: {q_path}")
        q_vec = embed_image(q_path)

        # nearest neighbor in global embedding space
        diff = emb_all - q_vec
        dists = np.sum(diff * diff, axis=1)
        nn_idx = int(np.argmin(dists))
        label = int(labels[nn_idx])
        print(
            f"[Similar] Assigned query to cluster {label} "
            f"via nearest neighbor {photo_ids[nn_idx]}"
        )

    # 2) Collect cluster members
    candidate_indices = np.where(labels == label)[0]
    if candidate_indices.size == 0:
        print("[Similar] No members found for this cluster; returning empty list.")
        return []

    # 3) Sort by cosine distance to query
    cluster_embs = emb_all[candidate_indices]      # (m, d)
    # q_vec is already L2-normalized, emb_all also normalized
    diff = cluster_embs - q_vec
    dists = np.sum(diff * diff, axis=1)           # smaller = closer

    order = np.argsort(dists)
    candidate_indices = candidate_indices[order]
    dists = dists[order]

    if top_k is not None:
        candidate_indices = candidate_indices[:top_k]
        dists = dists[:top_k]

    results = [
        (photo_ids[i], float(d))
        for i, d in zip(candidate_indices, dists)
    ]
    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python3 get_similar_images.py <path/to/image> [top_k]")
        raise SystemExit(1)

    image_path = sys.argv[1]
    top_k = int(sys.argv[2]) if len(sys.argv) >= 3 else None

    results = get_similar_images(image_path, top_k=top_k)

    print("\nCluster-aware similar images:")
    for p, d in results:
        print(f"{p}  (distance={d:.4f})")