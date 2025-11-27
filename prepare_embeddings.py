# prepare_embeddings.py

import os
from pathlib import Path

import numpy as np
from PIL import Image, ImageFile
from tqdm import tqdm

import torch
import open_clip

from config import DATASET_ROOT, OUTPUT_DIR, EMBEDDINGS_FILE, PHOTO_IDS_FILE

ImageFile.LOAD_TRUNCATED_IMAGES = True

device = "cuda" if torch.cuda.is_available() else "cpu"


def list_image_paths(root: Path):
    exts = {".jpg", ".jpeg", ".png"}
    paths = []
    for dirpath, _, filenames in os.walk(root):
        for fname in filenames:
            if Path(fname).suffix.lower() in exts:
                paths.append(Path(dirpath) / fname)
    return sorted(paths)


def build_model_and_preprocess():
    model, _, preprocess = open_clip.create_model_and_transforms(
        "ViT-B-32",
        pretrained="laion2b_s34b_b79k"
    )
    model.eval()
    model.to(device)
    return model, preprocess


def extract_embeddings(image_paths, batch_size=32):
    model, preprocess = build_model_and_preprocess()

    all_embs = []
    photo_ids = []

    with torch.no_grad():
        for i in tqdm(range(0, len(image_paths), batch_size), desc="Embedding"):
            batch_paths = image_paths[i : i + batch_size]
            imgs = []
            for p in batch_paths:
                img = Image.open(p)
                img.load()
                img = img.convert("RGB")
                imgs.append(preprocess(img))

            x = torch.stack(imgs).to(device)   # [B,3,224,224]
            feats = model.encode_image(x)      # [B,512]
            feats = feats / feats.norm(dim=-1, keepdim=True)
            feats = feats.cpu().numpy().astype("float32")

            all_embs.append(feats)
            photo_ids.extend([str(p) for p in batch_paths])

    embeddings = np.vstack(all_embs)
    return embeddings, photo_ids


def main():
    print(f"Using device: {device}")
    print(f"Scanning dataset root: {DATASET_ROOT}")

    image_paths = list_image_paths(DATASET_ROOT)
    print(f"Found {len(image_paths)} images")
    if not image_paths:
        raise RuntimeError("No images found under DATASET_ROOT")

    embeddings, photo_ids = extract_embeddings(image_paths)

    print(f"Embeddings shape: {embeddings.shape}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    np.save(EMBEDDINGS_FILE, embeddings)

    with open(PHOTO_IDS_FILE, "w") as f:
        for pid in photo_ids:
            f.write(pid + "\n")

    print(f"Saved embeddings to {EMBEDDINGS_FILE}")
    print(f"Saved photo_ids to {PHOTO_IDS_FILE}")


if __name__ == "__main__":
    main()