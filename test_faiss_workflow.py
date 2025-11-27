# test_hac_incremental_pipeline.py

import os
import random
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent))

from incremental_update import process_new_image
from get_similar_images import get_similar_images
from config import DATASET_ROOT
import numpy as np


def list_local_images(folder: Path):
    exts = {".jpg", ".jpeg", ".png"}
    return sorted([folder / f for f in os.listdir(folder) if folder / f and (folder / f).suffix.lower() in exts])


def list_source_images(folder: Path):
    exts = {".jpg", ".jpeg", ".png"}
    paths = []
    for dirpath, _, filenames in os.walk(folder):
        for fname in filenames:
            if Path(fname).suffix.lower() in exts:
                paths.append(Path(dirpath) / fname)
    return sorted(paths)


def main():

    print("\n========== HAC INCREMENTAL PIPELINE TEST ==========\n")

    # Uploaded folder (working folder)
    uploaded_folder = DATASET_ROOT
    uploaded_folder.mkdir(parents=True, exist_ok=True)

    # Source dataset — from where test uploads come
    source_dataset = Path("/Users/bharat/Downloads/ALL")
    source_images = list_source_images(source_dataset)

    print(f"Found {len(source_images)} source images.\n")

    # Randomly sample 20 images
    if len(source_images) > 20:
        source_images = random.sample(source_images, 20)
        print(f"Randomly selected {len(source_images)} images for testing.\n")

    upload_count = 0

    for img_path in source_images:
        upload_count += 1
        print(f"\n===== UPLOAD #{upload_count} =====")
        new_path = process_new_image(img_path, uploaded_folder)

        # Every 10 uploaded images → run getSimilarImages
        if upload_count % 10 == 0:
            local_imgs = list_local_images(uploaded_folder)
            if not local_imgs:
                continue

            rand_img = random.choice(local_imgs)
            print(f"\n>>> Running getSimilarImages() on: {rand_img}")

            results = get_similar_images(str(rand_img), top_k=None)

            print("\nSimilar images in its HAC cluster:")
            for p, d in results[:20]:
                print(f"  {p} (distance={d:.4f})")

    print("\n===== TEST COMPLETE =====\n")


if __name__ == "__main__":
    main()