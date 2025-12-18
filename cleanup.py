import shutil
import os

folders_to_delete = [
    "data/bronze",
    "data/silver",
    "data/gold",
    "data/checkpoints"
]

print("=== Cleanup Started ===\n")

for folder in folders_to_delete:
    if os.path.exists(folder):
        shutil.rmtree(folder)
        print(f" Deleted: {folder}")
    else:
        print(f" Not Found (skipped): {folder}")

print("\n=== Cleanup Completed ===")
