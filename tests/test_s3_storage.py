from src.common.storage import S3Storage
import pandas as pd


def cleanup(storage: S3Storage, base_dir: str):
    print("\n[cleanup] deleting test objects...")

    prefix = storage._to_key(base_dir).rstrip("/") + "/"

    paginator = storage.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=storage.bucket, Prefix=prefix)

    keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            keys.append({"Key": obj["Key"]})

    if keys:
        storage.s3_client.delete_objects(
            Bucket=storage.bucket,
            Delete={"Objects": keys}
        )
        print(f"Deleted {len(keys)} objects")
    else:
        print("No objects to delete")


def main():
    print("=== S3 STORAGE TEST START ===")

    storage = S3Storage()
    test_root = "test_storage"

    # -------------------------
    # 1. JSONL WRITE
    # -------------------------
    print("\n[1] write_jsonl")
    jsonl_path = storage.join(test_root, "bronze/test/test.jsonl")

    records = [
        {"id": 1, "event": "test"},
        {"id": 2, "event": "test2"},
    ]

    print("Path:", jsonl_path)
    storage.write_jsonl(jsonl_path, records)

    # -------------------------
    # 2. JSONL READ
    # -------------------------
    print("\n[2] read_jsonl")
    print(storage.read_jsonl(jsonl_path))

    # -------------------------
    # 3. EXISTS
    # -------------------------
    print("\n[3] exists")
    print(storage.exists(jsonl_path))

    # -------------------------
    # 4. LIST PATHS
    # -------------------------
    print("\n[4] list_paths")
    print(storage.list_paths(storage.join(test_root, "bronze/test"), "*.jsonl"))

    # -------------------------
    # 5. PARQUET
    # -------------------------
    print("\n[5] parquet test")
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

    parquet_path = storage.join(test_root, "silver/test/test.parquet")
    storage.write_parquet(parquet_path, df)

    print(storage.read_parquet(parquet_path))

    # -------------------------
    # 6. DT PARTITION
    # -------------------------
    print("\n[6] dt partition")
    dt_path = storage.join(test_root, "gold/test/dt=2026-03-24/part-000.parquet")
    storage.write_parquet(dt_path, df)

    print(storage.list_paths(storage.join(test_root, "gold/test"), "dt=*"))

    # -------------------------
    # 7. CLEANUP
    # -------------------------
    cleanup(storage, test_root)

    print("\n=== DONE ===")


if __name__ == "__main__":
    main()