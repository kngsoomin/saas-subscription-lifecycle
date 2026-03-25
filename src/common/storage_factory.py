from src.common.config import STORAGE_BACKEND
from src.common.storage import LocalStorage, S3Storage


def get_storage():
    if STORAGE_BACKEND == "s3":
        return S3Storage()
    return LocalStorage()