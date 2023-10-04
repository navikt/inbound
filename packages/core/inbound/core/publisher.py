import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from google.cloud import storage

from inbound.core import settings
from inbound.core.logging import LOGGER
from inbound.core.settings import Settings


def publish_metadata(id: str, paths: List[str], bucket: str, run_id: str = None) -> str:
    for path in paths:
        upload_metadata_to_gcs(id, path, bucket, run_id)
    return "DONE"


def upload_metadata_to_gcs(id: str, path: str, run_id: str = None) -> str:
    LOGGER.info(f"Upload metadata {path} uploading to {name}.")

    settings = Settings()

    res = "FAILED"

    full_path = Path.cwd() / Path(path).resolve()

    name = f'{id.replace(".","_")}/{path.lower()}'

    if run_id:
        name = f'{id.replace(".","_")}/{run_id}/{path.lower()}'

    if Path(full_path).is_file():
        LOGGER.info(f"Metadata file {full_path} uploading to {name}.")
        try:
            # using GOOGLE_APPLICATION_CREDENTIALS
            storage_client = storage.Client()

            bucket = storage_client.get_bucket(settings.spec.gcp.metadatabucket)
            # upload current run
            blob = bucket.blob(name)
            blob.upload_from_filename(full_path)
            res = "DONE"
        except Exception as e:
            LOGGER.debug(f"Error uploading metadata file {full_path} to {name}. {e}")
    else:
        LOGGER.debug(
            f"Error uploading metadata file {full_path} to {name}. The file could not be found"
        )

    return res
