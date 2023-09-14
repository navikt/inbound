import os
from pathlib import Path

from google.cloud import storage

os.environ["DATAPRODUCT_UI_BUCKET"] = "snowfront"
os.environ["DATAPRODUCT_UI_VERSION"] = "0.0.1"
os.environ["PROJECT_ID"] = "virksomhetsdatalaget-dev-30e3"

storage_client = storage_client = storage.Client(os.environ["PROJECT_ID"])
bucket = storage_client.get_bucket(os.environ["DATAPRODUCT_UI_BUCKET"])


def download_app():
    # create dirs and download files from GCS
    blobs = bucket.list_blobs()
    for blob in blobs:
        file = blob.name
        if not file.endswith("/"):
            dp = Path(
                Path.cwd() / "apps/frontend" / "/".join(blob.name.split("/")[2:-1])
            )
            if not dp.exists():
                dp.mkdir(parents=True, exist_ok=True)
            file_name = f"./apps/frontend/{'/'.join(blob.name.split('/')[2:])}"
            print("file_name: ", file_name)
            blob.download_to_filename(file_name)


# alt
# gsutil cp -r gs://mybucket/directory localDirectory

if __name__ == "__main__":
    download_app()
