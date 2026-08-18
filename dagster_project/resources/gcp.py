"""Recursos Dagster para BigQuery e Google Cloud Storage."""
from __future__ import annotations

from google.cloud import bigquery, storage
from google.oauth2 import service_account

from dagster import ConfigurableResource

from dagster_project.settings import SETTINGS


def _credentials():
    return service_account.Credentials.from_service_account_file(
        str(SETTINGS.credentials_path)
    )


class BigQueryResource(ConfigurableResource):
    project: str = SETTINGS.gcp_project
    dataset: str = SETTINGS.bq_dataset
    location: str = SETTINGS.bq_location

    def get_client(self) -> bigquery.Client:
        return bigquery.Client(
            project=self.project,
            credentials=_credentials(),
            location=self.location,
        )

    def table_ref(self, table_name: str) -> str:
        return f"{self.project}.{self.dataset}.{table_name}"


class GCSResource(ConfigurableResource):
    project: str = SETTINGS.gcp_project
    bucket_name: str = SETTINGS.gcs_bucket

    def get_client(self) -> storage.Client:
        return storage.Client(project=self.project, credentials=_credentials())

    def get_bucket(self):
        return self.get_client().bucket(self.bucket_name)

    def upload_file(self, local_path: str, blob_path: str) -> str:
        bucket = self.get_bucket()
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_path)
        return f"gs://{self.bucket_name}/{blob_path}"

    def gs_uri(self, blob_prefix: str) -> str:
        return f"gs://{self.bucket_name}/{blob_prefix}"
