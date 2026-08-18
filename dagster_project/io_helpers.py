"""Helpers compartilhados para carregar arquivos do GCS no BigQuery (raw layer)."""
from __future__ import annotations

import datetime as dt
import uuid

from google.cloud import bigquery

from dagster_project.resources.gcp import BigQueryResource, GCSResource

INGESTION_DATE_FIELD = "_ingestion_date"
INGESTION_RUN_FIELD = "_ingestion_run_id"


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


def today_partition() -> dt.date:
    """Data de ingestão como `date` de verdade — precisa ser DATE/TIMESTAMP no
    Parquet (não string) para o BigQuery aceitar como coluna de particionamento."""
    return dt.date.today()


def today_partition_str() -> str:
    return today_partition().isoformat()


def load_parquet_prefix_to_bq(
    bq: BigQueryResource,
    gcs: GCSResource,
    blob_prefix: str,
    table_name: str,
    partition_field: str | None = INGESTION_DATE_FIELD,
    write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
) -> bigquery.LoadJob:
    """Carrega todos os arquivos parquet sob um prefixo do GCS para uma tabela do BigQuery.

    Idempotência: quando `partition_field` é informado, a tabela é particionada por
    essa coluna (data de ingestão) e o load usa WRITE_TRUNCATE apenas na partição do
    dia corrente (via `table$YYYYMMDD` decorator), então re-rodar no mesmo dia
    substitui os dados daquele dia em vez de duplicar.
    """
    client = bq.get_client()
    table_ref = bq.table_ref(table_name)
    uri = gcs.gs_uri(f"{blob_prefix}*.parquet")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=write_disposition,
    )

    destination = table_ref
    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition_field)
        partition_suffix = today_partition().strftime("%Y%m%d")
        destination = f"{table_ref}${partition_suffix}"
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    load_job = client.load_table_from_uri(uri, destination, job_config=job_config)
    load_job.result()
    return load_job
