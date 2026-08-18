"""Assets raw a partir do banco transacional SQLite.

`raw_clientes` e `raw_produtos` são pequenos e carregados de uma vez. `raw_itens_pedido`
tem 5M+ linhas — lido em batches via cursor.fetchmany() e escrito em Parquet
incrementalmente, sem nunca materializar a tabela inteira em memória (nem em pandas).
"""
import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dagster import AssetExecutionContext, MetadataValue, RetryPolicy, asset

from dagster_project.io_helpers import (
    INGESTION_DATE_FIELD,
    INGESTION_RUN_FIELD,
    load_parquet_prefix_to_bq,
    new_run_id,
    today_partition,
)
from dagster_project.resources.gcp import BigQueryResource, GCSResource
from dagster_project.settings import SETTINGS
from dagster_project.sqlite_utils import iter_sqlite_batches

ITENS_PEDIDO_BATCH_SIZE = 200_000


def _load_small_table_asset(
    context: AssetExecutionContext,
    bigquery: BigQueryResource,
    gcs: GCSResource,
    table_name: str,
    bq_table_name: str,
) -> None:
    run_id = new_run_id()
    partition = today_partition()

    conn = sqlite3.connect(str(SETTINGS.sqlite_abs_path))
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()

    df[INGESTION_DATE_FIELD] = partition
    df[INGESTION_RUN_FIELD] = run_id

    blob_prefix = f"raw/{bq_table_name}/dt={partition}/run_id={run_id}/"
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / "part-000.parquet"
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, local_path)
        gcs.upload_file(str(local_path), f"{blob_prefix}part-000.parquet")

    load_job = load_parquet_prefix_to_bq(bigquery, gcs, blob_prefix, table_name=bq_table_name)

    context.log.info("raw_%s: %s linhas carregadas, run_id=%s", bq_table_name, len(df), run_id)
    context.add_output_metadata(
        {
            "rows_loaded": len(df),
            "bq_load_output_rows": load_job.output_rows,
            "run_id": run_id,
            "null_counts": MetadataValue.json(df.isna().sum().to_dict()),
        }
    )


@asset(group_name="raw", compute_kind="sqlite")
def raw_clientes(
    context: AssetExecutionContext, bigquery: BigQueryResource, gcs: GCSResource
) -> None:
    _load_small_table_asset(context, bigquery, gcs, "clientes", "clientes")


@asset(group_name="raw", compute_kind="sqlite")
def raw_produtos(
    context: AssetExecutionContext, bigquery: BigQueryResource, gcs: GCSResource
) -> None:
    _load_small_table_asset(context, bigquery, gcs, "produtos", "produtos")


@asset(
    group_name="raw",
    compute_kind="sqlite",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
)
def raw_itens_pedido(
    context: AssetExecutionContext, bigquery: BigQueryResource, gcs: GCSResource
) -> None:
    """Lê itens_pedido (5M+ linhas) em batches, nunca carregando a tabela inteira
    em memória: cada batch vira um arquivo Parquet próprio, subido ao GCS assim
    que é escrito; um único load job do BigQuery lê todo o prefixo no final."""
    run_id = new_run_id()
    partition = today_partition()
    blob_prefix = f"raw/itens_pedido/dt={partition}/run_id={run_id}/"

    total_rows = 0
    batch_index = 0
    with tempfile.TemporaryDirectory() as tmp_dir:
        for records in iter_sqlite_batches(
            str(SETTINGS.sqlite_abs_path), "itens_pedido", ITENS_PEDIDO_BATCH_SIZE
        ):
            for record in records:
                record[INGESTION_DATE_FIELD] = partition
                record[INGESTION_RUN_FIELD] = run_id

            local_path = Path(tmp_dir) / f"part-{batch_index:04d}.parquet"
            table = pa.Table.from_pylist(records)
            pq.write_table(table, local_path)
            gcs.upload_file(
                str(local_path), f"{blob_prefix}part-{batch_index:04d}.parquet"
            )
            local_path.unlink()  # libera disco local imediatamente após o upload

            total_rows += len(records)
            batch_index += 1
            context.log.info(
                "raw_itens_pedido: batch %s enviado (%s linhas, total acumulado %s)",
                batch_index, len(records), total_rows,
            )

    load_job = load_parquet_prefix_to_bq(
        bigquery, gcs, blob_prefix, table_name="itens_pedido"
    )

    context.log.info(
        "raw_itens_pedido: %s linhas em %s batches, run_id=%s",
        total_rows, batch_index, run_id,
    )
    context.add_output_metadata(
        {
            "rows_loaded": total_rows,
            "batches": batch_index,
            "batch_size": ITENS_PEDIDO_BATCH_SIZE,
            "bq_load_output_rows": load_job.output_rows,
            "run_id": run_id,
        }
    )
