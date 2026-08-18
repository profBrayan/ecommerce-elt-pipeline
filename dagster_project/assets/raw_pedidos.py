"""Asset raw: ingestão da API de vendas paginada (pedidos)."""
import tempfile
from pathlib import Path

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
from dagster_project.resources.http import SalesAPIClient

# Campos esperados do payload de cada pedido. Nulos/valores fora do formato
# esperado são preservados como estão — o tratamento acontece na staging (dbt),
# a raw layer só registra o que a API retornou.
_COLUMNS = [
    "pedido_id",
    "cliente_id",
    "produto_id",
    "quantidade",
    "valor_unitario",
    "status",
    "data_pedido",
    "updated_at",
]


@asset(
    group_name="raw",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=3, delay=10),
)
def raw_pedidos(
    context: AssetExecutionContext, bigquery: BigQueryResource, gcs: GCSResource
) -> None:
    """Extrai todos os pedidos da API paginada e carrega em `raw.pedidos`."""
    client = SalesAPIClient()
    run_id = new_run_id()
    partition = today_partition()

    rows: list[dict] = []
    pages_read = 0
    total_records_reported = None

    for payload in client.iter_pedidos():
        pages_read += 1
        total_records_reported = payload.get("total_records", total_records_reported)
        for item in payload.get("data", []):
            row = {col: item.get(col) for col in _COLUMNS}
            row[INGESTION_DATE_FIELD] = partition
            row[INGESTION_RUN_FIELD] = run_id
            rows.append(row)

    null_counts = {
        col: sum(1 for r in rows if r.get(col) is None) for col in _COLUMNS
    }

    blob_prefix = f"raw/pedidos/dt={partition}/run_id={run_id}/"
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / "part-000.parquet"
        table = pa.Table.from_pylist(rows)
        pq.write_table(table, local_path)
        gcs.upload_file(str(local_path), f"{blob_prefix}part-000.parquet")

    load_job = load_parquet_prefix_to_bq(
        bigquery, gcs, blob_prefix, table_name="pedidos"
    )

    context.log.info(
        "raw_pedidos: %s páginas lidas, %s linhas carregadas, run_id=%s",
        pages_read, len(rows), run_id,
    )

    context.add_output_metadata(
        {
            "rows_loaded": len(rows),
            "pages_read": pages_read,
            "total_records_api": total_records_reported,
            "null_counts": MetadataValue.json(null_counts),
            "bq_load_output_rows": load_job.output_rows,
            "run_id": run_id,
        }
    )
