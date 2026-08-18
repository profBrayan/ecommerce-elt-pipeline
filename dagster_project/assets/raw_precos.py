"""Asset raw: scraping de preços de concorrentes, com parsing defensivo e
schema drift proposital na origem (ver dagster_project/parsing.py)."""
import dataclasses
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from dagster import AssetExecutionContext, MetadataValue, RetryPolicy, asset

from dagster_project.io_helpers import (
    INGESTION_DATE_FIELD,
    INGESTION_RUN_FIELD,
    load_parquet_prefix_to_bq,
    new_run_id,
    today_partition,
)
from dagster_project.parsing import ScrapingParseError, parse_precos_concorrentes
from dagster_project.resources.gcp import BigQueryResource, GCSResource
from dagster_project.settings import SETTINGS

# Requisito proposital: este asset pode falhar de verdade quando o parsing
# defensivo esgota todas as estratégias (ver parsing.py) — o RetryPolicy tenta
# de novo (a página muda de estrutura a cada request, então uma nova tentativa
# tem chance real de suceder); se todas as tentativas falharem, o asset falha e
# fica visível/alertável na UI do Dagster em vez de derrubar o pipeline inteiro.
@asset(
    group_name="raw",
    compute_kind="python",
    retry_policy=RetryPolicy(max_retries=4, delay=5),
)
def raw_precos_concorrentes(
    context: AssetExecutionContext, bigquery: BigQueryResource, gcs: GCSResource
) -> None:
    run_id = new_run_id()
    partition = today_partition()

    response = requests.get(SETTINGS.scraping_url, timeout=30)
    response.raise_for_status()
    html = response.text

    blob_prefix = f"raw/precos_concorrentes/dt={partition}/run_id={run_id}/"
    with tempfile.TemporaryDirectory() as tmp_dir:
        html_path = Path(tmp_dir) / "snapshot.html"
        html_path.write_text(html, encoding="utf-8")
        try:
            gcs.upload_file(str(html_path), f"{blob_prefix}snapshot.html")
        except Exception as exc:  # auditoria é best-effort, não pode derrubar o asset
            context.log.warning("Falha ao subir snapshot HTML de auditoria: %s", exc)

    try:
        rows, strategy_used = parse_precos_concorrentes(html)
    except ScrapingParseError as exc:
        context.log.error(
            "raw_precos_concorrentes: parsing falhou em todas as estratégias "
            "(run_id=%s). %s", run_id, exc,
        )
        raise

    records = []
    for row in rows:
        record = dataclasses.asdict(row)
        record[INGESTION_DATE_FIELD] = partition
        record[INGESTION_RUN_FIELD] = run_id
        records.append(record)

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = Path(tmp_dir) / "part-000.parquet"
        table = pa.Table.from_pylist(records)
        pq.write_table(table, local_path)
        gcs.upload_file(str(local_path), f"{blob_prefix}part-000.parquet")

    load_job = load_parquet_prefix_to_bq(
        bigquery, gcs, blob_prefix, table_name="precos_concorrentes"
    )

    context.log.info(
        "raw_precos_concorrentes: %s linhas extraídas via '%s', run_id=%s",
        len(records), strategy_used, run_id,
    )

    context.add_output_metadata(
        {
            "rows_extracted": len(records),
            "strategy_used": strategy_used,
            "bq_load_output_rows": load_job.output_rows,
            "run_id": run_id,
        }
    )
