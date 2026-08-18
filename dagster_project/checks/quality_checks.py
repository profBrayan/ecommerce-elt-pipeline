"""Asset checks (observabilidade / qualidade) sobre os assets raw.

Cada check consulta a tabela raw recém-carregada no BigQuery (não confia só no
metadata do run em memória) para validar volume e nulos críticos, e reporta
como WARN — visível na UI do Dagster — sem derrubar o pipeline.
"""
from __future__ import annotations

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from dagster_project.assets.raw_pedidos import raw_pedidos
from dagster_project.assets.raw_precos import raw_precos_concorrentes
from dagster_project.assets.raw_sqlite import raw_clientes, raw_itens_pedido, raw_produtos
from dagster_project.io_helpers import today_partition
from dagster_project.resources.gcp import BigQueryResource

MIN_EXPECTED_PRECOS_ROWS = 5
EXPECTED_ITENS_PEDIDO_ROWS = 5_000_000
ITENS_PEDIDO_TOLERANCE = 0.001  # 0,1% de tolerância
MAX_NULL_RATE_PEDIDOS = 0.30  # o enunciado avisa que "alguns campos vêm nulos"


def _count_today(bigquery: BigQueryResource, table_name: str) -> int:
    client = bigquery.get_client()
    table = bigquery.table_ref(table_name)
    query = f"SELECT COUNT(*) AS n FROM `{table}` WHERE _ingestion_date = @dt"
    from google.cloud import bigquery as bq_lib

    job_config = bq_lib.QueryJobConfig(
        query_parameters=[
            bq_lib.ScalarQueryParameter("dt", "DATE", today_partition())
        ]
    )
    result = list(client.query(query, job_config=job_config).result())
    return int(result[0]["n"]) if result else 0


@asset_check(
    asset=raw_precos_concorrentes,
    description="Volume mínimo de linhas extraídas do scraping no dia",
)
def precos_row_count_check(bigquery: BigQueryResource) -> AssetCheckResult:
    count = _count_today(bigquery, "precos_concorrentes")
    passed = count >= MIN_EXPECTED_PRECOS_ROWS
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={"row_count": count, "min_expected": MIN_EXPECTED_PRECOS_ROWS},
    )


@asset_check(
    asset=raw_pedidos,
    description="Taxa de nulos em campos críticos (cliente_id, produto_id) dentro do esperado",
)
def pedidos_null_rate_check(bigquery: BigQueryResource) -> AssetCheckResult:
    client = bigquery.get_client()
    table = bigquery.table_ref("pedidos")
    query = f"""
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN cliente_id IS NULL THEN 1 ELSE 0 END) AS null_cliente,
          SUM(CASE WHEN produto_id IS NULL THEN 1 ELSE 0 END) AS null_produto
        FROM `{table}`
        WHERE _ingestion_date = DATE('{today_partition()}')
    """
    row = list(client.query(query).result())[0]
    total = row["total"] or 0
    null_rate = ((row["null_cliente"] or 0) + (row["null_produto"] or 0)) / (2 * total) if total else 1.0
    passed = null_rate <= MAX_NULL_RATE_PEDIDOS
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={"total_rows": total, "null_rate": round(null_rate, 4), "max_expected": MAX_NULL_RATE_PEDIDOS},
    )


@asset_check(
    asset=raw_itens_pedido,
    description="Volume carregado bate com o total de linhas do SQLite de origem (~5M)",
)
def itens_pedido_volume_check(bigquery: BigQueryResource) -> AssetCheckResult:
    count = _count_today(bigquery, "itens_pedido")
    lower_bound = EXPECTED_ITENS_PEDIDO_ROWS * (1 - ITENS_PEDIDO_TOLERANCE)
    passed = count >= lower_bound
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count, "expected": EXPECTED_ITENS_PEDIDO_ROWS},
    )


@asset_check(
    asset=raw_clientes,
    description="raw_clientes não perdeu linhas na ingestão (comparado ao SQLite de origem)",
)
def clientes_row_count_check(bigquery: BigQueryResource) -> AssetCheckResult:
    count = _count_today(bigquery, "clientes")
    passed = count > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
    )


@asset_check(
    asset=raw_produtos,
    description="raw_produtos não perdeu linhas na ingestão (comparado ao SQLite de origem)",
)
def produtos_row_count_check(bigquery: BigQueryResource) -> AssetCheckResult:
    count = _count_today(bigquery, "produtos")
    passed = count > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
    )
