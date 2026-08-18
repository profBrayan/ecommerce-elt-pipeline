from __future__ import annotations

import os
import sys
from pathlib import Path

from dagster_dbt import DbtCliResource

from dagster import Definitions

from dagster_project.assets.dbt_assets import dbt_project, staging_mart_assets
from dagster_project.assets.raw_pedidos import raw_pedidos
from dagster_project.assets.raw_precos import raw_precos_concorrentes
from dagster_project.assets.raw_sqlite import raw_clientes, raw_itens_pedido, raw_produtos
from dagster_project.checks.quality_checks import (
    clientes_row_count_check,
    itens_pedido_volume_check,
    pedidos_null_rate_check,
    precos_row_count_check,
    produtos_row_count_check,
)
from dagster_project.resources.gcp import BigQueryResource, GCSResource
from dagster_project.schedules_sensors import daily_schedule, pipeline_job

# `dbt` pode não estar no PATH do processo (venv não "ativado") — aponta
# explicitamente para o executável instalado no mesmo venv do Dagster.
_DBT_EXECUTABLE = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")

defs = Definitions(
    assets=[
        raw_pedidos,
        raw_precos_concorrentes,
        raw_clientes,
        raw_produtos,
        raw_itens_pedido,
        staging_mart_assets,
    ],
    asset_checks=[
        precos_row_count_check,
        pedidos_null_rate_check,
        itens_pedido_volume_check,
        clientes_row_count_check,
        produtos_row_count_check,
    ],
    jobs=[pipeline_job],
    schedules=[daily_schedule],
    resources={
        "bigquery": BigQueryResource(),
        "gcs": GCSResource(),
        "dbt": DbtCliResource(
            project_dir=os.fspath(dbt_project.project_dir),
            profiles_dir=os.fspath(dbt_project.project_dir),
            dbt_executable=os.fspath(_DBT_EXECUTABLE),
        ),
    },
)
