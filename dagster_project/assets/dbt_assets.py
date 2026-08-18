"""Camadas staging e mart, modeladas em dbt e expostas como assets do Dagster.

`DbtProject.prepare_if_dev()` gera o manifest automaticamente em desenvolvimento
(roda `dbt parse` se necessário); em produção o manifest deve ser gerado no build
da imagem. Os assets staging/mart dependem dos assets raw correspondentes via
`@dbt_assets` + sources do dbt mapeadas para os assets raw (ver DAGSTER_DBT_TRANSLATOR
em definitions.py, que liga `source('raw', 'x')` ao asset `raw_x`).
"""
from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from dagster import AssetExecutionContext
from dagster_project.dbt_translator import RawSourceDbtTranslator

DBT_PROJECT_DIR = Path(__file__).resolve().parent.parent.parent / "dbt_project"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=RawSourceDbtTranslator(),
)
def staging_mart_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
