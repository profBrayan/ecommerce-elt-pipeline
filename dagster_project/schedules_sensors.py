"""Job e schedule diários do pipeline completo (raw -> staging -> mart)."""
from __future__ import annotations

from dagster import ScheduleDefinition, define_asset_job

pipeline_job = define_asset_job(
    name="pipeline_saude_comercial",
    selection="*",
    description="Ingestão (API, scraping, SQLite) + staging + mart, ponta a ponta.",
)

daily_schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 6 * * *",
    execution_timezone="America/Sao_Paulo",
    description="Roda o pipeline completo todo dia às 06:00 (horário de Brasília).",
)
