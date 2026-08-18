"""Configuração central do pipeline, lida de variáveis de ambiente (.env)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Variável de ambiente obrigatória '{name}' não definida. "
            f"Copie .env.example para .env e preencha os valores."
        )
    return value


@dataclass(frozen=True)
class Settings:
    gcp_project: str
    bq_dataset: str
    bq_location: str
    gcs_bucket: str
    google_application_credentials: str

    sales_api_base_url: str
    sales_api_token: str
    sales_api_page_size: int

    scraping_url: str

    sqlite_path: str

    @property
    def credentials_path(self) -> Path:
        path = Path(self.google_application_credentials)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        return path

    @property
    def sqlite_abs_path(self) -> Path:
        path = Path(self.sqlite_path)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        return path


def load_settings() -> Settings:
    return Settings(
        gcp_project=_require("GCP_PROJECT"),
        bq_dataset=_require("BQ_DATASET"),
        bq_location=os.environ.get("BQ_LOCATION", "southamerica-east1"),
        gcs_bucket=_require("GCS_BUCKET"),
        google_application_credentials=_require("GOOGLE_APPLICATION_CREDENTIALS"),
        sales_api_base_url=_require("SALES_API_BASE_URL"),
        sales_api_token=_require("SALES_API_TOKEN"),
        sales_api_page_size=int(os.environ.get("SALES_API_PAGE_SIZE", "500")),
        scraping_url=_require("SCRAPING_URL"),
        sqlite_path=os.environ.get("SQLITE_PATH", "data/banco_transacional.sqlite"),
    )


SETTINGS = load_settings()

# dbt (invocado como subprocesso pelo dagster-dbt) herda o ambiente do processo
# atual — garantimos aqui que o caminho da credencial seja absoluto, já que o
# dbt pode ser executado a partir de um cwd diferente (dbt_project/).
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SETTINGS.credentials_path)

