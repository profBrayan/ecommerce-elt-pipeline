"""Sessão HTTP resiliente para a API de vendas: retry/backoff em 429 e 500."""
from __future__ import annotations

import logging
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from dagster_project.settings import SETTINGS

logger = logging.getLogger(__name__)


class RetryableAPIError(Exception):
    """Erro que justifica retry (429 rate limit ou 5xx intermitente)."""

    def __init__(self, status_code: int, message: str, retry_after: float | None = None):
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(message)


def _is_retryable(exc: BaseException) -> bool:
    return isinstance(exc, (RetryableAPIError, requests.exceptions.ConnectionError,
                             requests.exceptions.Timeout))


def _wait_strategy(retry_state):
    """Em 429, respeita o header Retry-After enviado pelo servidor (a API usa
    uma janela fixa, não um limite aleatório — medido empiricamente: 30
    requests liberadas, depois 429 com Retry-After constante até a janela
    resetar). Para outros erros retryable (5xx, timeout/conexão), usa
    exponential backoff com jitter."""
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if isinstance(exc, RetryableAPIError) and exc.retry_after:
        return exc.retry_after + 2  # pequena margem de segurança
    return wait_exponential_jitter(initial=1, max=30)(retry_state)


class SalesAPIClient:
    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update(
            {"Authorization": f"Bearer {SETTINGS.sales_api_token}"}
        )
        self._base_url = SETTINGS.sales_api_base_url

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=_wait_strategy,
        stop=stop_after_attempt(8),
        reraise=True,
    )
    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        response = self._session.get(f"{self._base_url}{path}", params=params, timeout=30)
        if response.status_code == 429:
            retry_after_header = response.headers.get("Retry-After")
            retry_after = float(retry_after_header) if retry_after_header and retry_after_header.isdigit() else None
            logger.warning(
                "Rate limit (429) atingido, aguardando %ss (Retry-After). params=%s",
                retry_after, params,
            )
            raise RetryableAPIError(429, "rate limited", retry_after=retry_after)
        if response.status_code >= 500:
            logger.warning(
                "Falha intermitente (%s) na API de vendas, retry. params=%s",
                response.status_code, params,
            )
            raise RetryableAPIError(response.status_code, "server error")
        response.raise_for_status()
        return response.json()

    def iter_pedidos(self, page_size: int | None = None):
        """Itera todas as páginas de /api/pedidos, uma página por vez (lazy)."""
        page_size = page_size or SETTINGS.sales_api_page_size
        page = 1
        while True:
            payload = self._get("/api/pedidos", {"page": page, "page_size": page_size})
            yield payload
            if not payload.get("has_next"):
                break
            page += 1
