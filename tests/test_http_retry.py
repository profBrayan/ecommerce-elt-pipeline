"""Testa retry/backoff da API de vendas em 429 e 500, sem bater na API real."""
from unittest.mock import MagicMock, patch

from dagster_project.resources.http import SalesAPIClient


def _make_response(status_code: int, json_body: dict | None = None, headers: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body or {}
    resp.raise_for_status.return_value = None
    resp.headers = headers or {}
    return resp


@patch("dagster_project.resources.http.SETTINGS")
def test_retries_on_429_then_succeeds(mock_settings):
    """Servidor real usa Retry-After fixo (medido empiricamente: 30 requests
    liberadas, depois 429 com Retry-After constante). Usamos '0' aqui só para
    o teste não esperar segundos reais — o comportamento de honrar o header é
    o que importa, não o valor exato."""
    mock_settings.sales_api_base_url = "https://fake"
    mock_settings.sales_api_token = "token"
    mock_settings.sales_api_page_size = 100

    client = SalesAPIClient()
    ok_body = {"data": [], "has_next": False}
    responses = [
        _make_response(429, headers={"Retry-After": "0"}),
        _make_response(429, headers={"Retry-After": "0"}),
        _make_response(200, ok_body),
    ]

    with patch.object(client._session, "get", side_effect=responses) as mock_get:
        result = client._get("/api/pedidos", {"page": 1, "page_size": 100})

    assert result == ok_body
    assert mock_get.call_count == 3


@patch("dagster_project.resources.http.SETTINGS")
def test_retries_on_500_then_succeeds(mock_settings):
    mock_settings.sales_api_base_url = "https://fake"
    mock_settings.sales_api_token = "token"
    mock_settings.sales_api_page_size = 100

    client = SalesAPIClient()
    ok_body = {"data": [{"pedido_id": 1}], "has_next": False}
    responses = [_make_response(500), _make_response(200, ok_body)]

    with patch.object(client._session, "get", side_effect=responses) as mock_get:
        result = client._get("/api/pedidos", {"page": 1, "page_size": 100})

    assert result == ok_body
    assert mock_get.call_count == 2


@patch("dagster_project.resources.http.SETTINGS")
def test_iter_pedidos_stops_at_has_next_false(mock_settings):
    mock_settings.sales_api_base_url = "https://fake"
    mock_settings.sales_api_token = "token"
    mock_settings.sales_api_page_size = 2

    client = SalesAPIClient()
    page1 = _make_response(200, {"data": [{"pedido_id": 1}], "has_next": True})
    page2 = _make_response(200, {"data": [{"pedido_id": 2}], "has_next": False})

    with patch.object(client._session, "get", side_effect=[page1, page2]) as mock_get:
        pages = list(client.iter_pedidos(page_size=2))

    assert len(pages) == 2
    assert mock_get.call_count == 2
