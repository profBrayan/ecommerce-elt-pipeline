"""Regressão: a API de vendas mistura float e string ("462.99 BRL") no mesmo
campo `valor_unitario` entre registros diferentes — descoberto rodando contra
o serviço real (ver scripts/diagnose_pedidos_types.py). Isso quebra
pyarrow.Table.from_pylist quando o campo não é normalizado para um único tipo
antes de virar Parquet."""
import datetime as dt

import pyarrow as pa

from dagster_project.assets.raw_pedidos import normalize_pedido_row


def test_valor_unitario_misto_normalizado_para_string():
    partition = dt.date(2026, 8, 18)
    row_float = normalize_pedido_row(
        {"pedido_id": 1, "valor_unitario": 151.3}, partition, "run1"
    )
    row_sujo = normalize_pedido_row(
        {"pedido_id": 2, "valor_unitario": "462.99 BRL"}, partition, "run1"
    )

    assert row_float["valor_unitario"] == "151.3"
    assert row_sujo["valor_unitario"] == "462.99 BRL"
    assert isinstance(row_float["valor_unitario"], str)
    assert isinstance(row_sujo["valor_unitario"], str)


def test_valor_unitario_nulo_permanece_nulo():
    row = normalize_pedido_row({"pedido_id": 3, "valor_unitario": None}, dt.date(2026, 8, 18), "run1")
    assert row["valor_unitario"] is None


def test_lote_misto_nao_quebra_pyarrow():
    """O teste real: um lote com os dois formatos não deve levantar ArrowTypeError."""
    partition = dt.date(2026, 8, 18)
    rows = [
        normalize_pedido_row({"pedido_id": 1, "valor_unitario": 151.3}, partition, "run1"),
        normalize_pedido_row({"pedido_id": 2, "valor_unitario": "462.99 BRL"}, partition, "run1"),
        normalize_pedido_row({"pedido_id": 3, "valor_unitario": None}, partition, "run1"),
    ]
    table = pa.Table.from_pylist(rows)
    assert table.num_rows == 3
