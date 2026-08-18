"""Testa o parsing defensivo do scraping contra variações de estrutura HTML
(schema drift simulado) — cada estratégia precisa se sustentar sozinha."""
import pytest

from dagster_project.parsing import ScrapingParseError, parse_precos_concorrentes

HTML_DATA_ATTRS = """
<html><body><div id="price-grid">
  <div class="price-card" data-product="Tenis Runner Pro" data-store="ConcorrenteA">
    <span class="pc-cat">Calcados</span>
    <span class="pc-price">354.69</span>
    <span class="pc-stock">Em estoque</span>
  </div>
  <div class="price-card" data-product="Sandalia Comfort" data-store="ConcorrenteB">
    <span class="pc-cat">Calcados</span>
    <span class="pc-price">131.08</span>
    <span class="pc-stock">Indisponivel</span>
  </div>
</div></body></html>
"""

HTML_TABLE = """
<html><body>
<table>
  <tr><th>produto</th><th>loja</th><th>categoria</th><th>preco</th></tr>
  <tr><td>Mochila Urban</td><td>ConcorrenteC</td><td>Acessorios</td><td>202,18</td></tr>
  <tr><td>Bone Classic</td><td>ConcorrenteA</td><td>Acessorios</td><td>58,68</td></tr>
</table>
</body></html>
"""

HTML_GENERIC_CARDS = """
<html><body>
  <div class="item"><h3>Meia Performance</h3><p>41.25</p><p>indisponivel</p></div>
  <div class="item"><h3>Tenis Casual Lite</h3><p>260.18</p><p>em estoque</p></div>
</body></html>
"""

HTML_UNPARSEABLE = "<html><body><div>estrutura totalmente inesperada, sem precos</div></body></html>"


def test_strategy_data_attrs_extracts_rows():
    rows, strategy = parse_precos_concorrentes(HTML_DATA_ATTRS)
    assert strategy == "_strategy_data_attrs"
    assert len(rows) == 2
    assert rows[0].produto == "Tenis Runner Pro"
    assert rows[0].loja == "ConcorrenteA"
    assert rows[0].categoria == "Calcados"
    assert rows[0].preco == 354.69
    assert rows[0].estoque_status == "em_estoque"
    assert rows[1].estoque_status == "indisponivel"


def test_strategy_table_fallback_extracts_rows():
    rows, strategy = parse_precos_concorrentes(HTML_TABLE)
    assert strategy == "_strategy_table"
    assert len(rows) == 2
    produtos = {r.produto for r in rows}
    assert produtos == {"Mochila Urban", "Bone Classic"}
    assert all(r.preco is not None for r in rows)


def test_strategy_generic_cards_fallback_extracts_rows():
    rows, strategy = parse_precos_concorrentes(HTML_GENERIC_CARDS)
    assert strategy == "_strategy_generic_cards"
    assert len(rows) == 2
    assert {r.preco for r in rows} == {41.25, 260.18}


def test_all_strategies_fail_raises_scraping_parse_error():
    with pytest.raises(ScrapingParseError):
        parse_precos_concorrentes(HTML_UNPARSEABLE)
