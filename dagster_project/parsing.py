"""Parsing defensivo do HTML de preços de concorrentes.

A página sofre "schema drift" propositalmente entre execuções (classes, tags e
estrutura mudam). Este módulo tenta múltiplas estratégias, da mais específica
(estrutura observada hoje) até fallbacks genéricos, e só desiste (levanta
`ScrapingParseError`) se nenhuma delas conseguir extrair nada — sinal para o
asset acionar retry/alerta em vez de quebrar o pipeline.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from bs4 import BeautifulSoup

_PRICE_RE = re.compile(r"\d{1,3}(?:[.,]\d{3})*[.,]\d{2}")
_OUT_OF_STOCK_KEYWORDS = ("indispon", "esgotad", "sem estoque")
_IN_STOCK_KEYWORDS = ("em estoque", "dispon")


@dataclass
class PrecoConcorrente:
    produto: str
    loja: str | None
    categoria: str | None
    preco: float | None
    estoque_status: str | None


class ScrapingParseError(Exception):
    """Nenhuma estratégia de parsing conseguiu extrair linhas válidas."""


def _parse_price(text: str) -> float | None:
    match = _PRICE_RE.search(text)
    if not match:
        return None
    raw = match.group(0)
    if "," in raw and "." in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_stock(text: str) -> str | None:
    lowered = text.lower()
    if any(kw in lowered for kw in _OUT_OF_STOCK_KEYWORDS):
        return "indisponivel"
    if any(kw in lowered for kw in _IN_STOCK_KEYWORDS):
        return "em_estoque"
    return None


_CAT_CLASS_RE = re.compile(r"cat", re.IGNORECASE)


def _find_categoria(el) -> str | None:
    """Best-effort: procura um filho cuja classe sugira 'categoria' (ex: 'pc-cat').
    Se a estrutura mudou e essa dica não existir, simplesmente retorna None —
    campo opcional, não impede a extração do restante da linha."""
    child = el.find(class_=_CAT_CLASS_RE)
    if child:
        text = child.get_text(strip=True)
        return text or None
    return None


def _strategy_data_attrs(soup: BeautifulSoup) -> list[PrecoConcorrente]:
    """Estratégia 1: qualquer elemento com data-product (independente de tag/classe)."""
    results: list[PrecoConcorrente] = []
    for el in soup.find_all(attrs={"data-product": True}):
        produto = el.get("data-product", "").strip()
        if not produto:
            continue
        loja = el.get("data-store")
        text = el.get_text(" ", strip=True)
        results.append(
            PrecoConcorrente(
                produto=produto,
                loja=loja,
                categoria=_find_categoria(el),
                preco=_parse_price(text),
                estoque_status=_parse_stock(text),
            )
        )
    return results


def _strategy_table(soup: BeautifulSoup) -> list[PrecoConcorrente]:
    """Estratégia 2: fallback genérico de <table>, heurística por conteúdo da célula."""
    results: list[PrecoConcorrente] = []
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
            if not cells:
                continue
            row_text = " ".join(cells)
            preco = _parse_price(row_text)
            if preco is None:
                continue
            produto = cells[0]
            results.append(
                PrecoConcorrente(
                    produto=produto,
                    loja=cells[1] if len(cells) > 1 else None,
                    categoria=cells[2] if len(cells) > 2 else None,
                    preco=preco,
                    estoque_status=_parse_stock(row_text),
                )
            )
    return results


def _strategy_generic_cards(soup: BeautifulSoup) -> list[PrecoConcorrente]:
    """Estratégia 3: qualquer container repetido que tenha um preço reconhecível
    e algum texto de título próximo (heurística mais frouxa, último recurso antes
    de desistir)."""
    results: list[PrecoConcorrente] = []
    candidates = soup.find_all(["div", "li", "article"], recursive=True)
    for el in candidates:
        text = el.get_text(" ", strip=True)
        preco = _parse_price(text)
        if preco is None:
            continue
        # evita capturar containers pais que englobam múltiplos cards/preços
        if len(_PRICE_RE.findall(text)) != 1:
            continue
        titulo = el.get("title") or el.get("data-product")
        if not titulo:
            first_tag = el.find(["h1", "h2", "h3", "h4", "span", "strong"])
            titulo = first_tag.get_text(strip=True) if first_tag else None
        if not titulo:
            continue
        results.append(
            PrecoConcorrente(
                produto=titulo,
                loja=el.get("data-store"),
                categoria=_find_categoria(el),
                preco=preco,
                estoque_status=_parse_stock(text),
            )
        )
    return results


_STRATEGIES = (_strategy_data_attrs, _strategy_table, _strategy_generic_cards)


def parse_precos_concorrentes(html: str) -> tuple[list[PrecoConcorrente], str]:
    """Retorna (linhas, nome_da_estrategia_usada). Levanta ScrapingParseError se
    nenhuma estratégia extrair nenhuma linha."""
    soup = BeautifulSoup(html, "html.parser")
    for strategy in _STRATEGIES:
        rows = strategy(soup)
        if rows:
            return rows, strategy.__name__
    raise ScrapingParseError(
        "Nenhuma estratégia de parsing conseguiu extrair linhas do HTML recebido "
        "(possível mudança de estrutura fora do previsto)."
    )
