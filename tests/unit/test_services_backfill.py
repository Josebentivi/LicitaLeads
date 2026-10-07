"""Unit tests for the raw-payload backfill parsing helpers."""

from __future__ import annotations

from app.services.ingestion.backfill import (
    _compras_rows,
    _observed_fields,
    _pncp_legal_basis,
    _pncp_rows,
    _row_identifiers,
)


def test_row_parsers_accept_paginated_flat_and_invalid_payloads() -> None:
    assert _pncp_rows({"data": [{"numeroControlePNCP": "x"}, "bad"]}) == [
        {"numeroControlePNCP": "x"}
    ]
    assert _pncp_rows({"numeroControlePNCP": "y"}) == [{"numeroControlePNCP": "y"}]
    assert _pncp_rows([{"a": 1}, 2]) == [{"a": 1}]
    assert _pncp_rows(None) == []
    assert _pncp_rows({"data": "not-a-list"}) == []

    assert _compras_rows({"resultado": [{"idCompra": "1"}, None]}) == [{"idCompra": "1"}]
    assert _compras_rows({"idCompra": "2"}) == [{"idCompra": "2"}]
    assert _compras_rows({"numeroControlePNCP": "c"}) == [{"numeroControlePNCP": "c"}]
    assert _compras_rows("text") == []


def test_observed_fields_read_literal_source_keys_only() -> None:
    assert _pncp_legal_basis({"amparoLegal": {"nome": "Objeto"}}) == "Objeto"
    assert _pncp_legal_basis({"amparoLegal": {"descricao": "Descrição"}}) == "Descrição"
    assert _pncp_legal_basis({"amparoLegal": "Texto"}) == "Texto"
    assert _pncp_legal_basis({"amparoLegalNome": "Plano"}) == "Plano"
    assert _pncp_legal_basis({}) is None

    assert _observed_fields("pncp", {"srp": True}) == {"is_srp": True, "legal_basis": None}
    assert _observed_fields("compras_gov", {"amparoLegalCodigoPncp": "C"}) == {
        "is_srp": None,
        "legal_basis": "C",
    }
    assert _observed_fields("compras_gov", {"amparoLegalDescricao": "D"})["legal_basis"] == "D"
    assert _observed_fields("desconhecida", {"srp": True}) == {}


def test_row_identifiers_fall_back_to_composite_and_source_keys() -> None:
    assert _row_identifiers("pncp", {"numeroControlePNCP": "abc"}) == ("ABC", None)
    assert _row_identifiers(
        "pncp",
        {"orgaoEntidade": {"cnpj": "123"}, "anoCompra": 2026, "sequencialCompra": 7},
    ) == (None, "123:2026:7")
    assert _row_identifiers("pncp", {}) == (None, None)
    assert _row_identifiers("compras_gov", {"idCompra": "98092105900012026"}) == (
        None,
        "98092105900012026",
    )
