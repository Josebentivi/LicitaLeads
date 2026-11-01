"""Sidebar navigation and breadcrumb derivation."""

from __future__ import annotations

from app.navigation import active_group_id, active_item_href, breadcrumbs, get_group


def test_navigation_groups_and_active_paths() -> None:
    assert active_group_id("/dashboard") == "visao-geral"
    assert active_group_id("/procurements/123") == "compras"
    assert active_group_id("/grupos/compras") == "compras"
    assert active_group_id("/pagamentos") == "financeiro"
    assert active_group_id("/empresas/00000000000191") == "oportunidades"
    assert active_group_id("/source-capabilities") == "operacao"
    assert active_group_id("/settings") == "sistema"
    assert active_group_id("/testes") == "sistema"
    assert active_group_id("/desconhecido") is None
    assert active_item_href("/contratos/abc") == "/contratos"
    assert active_item_href("/testes") == "/testes"
    assert active_item_href("/grupos/compras") is None


def test_breadcrumbs_for_hub_list_and_detail() -> None:
    assert [item["label"] for item in breadcrumbs("/dashboard")] == ["Início"]
    assert [item["label"] for item in breadcrumbs("/grupos/compras")] == [
        "Início",
        "Compras públicas",
    ]
    assert [item["label"] for item in breadcrumbs("/procurements/123")] == [
        "Início",
        "Compras públicas",
        "Contratações",
    ]
    assert [item["label"] for item in breadcrumbs("/testes")][-1] == "Sistema"
    assert get_group("financeiro") is not None
    assert get_group("inexistente") is None
