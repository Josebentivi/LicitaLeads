"""Sidebar navigation model shared by templates and group-hub pages."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NavigationItem:
    label: str
    href: str


@dataclass(frozen=True, slots=True)
class NavigationGroup:
    id: str
    label: str
    href: str
    icon: str
    eyebrow: str
    description: str
    items: tuple[NavigationItem, ...] = ()
    hub: bool = True


NAV_GROUPS: tuple[NavigationGroup, ...] = (
    NavigationGroup(
        id="visao-geral",
        label="Visão geral",
        href="/dashboard",
        icon="dashboard",
        eyebrow="Visão geral",
        description="Panorama operacional e oportunidades recentes.",
        hub=False,
    ),
    NavigationGroup(
        id="compras",
        label="Compras públicas",
        href="/grupos/compras",
        icon="compras",
        eyebrow="Compras públicas",
        description="Contratações, atas de registro de preços e contratos oficiais.",
        items=(
            NavigationItem("Contratações", "/procurements"),
            NavigationItem("Atas (ARP)", "/atas"),
            NavigationItem("Contratos", "/contratos"),
        ),
    ),
    NavigationGroup(
        id="financeiro",
        label="Execução financeira",
        href="/grupos/financeiro",
        icon="financeiro",
        eyebrow="Execução financeira",
        description="Pagamentos federais e municipais, conciliação e proveniência.",
        items=(NavigationItem("Pagamentos", "/pagamentos"),),
    ),
    NavigationGroup(
        id="oportunidades",
        label="Empresas e oportunidades",
        href="/grupos/oportunidades",
        icon="oportunidades",
        eyebrow="Empresas e oportunidades",
        description="Empresas monitoradas, participações, leads e revisão humana.",
        items=(
            NavigationItem("Empresas", "/empresas"),
            NavigationItem("Leads", "/leads"),
        ),
    ),
    NavigationGroup(
        id="operacao",
        label="Operação de dados",
        href="/grupos/operacao",
        icon="operacao",
        eyebrow="Operação de dados",
        description="Coletas, qualidade das fontes e saúde do scheduler.",
        items=(
            NavigationItem("Coletas", "/crawls"),
            NavigationItem("Fontes", "/source-capabilities"),
        ),
    ),
    NavigationGroup(
        id="sistema",
        label="Sistema",
        href="/grupos/sistema",
        icon="sistema",
        eyebrow="Sistema",
        description="Configuração local, manutenção e ajuda operacional.",
        items=(
            NavigationItem("Configurações", "/settings"),
            NavigationItem("Testes", "/testes"),
            NavigationItem("Ajuda", "/ajuda"),
        ),
    ),
)

_BY_ID = {group.id: group for group in NAV_GROUPS}


def get_group(group_id: str) -> NavigationGroup | None:
    return _BY_ID.get(group_id)


def active_group_id(path: str) -> str | None:
    """Return the navigation group that owns ``path``."""

    path = path.rstrip("/") or "/"
    if path.startswith("/grupos/"):
        candidate = path.split("/", 3)[2] if len(path.split("/", 3)) > 2 else ""
        if candidate in _BY_ID and _BY_ID[candidate].hub:
            return candidate
    for group in NAV_GROUPS:
        if path == group.href:
            return group.id
        if any(path == item.href or path.startswith(f"{item.href}/") for item in group.items):
            return group.id
    return None


def active_item_href(path: str) -> str | None:
    """Return the child item matching ``path``, including detail pages."""

    path = path.rstrip("/") or "/"
    for group in NAV_GROUPS:
        for item in group.items:
            if path == item.href or path.startswith(f"{item.href}/"):
                return item.href
    return None


def breadcrumbs(path: str) -> list[dict[str, str]]:
    """Build the two/three-level breadcrumb shown above each page."""

    result = [{"label": "Início", "href": "/dashboard"}]
    group = _BY_ID.get(active_group_id(path) or "")
    if group is None or group.id == "visao-geral":
        return result
    result.append({"label": group.label, "href": group.href})
    item_href = active_item_href(path)
    if item_href:
        item = next(item for item in group.items if item.href == item_href)
        if path != item.href:
            result.append({"label": item.label, "href": item.href})
    return result
