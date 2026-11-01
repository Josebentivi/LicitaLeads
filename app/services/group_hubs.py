"""Aggregated data for the operator group hub pages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.connectors.municipal_payments.sources import get_municipal_source
from app.connectors.payment_sources import list_payment_sources
from app.models import (
    Company,
    Contract,
    CrawlRun,
    CrawlRunStatus,
    Deadline,
    DeadlineStatus,
    Lead,
    LeadStatus,
    Payment,
    PriceRegistry,
    Procurement,
)
from app.navigation import NavigationGroup, get_group
from app.repositories.procurements import ProcurementRepository
from app.services.ingestion.municipal_payments import latest_municipal_runs
from app.services.maintenance import COUNT_TABLE_LABELS, data_counts
from app.services.scheduler_control import scheduler_status


@dataclass(frozen=True, slots=True)
class HubMetric:
    label: str
    value: str | int
    note: str | None = None
    href: str | None = None


@dataclass(frozen=True, slots=True)
class HubAction:
    title: str
    description: str
    href: str
    label: str = "Abrir"
    count: int | None = None
    tone: str = "default"


@dataclass(frozen=True, slots=True)
class HubAlert:
    title: str
    message: str
    href: str | None = None
    label: str = "Ver"
    severity: str = "info"


@dataclass(frozen=True, slots=True)
class HubRow:
    title: str
    subtitle: str | None = None
    meta: str | None = None
    href: str | None = None
    status: str | None = None
    status_class: str | None = None


@dataclass(frozen=True, slots=True)
class GroupHub:
    group: NavigationGroup
    primary_action: HubAction | None
    metrics: tuple[HubMetric, ...]
    actions: tuple[HubAction, ...]
    alerts: tuple[HubAlert, ...]
    recent_title: str | None = None
    recent_rows: tuple[HubRow, ...] = ()
    recent_empty: str = "Nenhum registro recente."
    show_crawl_form: bool = False
    selected_uf: str = "MA"


def _currency(value: Decimal | None) -> str:
    if value is None:
        return "Não informado"
    formatted = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {formatted}"


def _local_datetime(value: datetime | None) -> str:
    if value is None:
        return "Não informado"
    local = value.astimezone(ZoneInfo(get_settings().timezone))
    return local.strftime("%d/%m/%Y %H:%M")


def _scalar_int(value: int | None) -> int:
    return int(value or 0)


async def _count(session: AsyncSession, statement) -> int:
    return _scalar_int(await session.scalar(statement))


async def build_group_hub(session: AsyncSession, group_id: str) -> GroupHub | None:
    group = get_group(group_id)
    if group is None or not group.hub:
        return None
    if group.id == "compras":
        return await _build_compras(session, group)
    if group.id == "financeiro":
        return await _build_financeiro(session, group)
    if group.id == "oportunidades":
        return await _build_oportunidades(session, group)
    if group.id == "operacao":
        return await _build_operacao(session, group)
    if group.id == "sistema":
        return await _build_sistema(session, group)
    return None


async def _build_compras(session: AsyncSession, group: NavigationGroup) -> GroupHub:
    now = datetime.now(UTC)
    repository = ProcurementRepository(session)
    open_page = await repository.list_filtered(
        page=1, page_size=1, status_category="aberta", now=now
    )
    total = await _count(session, select(func.count(Procurement.id)))
    atas = await _count(
        session,
        select(func.count(PriceRegistry.id)).where(
            PriceRegistry.valid_until >= now,
            or_(PriceRegistry.valid_from.is_(None), PriceRegistry.valid_from <= now),
        ),
    )
    contracts = await _count(
        session,
        select(func.count(Contract.id)).where(Contract.valid_until >= now),
    )
    recent = await repository.list_filtered(page=1, page_size=5, status_category="aberta", now=now)
    rows: list[HubRow] = []
    for procurement in recent.items:
        process = (
            f"{procurement.purchase_number}/{procurement.purchase_year}"
            if procurement.purchase_number and procurement.purchase_year
            else procurement.purchase_number
        )
        title = procurement.title or procurement.object_description or process or "Contratação"
        subtitle = " · ".join(
            value
            for value in (
                procurement.agency_name,
                procurement.municipality,
                procurement.uf,
            )
            if value
        )
        rows.append(
            HubRow(
                title=title,
                subtitle=subtitle or "Órgão não informado",
                meta=(
                    f"Propostas até {_local_datetime(procurement.proposal_end_at)}"
                    if procurement.proposal_end_at
                    else "Sem data final de propostas"
                ),
                href=f"/procurements/{procurement.id}",
                status=procurement.status or procurement.current_phase.value,
                status_class=procurement.current_phase.value,
            )
        )
    failed = await _count(
        session,
        select(func.count(CrawlRun.id)).where(
            CrawlRun.status == CrawlRunStatus.FAILED,
            CrawlRun.created_at >= now - timedelta(hours=24),
        ),
    )
    alerts: list[HubAlert] = []
    if failed:
        alerts.append(
            HubAlert(
                title="Coletas com falha",
                message=f"{failed} coleta(s) falharam nas últimas 24 horas.",
                href="/crawls",
                label="Revisar coletas",
                severity="danger",
            )
        )
    return GroupHub(
        group=group,
        primary_action=HubAction(
            title="Contratações abertas",
            description="Ver processos que ainda podem receber propostas.",
            href="/procurements?status_category=aberta",
            label="Abrir contratações",
        ),
        metrics=(
            HubMetric("Contratações abertas", open_page.total, "Podem receber proposta"),
            HubMetric("Atas vigentes", atas, "Com vigência ativa"),
            HubMetric("Contratos vigentes", contracts, "Com vigência ativa"),
            HubMetric("Processos monitorados", total, "Base histórica"),
        ),
        actions=(
            HubAction(
                "Contratações",
                "Filtrar processos por UF, modalidade, fase e valor.",
                "/procurements?status_category=aberta",
                "Ver contratações",
                open_page.total,
            ),
            HubAction(
                "Atas de registro de preços",
                "Fornecedores, itens, saldos e adesões publicados.",
                "/atas",
                "Ver atas",
                atas,
            ),
            HubAction(
                "Contratos",
                "Contratos, itens, aditivos e pagamentos vinculados.",
                "/contratos",
                "Ver contratos",
                contracts,
            ),
        ),
        alerts=tuple(alerts),
        recent_title="Contratações abertas recentes",
        recent_rows=tuple(rows),
        recent_empty="Nenhuma contratação aberta no momento.",
    )


async def _build_financeiro(session: AsyncSession, group: NavigationGroup) -> GroupHub:
    settings = get_settings()
    payments = await _count(
        session, select(func.count(Payment.id)).where(Payment.is_current.is_(True))
    )
    unlinked = await _count(
        session,
        select(func.count(Payment.id)).where(
            Payment.is_current.is_(True), Payment.requires_manual_review.is_(True)
        ),
    )
    municipal = await _count(
        session,
        select(func.count(Payment.id)).where(
            Payment.is_current.is_(True), Payment.source.like("municipal:%")
        ),
    )
    sources = list_payment_sources(settings)
    configured_sources = sum(1 for source in sources if source.configured)
    latest_runs = await latest_municipal_runs(session)
    failed_sources: list[str] = []
    for source_id, run in latest_runs.items():
        if run.status is CrawlRunStatus.FAILED:
            try:
                failed_sources.append(get_municipal_source(source_id).name)
            except ValueError:
                failed_sources.append(source_id)
    recent = list(
        (
            await session.scalars(
                select(Payment)
                .where(Payment.is_current.is_(True), Payment.requires_manual_review.is_(True))
                .order_by(Payment.created_at.desc(), Payment.id)
                .limit(5)
            )
        ).all()
    )
    rows = tuple(
        HubRow(
            title=payment.supplier_name or "Fornecedor não informado",
            subtitle=" · ".join(
                value
                for value in (payment.source, payment.document_number, payment.agency_name)
                if value
            ),
            meta=f"{_currency(payment.amount)} · {_local_datetime(payment.payment_date)}",
            href="/pagamentos",
            status="Não conciliado",
            status_class="status-partial",
        )
        for payment in recent
    )
    alerts: list[HubAlert] = []
    if unlinked:
        alerts.append(
            HubAlert(
                title="Conciliação pendente",
                message=f"{unlinked} pagamento(s) sem contrato conciliado.",
                href="/pagamentos",
                label="Revisar pagamentos",
                severity="warning",
            )
        )
    if failed_sources:
        alerts.append(
            HubAlert(
                title="Portais municipais indisponíveis",
                message="Falha na última coleta: " + ", ".join(failed_sources) + ".",
                href="/pagamentos#portais",
                label="Ver fontes",
                severity="warning",
            )
        )
    return GroupHub(
        group=group,
        primary_action=HubAction(
            title="Importar pagamentos",
            description="Importe CSV/JSON de um portal municipal.",
            href="/pagamentos#importar",
            label="Importar arquivo",
        ),
        metrics=(
            HubMetric("Pagamentos", payments, "Documentos financeiros"),
            HubMetric("Não conciliados", unlinked, "Exigem revisão humana"),
            HubMetric("Municipais", municipal, "Origem municipal"),
            HubMetric("Fontes configuradas", configured_sources, "Federais, manuais e municipais"),
        ),
        actions=(
            HubAction(
                "Importar exportação",
                "CSV ou JSON do portal, com proveniência por linha.",
                "/pagamentos#importar",
                "Importar",
            ),
            HubAction(
                "Coletar portais municipais",
                "Executar uma fonte e registrar a resposta bruta.",
                "/pagamentos#portais",
                "Escolher fonte",
            ),
            HubAction(
                "Ver pagamentos",
                "Filtrar por fornecedor, fase, fonte e contrato.",
                "/pagamentos",
                "Abrir pagamentos",
                payments,
            ),
        ),
        alerts=tuple(alerts),
        recent_title="Pagamentos aguardando revisão",
        recent_rows=rows,
        recent_empty="Nenhum pagamento pendente de revisão.",
    )


async def _build_oportunidades(session: AsyncSession, group: NavigationGroup) -> GroupHub:
    from app.services.participation_integrity import deadline_current, lead_current

    leads_total = await _count(session, select(func.count(Lead.id)).where(lead_current()))
    pending = await _count(
        session,
        select(func.count(Lead.id)).where(
            Lead.lead_status == LeadStatus.PENDING_REVIEW, lead_current()
        ),
    )
    companies = await _count(session, select(func.count(Company.id)))
    due_24h = await _count(
        session,
        select(func.count(Deadline.id)).where(
            Deadline.status == DeadlineStatus.DUE_WITHIN_24H, deadline_current()
        ),
    )
    recent = list(
        (
            await session.scalars(
                select(Lead)
                .where(lead_current())
                .options(
                    selectinload(Lead.company),
                    selectinload(Lead.triggering_event),
                    selectinload(Lead.deadline),
                )
                .order_by(Lead.score.desc(), Lead.created_at.desc())
                .limit(5)
            )
        ).all()
    )
    rows = tuple(
        HubRow(
            title=lead.company.legal_name or lead.company.normalized_name,
            subtitle=f"Score {lead.score} · {lead.lead_status.value}",
            meta=(
                f"Prazo: {lead.deadline.status.value}"
                if lead.deadline is not None
                else "Sem prazo vinculado"
            ),
            href=f"/leads/{lead.id}",
            status=lead.triggering_event.event_type.value,
            status_class=(
                "status-partial"
                if lead.triggering_event.requires_manual_review
                else "status-completed"
            ),
        )
        for lead in recent
    )
    alerts: list[HubAlert] = []
    if pending:
        alerts.append(
            HubAlert(
                title="Leads aguardando revisão",
                message=f"{pending} lead(s) precisam de decisão humana.",
                href="/leads?lead_status=pending_review",
                label="Revisar fila",
                severity="warning",
            )
        )
    if due_24h:
        alerts.append(
            HubAlert(
                title="Prazos em 24 horas",
                message=f"{due_24h} prazo(s) vencem nas próximas 24 horas.",
                href="/leads",
                label="Ver leads",
                severity="danger",
            )
        )
    return GroupHub(
        group=group,
        primary_action=HubAction(
            title="Revisar leads",
            description="Avaliar oportunidades e decidir o próximo passo.",
            href="/leads",
            label="Abrir leads",
        ),
        metrics=(
            HubMetric("Leads", leads_total, "Oportunidades registradas"),
            HubMetric("Revisão pendente", pending, "Decisão humana"),
            HubMetric("Empresas", companies, "Consolidadas por CNPJ"),
            HubMetric("Prazos em 24h", due_24h, "Atenção imediata"),
        ),
        actions=(
            HubAction(
                "Leads",
                "Fila por score, evento, prazo e revisão.",
                "/leads",
                "Ver leads",
                leads_total,
            ),
            HubAction(
                "Empresas",
                "Participações, derrotas documentadas e contatos.",
                "/empresas",
                "Ver empresas",
                companies,
            ),
            HubAction(
                "Prazos e revisão",
                "Priorize o que vence e o que ainda precisa de decisão.",
                "/leads?requires_review=true",
                "Abrir pendências",
                pending,
            ),
        ),
        alerts=tuple(alerts),
        recent_title="Leads prioritários",
        recent_rows=rows,
        recent_empty="Nenhum lead registrado.",
    )


async def _build_operacao(session: AsyncSession, group: NavigationGroup) -> GroupHub:
    now = datetime.now(UTC)
    active = await _count(
        session,
        select(func.count(CrawlRun.id)).where(
            CrawlRun.status.in_([CrawlRunStatus.PENDING, CrawlRunStatus.RUNNING])
        ),
    )
    failed = await _count(
        session,
        select(func.count(CrawlRun.id)).where(
            CrawlRun.status == CrawlRunStatus.FAILED,
            CrawlRun.created_at >= now - timedelta(hours=24),
        ),
    )
    last_run = await session.scalar(select(CrawlRun).order_by(CrawlRun.created_at.desc()).limit(1))
    scheduler = await scheduler_status(session)
    recent = list(
        (
            await session.scalars(select(CrawlRun).order_by(CrawlRun.created_at.desc()).limit(5))
        ).all()
    )
    rows = tuple(
        HubRow(
            title=run.connector,
            subtitle=f"Status: {run.status.value}",
            meta=f"{run.records_found} registro(s) · {_local_datetime(run.created_at)}",
            href="/crawls",
            status=run.status.value,
            status_class=f"status-{run.status.value}",
        )
        for run in recent
    )
    alerts: list[HubAlert] = []
    if failed:
        alerts.append(
            HubAlert(
                title="Falhas recentes",
                message=f"{failed} coleta(s) falharam nas últimas 24 horas.",
                href="/crawls",
                label="Revisar falhas",
                severity="danger",
            )
        )
    if scheduler.get("paused"):
        alerts.append(
            HubAlert(
                title="Scheduler pausado",
                message="As coletas automáticas estão suspensas.",
                href="/settings",
                label="Abrir configurações",
                severity="warning",
            )
        )
    return GroupHub(
        group=group,
        primary_action=HubAction(
            title="Ver coletas",
            description="Acompanhar execuções, falhas e cancelamentos.",
            href="/crawls",
            label="Abrir coletas",
        ),
        metrics=(
            HubMetric("Coletas ativas", active, "Pendentes ou em andamento"),
            HubMetric("Falhas em 24h", failed, "Fonte indisponível ou erro"),
            HubMetric(
                "Última coleta", _local_datetime(last_run.created_at) if last_run else "Nunca"
            ),
            HubMetric(
                "Scheduler",
                "Pausado" if scheduler.get("paused") else "Ativo",
                "Processo separado",
            ),
        ),
        actions=(
            HubAction(
                "Coletas",
                "Histórico, progresso e repetição de fontes com falha.",
                "/crawls",
                "Ver coletas",
                active,
            ),
            HubAction(
                "Fontes",
                "Matriz de capacidades e limites de cada fonte.",
                "/source-capabilities",
                "Ver fontes",
            ),
            HubAction(
                "Configurações",
                "Pausar/retomar o scheduler e revisar contratos e pagamentos.",
                "/settings",
                "Abrir configurações",
            ),
        ),
        alerts=tuple(alerts),
        recent_title="Coletas recentes",
        recent_rows=rows,
        recent_empty="Nenhuma coleta executada.",
        show_crawl_form=True,
        selected_uf=get_settings().default_uf,
    )


async def _build_sistema(session: AsyncSession, group: NavigationGroup) -> GroupHub:
    settings = get_settings()
    public = settings.public_view()
    scheduler = await scheduler_status(session)
    counts = await data_counts(session)
    total = sum(counts.values())
    rows = tuple(
        HubRow(
            title=COUNT_TABLE_LABELS.get(table, table),
            subtitle=table,
            meta=f"{value} registro(s)",
            href="/settings",
        )
        for table, value in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:5]
        if value
    )
    alerts: list[HubAlert] = []
    if scheduler.get("paused"):
        alerts.append(
            HubAlert(
                title="Coletas automáticas pausadas",
                message="O scheduler está ativo, mas os jobs estão sendo ignorados.",
                href="/settings",
                label="Retomar",
                severity="warning",
            )
        )
    return GroupHub(
        group=group,
        primary_action=HubAction(
            title="Configurações",
            description="Revisar ambiente, scheduler e manutenção.",
            href="/settings",
            label="Abrir configurações",
        ),
        metrics=(
            HubMetric(
                "Scheduler",
                "Pausado" if scheduler.get("paused") else "Ativo",
                "Coletas automáticas",
            ),
            HubMetric("Banco", str(public.get("database_backend", "sqlite")), "Backend local"),
            HubMetric("Registros", total, "Dados armazenados"),
            HubMetric("Ambiente", str(public.get("app_env", "development")), "Execução atual"),
        ),
        actions=(
            HubAction(
                "Configurações",
                "Ambiente, scheduler, limites e zona de manutenção.",
                "/settings",
                "Abrir configurações",
            ),
            HubAction(
                "Testes",
                "Diagnóstico da aplicação e suíte completa em banco temporário.",
                "/testes",
                "Abrir testes",
            ),
            HubAction(
                "Ajuda",
                "Fluxo operacional, revisão de leads e importação.",
                "/ajuda",
                "Abrir ajuda",
            ),
            HubAction(
                "Fontes",
                "O que cada fonte publica e o que exige documento.",
                "/source-capabilities",
                "Ver matriz",
            ),
        ),
        alerts=tuple(alerts),
        recent_title="Dados armazenados",
        recent_rows=rows,
        recent_empty="Nenhum dado coletado.",
    )
