"""Repository behavior over real asynchronous SQLite transactions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.models import (
    CompanyContact,
    ContactStatus,
    ContactType,
    CrawlRun,
    CrawlRunStatus,
    DataAvailability,
    LeadReview,
    OutreachChannel,
    OutreachDraft,
    Procurement,
    ProcurementEventType,
    ReviewDecision,
    SourceRecord,
)
from app.repositories.base import BaseRepository, pagination_bounds
from app.repositories.crawls import CrawlRunRepository, JobLeaseRepository
from app.repositories.leads import (
    CompanyContactRepository,
    LeadRepository,
    LeadReviewRepository,
    OutreachDraftRepository,
)
from app.repositories.procurements import (
    CompanyRepository,
    EvidenceRepository,
    ProcurementEventRepository,
    ProcurementRepository,
    ProcurementSourceRepository,
    SourceRecordRepository,
)

from .conftest import DatabaseContext
from .test_api_leads import _full_graph


@pytest.mark.asyncio
async def test_procurement_and_provenance_repositories(database: DatabaseContext) -> None:
    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        procurement_repo = ProcurementRepository(session)
        procurement = await procurement_repo.get_detail(ids.procurement)
        assert procurement is not None and len(procurement.sources) == 1
        assert (
            await procurement_repo.find_by_pncp_control_number(
                procurement.pncp_control_number or ""
            )
            == procurement
        )
        assert await procurement_repo.find_by_source("pncp", "api-graph-1") == procurement
        assert (
            await procurement_repo.resolve_canonical(
                source="other",
                external_id="other-1",
                agency_cnpj=procurement.agency_cnpj,
                uasg=procurement.uasg,
                purchase_number=procurement.purchase_number,
                purchase_year=procurement.purchase_year,
                modality=procurement.modality,
            )
            == procurement
        )
        page = await procurement_repo.list_filtered(
            uf="ma",
            municipality="Luís",
            agency="Prefeitura",
            modality="pregao_eletronico",
            published_from=ids.observed_at - timedelta(days=1),
            published_to=ids.observed_at + timedelta(days=1),
            status="em julgamento",
        )
        assert page.total == 1 and page.pages == 1
        assert (await procurement_repo.list_filtered(modality="---")).total == 0
        source = await ProcurementSourceRepository(session).find("pncp", "api-graph-1")
        assert source is not None

        record = SourceRecord(
            source="pncp",
            endpoint="https://pncp.gov.br/test",
            request_parameters={"page": 1},
            response_hash="b" * 64,
            raw_payload={"ok": True},
            http_status=200,
            availability=DataAvailability.AVAILABLE,
            collected_at=datetime.now(UTC),
            fingerprint="c" * 64,
        )
        session.add(record)
        await session.flush()
        found = await SourceRecordRepository(session).find_response(
            source="pncp", endpoint=record.endpoint, response_hash=record.response_hash
        )
        assert found == record
        evidence = await EvidenceRepository(session).get_trace(ids.evidence)
        assert evidence is not None and evidence.document is not None
        events = await ProcurementEventRepository(session).list_for_procurement(
            ids.procurement, event_type=ProcurementEventType.INELIGIBLE
        )
        assert len(events) == 1


@pytest.mark.asyncio
async def test_company_lead_contact_and_outreach_repositories(database: DatabaseContext) -> None:
    ids = await _full_graph(database)
    async with database.sessions() as session, session.begin():
        lead_repo = LeadRepository(session)
        lead = await lead_repo.get_detail(ids.lead)
        assert lead is not None
        page = await lead_repo.list_filtered(
            status=lead.lead_status,
            minimum_score=80,
            event_type=ProcurementEventType.INELIGIBLE,
            deadline_status=lead.deadline.status if lead.deadline else None,
            has_contact=False,
            pending_review=False,
            created_from=datetime.now(UTC) - timedelta(days=1),
            created_to=datetime.now(UTC) + timedelta(days=1),
        )
        assert page.total == 1
        company_repo = CompanyRepository(session)
        company = await company_repo.find_by_cnpj("00000000000191")
        assert company is not None
        assert len(await company_repo.find_exact_name_candidates("EMPRESA EXEMPLO")) == 1

        contact = CompanyContact(
            company_id=company.id,
            contact_type=ContactType.EMAIL,
            contact_value="licitacoes@empresa.example",
            source_url="https://empresa.example/contato",
            is_corporate=True,
            is_personal=False,
            confidence=Decimal("1"),
            status=ContactStatus.VERIFIED,
            fingerprint="d" * 64,
        )
        review = LeadReview(
            lead_id=lead.id,
            decision=ReviewDecision.APPROVED,
            reviewer="Teste",
        )
        draft = OutreachDraft(
            lead_id=lead.id,
            channel=OutreachChannel.EMAIL,
            subject="Assunto",
            message="Mensagem",
            generation_method="test",
            facts_hash="e" * 64,
            template_hash="f" * 64,
            approved=False,
            sent=False,
            fingerprint="0" * 64,
        )
        await CompanyContactRepository(session).add(contact)
        await LeadReviewRepository(session).add(review)
        await OutreachDraftRepository(session).add(draft)
        assert (
            await CompanyContactRepository(session).find_identity(
                company_id=company.id,
                contact_type="email",
                contact_value=contact.contact_value,
            )
            == contact
        )
        assert (
            await OutreachDraftRepository(session).find_idempotent(
                lead_id=lead.id,
                channel="email",
                facts_hash=draft.facts_hash,
                template_hash=draft.template_hash,
            )
            == draft
        )

        contacted = await lead_repo.list_filtered(has_contact=True)
        assert contacted.total == 1


@pytest.mark.asyncio
async def test_base_crawl_and_lease_repositories(database: DatabaseContext) -> None:
    assert pagination_bounds(1, 50) == (0, 50)
    with pytest.raises(ValueError):
        pagination_bounds(0, 10)
    with pytest.raises(ValueError):
        pagination_bounds(1, 501)

    async with database.sessions() as session, session.begin():
        base = BaseRepository(session, Procurement)
        procurement, created = await base.upsert_by_fingerprint(
            "9" * 64,
            {
                "source": "pncp",
                "external_id": "repo-base",
                "title": "Primeiro",
            },
        )
        assert created is True
        updated, created = await base.upsert_by_fingerprint(
            "9" * 64,
            {
                "source": "pncp",
                "external_id": "repo-base",
                "title": "Atualizado",
            },
        )
        assert created is False and updated.title == "Atualizado"
        assert await base.get_required(procurement.id) == procurement
        with pytest.raises(LookupError):
            await base.get_required("00000000-0000-0000-0000-000000000000")
        with pytest.raises(TypeError):
            await BaseRepository(session, CrawlRun).get_by_fingerprint("x")

        run = CrawlRun(connector="pncp", filters={})
        await CrawlRunRepository(session).add(run)
        now = datetime.now(UTC)
        crawl_repo = CrawlRunRepository(session)
        await crawl_repo.mark_running(run, started_at=now)
        await crawl_repo.finish(
            run,
            status=CrawlRunStatus.COMPLETED,
            finished_at=now,
            records_found=1,
            records_created=1,
            records_updated=0,
        )
        assert (await crawl_repo.list_recent(connector="pncp")).total == 1
        with pytest.raises(ValueError):
            await crawl_repo.finish(
                run,
                status=CrawlRunStatus.RUNNING,
                finished_at=now,
                records_found=0,
                records_created=0,
                records_updated=0,
            )

        leases = JobLeaseRepository(session)
        assert await leases.acquire(
            name="test-job",
            owner_id="one",
            now=now,
            expires_at=now + timedelta(minutes=5),
        )
        assert not await leases.acquire(
            name="test-job",
            owner_id="two",
            now=now,
            expires_at=now + timedelta(minutes=5),
        )
        assert await leases.renew(
            name="test-job",
            owner_id="one",
            now=now,
            expires_at=now + timedelta(minutes=10),
        )
        assert await leases.release(name="test-job", owner_id="one")
        assert not await leases.release(name="test-job", owner_id="one")

        await base.delete(procurement)
        assert await base.get(procurement.id) is None
