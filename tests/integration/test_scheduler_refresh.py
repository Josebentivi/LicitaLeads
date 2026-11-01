"""Refresh old active processes directly instead of relying on publication dates."""

from datetime import UTC, datetime, timedelta

from app.config import Settings
from app.jobs import scheduler
from app.models import Procurement, ProcurementPhase, ProcurementSource


async def test_refresh_rotates_oldest_active_source_ids(database, monkeypatch):
    settings = Settings(
        _env_file=None, default_uf="MA", active_refresh_batch_size=1, compras_recheck_batch_size=1
    )
    requests = []

    class Pipeline:
        def __init__(self, _settings):
            pass

        async def run(self, request):
            requests.append(request)

    monkeypatch.setattr(scheduler, "get_settings", lambda: settings)
    monkeypatch.setattr(scheduler, "async_session_factory", database.sessions)
    monkeypatch.setattr(scheduler, "IngestionPipeline", Pipeline)
    now = datetime.now(UTC)
    cases = [
        ("old-active", "MA", "Em julgamento", ProcurementPhase.JUDGMENT, 300),
        ("new-active", "MA", "Em julgamento", ProcurementPhase.JUDGMENT, 2),
        ("cancelled", "MA", "Cancelada", ProcurementPhase.JUDGMENT, 400),
        ("closed", "MA", "Homologada", ProcurementPhase.HOMOLOGATION, 400),
        ("outside-uf", "PI", "Em julgamento", ProcurementPhase.JUDGMENT, 400),
    ]
    async with database.sessions() as session, session.begin():
        for index, (identifier, uf, status, phase, age) in enumerate(cases):
            procurement = Procurement(
                source="pncp",
                external_id=identifier,
                uf=uf,
                status=status,
                current_phase=phase,
                publication_at=now - timedelta(days=400),
                fingerprint=str(index) * 64,
            )
            session.add(procurement)
            await session.flush()
            session.add(
                ProcurementSource(
                    procurement_id=procurement.id,
                    source="pncp",
                    external_id=identifier,
                    collected_at=now - timedelta(days=age),
                    fingerprint=str(index + 5) * 64,
                )
            )
    await scheduler.refresh_active_procurements()
    assert requests[0].discovery_kind == "update"
    direct = [request for request in requests if request.external_ids]
    assert len(direct) == 1
    assert direct[0].connector == "pncp"
    assert direct[0].external_ids == ("old-active",)
    # A failed lookup must not pin the oldest process at the front forever.
    # The fake pipeline deliberately never updates collected_at on success.
    await scheduler.refresh_active_procurements()
    direct = [request for request in requests if request.external_ids]
    assert direct[-1].external_ids == ("new-active",)
