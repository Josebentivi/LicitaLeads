"""Audited comparison of São Luís pregões from PNCP, SINC-Contrata and Compras.gov.br."""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.connectors._common import normalized_text, to_int
from app.connectors.base import (
    ConnectorResult,
    ProcurementFilters,
    RawProcurement,
    RawSourceRecord,
)
from app.connectors.base import DataAvailability as ConnectorAvailability
from app.connectors.compras_gov import ComprasGovConnector
from app.connectors.http import AsyncHTTPClient
from app.connectors.pncp import PNCPConnector, parse_pncp_external_id
from app.connectors.tce_ma import SincContrataResource, SincContrataRow, TCEMAConnector
from app.database import create_database_engine, create_session_factory
from app.models import Base, CrawlRun, SourceRecord
from app.models.enums import CrawlRunStatus, DataAvailability
from app.services.event_detection.authority import nonaward_supplier
from app.services.identifiers import is_valid_cnpj, normalize_cnpj
from app.services.parallel import bounded_map

ENTE = 2111300
CNPJ = "06307102000130"
UASG = "980921"
PILOT_ITEM = 1
ORIGIN_HOST = "cnetmobile.estaleiro.serpro.gov.br"
ORIGIN_PATH = "/comprasnet-web/public/compras/acompanhamento-compra"
_CONTROL = re.compile(r"[A-Z0-9]{14}-1-[0-9]{6}/[0-9]{4}")
_JUDGMENT = re.compile(r"\b(?:ata|termo|relatorio) (?:de |do )?julgamento\b|^julgamento$")
_INCOMPLETE = {"temporary_error", "access_restricted", "not_supported"}


def _value(value: Any) -> str:
    return str(value).strip() if isinstance(value, (str, int)) else ""


def _cnpj(value: Any) -> str | None:
    string = _value(value)
    return normalize_cnpj(string) if is_valid_cnpj(string) else None


def _control(value: Any) -> str | None:
    string = _value(value).upper()
    return string if _CONTROL.fullmatch(string) and _cnpj(string[:14]) else None


def _origin_key(url: Any, *, item: bool = False) -> str | None:
    """Accept the published purchase link or its canonical public destination."""
    try:
        parsed = urlsplit(_value(url))
        if parsed.scheme != "https" or parsed.netloc != ORIGIN_HOST:
            return None
    except ValueError:
        return None
    paths = (
        {f"{ORIGIN_PATH}/item/{PILOT_ITEM}"}
        if item
        else {ORIGIN_PATH, "/comprasnet-web/public/landing"}
    )
    if parsed.path.rstrip("/") not in paths or parsed.fragment:
        return None
    query = parse_qs(parsed.query)
    if set(query) - {"compra", "destino"}:
        return None
    if parsed.path.endswith("/landing") and query.get("destino") != ["acompanhamento-compra"]:
        return None
    keys = query.get("compra", [])
    return keys[0] if len(keys) == 1 and re.fullmatch(r"[0-9]{17}", keys[0]) else None


class OriginObservation(BaseModel):
    """Facts transcribed from the platform's public view, with its exact URL."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    controle_pncp: str = Field(pattern=_CONTROL.pattern, max_length=28)
    controle_exibido: str | None = Field(default=None, max_length=28)
    url: str = Field(max_length=500)
    url_item: str | None = Field(default=None, max_length=500)
    observado_em: AwareDatetime
    situacao: str | None = Field(default=None, max_length=150)
    sessao_exibida: str | None = Field(default=None, max_length=80)
    propostas_visiveis: int | None = Field(default=None, ge=0)
    cnpjs: list[str] = Field(default_factory=list, max_length=1000)
    rotulos: list[str] = Field(default_factory=list, max_length=30)
    termo_julgamento: bool | None = None
    termo_homologacao: bool | None = None
    arquivo_julgamento_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    arquivo_julgamento_paginas: int | None = Field(default=None, ge=1)
    diagnostico: str | None = Field(default=None, max_length=200)
    item_diagnostico: str | None = Field(default=None, max_length=200)

    @field_validator("cnpjs")
    @classmethod
    def valid_cnpjs(cls, values: list[str]) -> list[str]:
        return sorted({cnpj for value in values if (cnpj := _cnpj(value))})


_TCE_FIELDS = frozenset(
    {
        "id",
        "enteIdEnvio",
        "cnpjProcedimento",
        "idProcedimento",
        "idContratacaoPncp",
        "numeroProcedimento",
        "anoProcedimento",
        "tipoProcedimento",
        "nomeTipoProcedimento",
        "numeroProcesso",
        "anoProcesso",
        "dataPublicacao",
        "dataSessao",
        "dataEnvio",
        "statusMuralStr",
        "statusNivel1",
        "statusNivel2",
        "tipoResultado",
        "nomeTipoResultado",
        "dataResultado",
        "idContrato",
        "numeroContrato",
        "anoContrato",
        "dataAssinatura",
        "valor",
        "link_documento",
        "licitante",
        "contratado",
        "vencedor",
    }
)
_SUPPLIER_FIELDS = frozenset({"licitante", "contratado", "niFornecedor", "codFornecedor"})
_PERSON_FIELDS = frozenset({"nomeFornecedor", "nomeRazaoSocialFornecedor"})


def project_tce_row(row: dict[str, Any]) -> dict[str, Any]:
    """Keep procurement evidence and corporate identifiers from the municipal inventory."""
    result = {key: row[key] for key in _TCE_FIELDS & row.keys()}
    for key in _SUPPLIER_FIELDS & result.keys():
        result[key] = _cnpj(result[key])
    return result


def _snapshot(raw: RawSourceRecord) -> Any:
    """Keep the fields used in the comparison and omit personal identifiers."""

    def row_snapshot(row: Any) -> Any:
        if not isinstance(row, dict):
            return None
        if raw.source == "tce_ma":
            return project_tce_row(row)
        result = {key: row[key] for key in row.keys() - _PERSON_FIELDS}
        for key in _SUPPLIER_FIELDS & result.keys():
            result[key] = _cnpj(result[key])
        return result

    payload = raw.raw_payload
    if isinstance(payload, list):
        return [row_snapshot(row) for row in payload]
    if isinstance(payload, dict):
        for key in (
            "content",
            "data",
            "resultado",
            "resultados",
            "itens",
            "documentos",
            "listaResultados",
        ):
            if isinstance(payload.get(key), list):
                return {**payload, key: [row_snapshot(row) for row in payload[key]]}
        return row_snapshot(payload)
    return None


class _Archive:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], run_id: UUID) -> None:
        self.sessions = sessions
        self.run_id = run_id
        self.references: dict[str, str] = {}

    async def save(
        self, result: ConnectorResult[Any], *, attempt: RawSourceRecord | None = None
    ) -> dict[str, Any]:
        ids = []
        async with self.sessions.begin() as session:
            for raw in result.raw_records or ([attempt] if attempt else []):
                key = raw.record_key
                if key not in self.references:
                    entity = SourceRecord(
                        crawl_run_id=self.run_id,
                        source=raw.source,
                        endpoint=raw.endpoint,
                        request_parameters=raw.request_parameters,
                        raw_payload=_snapshot(raw),
                        response_hash=raw.response_hash,
                        http_status=raw.http_status or None,
                        availability=DataAvailability(result.availability.value),
                        diagnostic=result.diagnostic,
                        collected_at=raw.collected_at,
                        collection_complete=result.is_complete,
                        pagination_metadata=result.pagination.model_dump()
                        if result.pagination
                        else None,
                        fingerprint=hashlib.sha256(f"{self.run_id}:{key}".encode()).hexdigest(),
                    )
                    session.add(entity)
                    await session.flush()
                    self.references[key] = str(entity.id)
                ids.append(self.references[key])
        return {
            "disponibilidade": result.availability.value,
            "parcial": result.partial,
            "diagnostico": result.diagnostic,
            "registros_fonte": ids,
        }


def _attempt(source: str, endpoint: str, params: dict[str, Any]) -> RawSourceRecord:
    return RawSourceRecord.build(
        source=source,
        endpoint=endpoint,
        request_parameters=params,
        raw_payload=None,
        http_status=0,
        collected_at=datetime.now(UTC),
    )


def sinc_procedure_key(row: SincContrataRow) -> tuple[str, str] | None:
    cnpj = _cnpj(row.payload.get("cnpjProcedimento"))
    identity = _value(row.payload.get("idProcedimento"))
    return (cnpj, identity) if cnpj and identity else None


def sinc_row_matches(row: SincContrataRow, key: tuple[str, str], control: str) -> bool:
    """A related record may omit the control, but it cannot point elsewhere."""
    asserted = _value(row.payload.get("idContratacaoPncp"))
    return sinc_procedure_key(row) == key and (not asserted or _control(asserted) == control)


def sinc_has_control(row: SincContrataRow, control: str | None) -> bool:
    expected = _control(control)
    return expected is not None and _control(row.payload.get("idContratacaoPncp")) == expected


def _number_year_modality(procurement: RawProcurement, row: SincContrataRow) -> bool:
    number = to_int(procurement.purchase_number)
    return (
        _cnpj(row.payload.get("cnpjProcedimento")) == procurement.agency_cnpj
        and number is not None
        and to_int(row.payload.get("numeroProcedimento")) == number
        and to_int(row.payload.get("anoProcedimento")) == procurement.purchase_year
        and _value(row.payload.get("tipoProcedimento")).upper() == "PE"
    )


@dataclass(frozen=True, slots=True)
class SincMatch:
    status: str
    key: tuple[str, str] | None
    rows: list[SincContrataRow]


def match_sinc_procedure(
    procurement: RawProcurement,
    procedures: ConnectorResult[list[SincContrataRow]],
) -> SincMatch:
    control = _control(procurement.pncp_control_number)
    exact = [row for row in procedures.data if sinc_has_control(row, control)]
    if exact:
        keys = {sinc_procedure_key(row) for row in exact}
        key = next(iter(keys)) if len(keys) == 1 else None
        if key is not None and all(_number_year_modality(procurement, row) for row in exact):
            related = [row for row in procedures.data if sinc_procedure_key(row) == key]
            if control and all(sinc_row_matches(row, key, control) for row in related):
                return SincMatch(
                    "exato" if procedures.is_complete else "exato_em_coleta_parcial", key, exact
                )
            return SincMatch("ambiguo", None, related)
        return SincMatch("ambiguo", None, exact)
    candidates = [row for row in procedures.data if _number_year_modality(procurement, row)]
    if candidates:
        return SincMatch("candidato_sem_vinculo", None, candidates)
    return SincMatch(
        "nao_localizado" if procedures.is_complete else "consulta_incompleta", None, []
    )


def _tce_case(
    procurement: RawProcurement,
    results: dict[SincContrataResource, ConnectorResult[list[SincContrataRow]]],
    refs: dict[str, str],
) -> dict[str, Any]:
    match = match_sinc_procedure(procurement, results[SincContrataResource.PROCEDURES])

    def summarize(row: SincContrataRow) -> dict[str, Any]:
        return {
            "id": row.payload.get("id"),
            "idProcedimento": row.payload.get("idProcedimento"),
            "idContratacaoPncp": row.payload.get("idContratacaoPncp"),
            "registro_fonte": refs[row.source_record_ref],
        }

    summary: dict[str, Any] = {
        "vinculo": match.status,
        "procedimentos": [
            {
                **summarize(row),
                **{
                    field: row.payload.get(field)
                    for field in (
                        "cnpjProcedimento",
                        "numeroProcedimento",
                        "anoProcedimento",
                        "tipoProcedimento",
                    )
                },
            }
            for row in match.rows
        ],
    }
    control = _control(procurement.pncp_control_number)
    if match.key is None or control is None:
        return summary
    for resource, result in results.items():
        rows = [row for row in result.data if sinc_row_matches(row, match.key, control)]
        if resource == SincContrataResource.PROCEDURES:
            rows = match.rows
        related = [
            {
                **summarize(row),
                **{
                    key: row.payload[key]
                    for key in (
                        "nomeTipoResultado",
                        "dataResultado",
                        "dataSessao",
                        "numeroContrato",
                        "anoContrato",
                        "idContrato",
                        "valor",
                        "link_documento",
                    )
                    if row.payload.get(key) not in (None, "", [])
                },
            }
            for row in rows
        ]
        if resource == SincContrataResource.CONTRACTS and procurement.purchase_year:
            for contract in related:
                contract_year = to_int(contract.get("anoContrato"))
                if contract_year is not None and contract_year < procurement.purchase_year:
                    contract["revisao"] = "Ano do contrato anterior ao ano do pregão no PNCP"
        summary[resource.value] = related
        if resource == SincContrataResource.BIDDERS:
            summary["cnpjs_licitantes"] = sorted(
                {cnpj for row in rows if (cnpj := _cnpj(row.payload.get("licitante")))}
            )
    return summary


def _purchase_key(row: RawProcurement) -> str | None:
    """Check the purchase in the PNCP link against the official unit/number/year."""
    key = _origin_key(row.raw_payload.get("linkSistemaOrigem"))
    number = to_int(row.purchase_number)
    if key and number is not None and row.purchase_year is not None:
        expected = f"{row.uasg}05{number:05d}{row.purchase_year:04d}"
        if key == expected:
            return key
    return None


async def _pncp_case(pncp: PNCPConnector, row: RawProcurement) -> dict[str, ConnectorResult[Any]]:
    items, documents = await asyncio.gather(
        pncp.fetch_items(row.external_id), pncp.fetch_documents(row.external_id)
    )
    results = await pncp.fetch_results(row.external_id, item_numbers={PILOT_ITEM})
    return {"itens": items, "documentos": documents, "resultados_item_1": results}


def _validate_compras_rows(
    result: ConnectorResult[Any], key: str, control: str | None
) -> ConnectorResult[Any]:
    def in_scope(row: Any) -> bool:
        payload = row.raw_payload
        ids = [
            payload.get(field)
            for field in ("idContratacaoPNCP", "numeroControlePNCPCompra")
            if _value(payload.get(field))
        ]
        return (
            bool(ids)
            and all(_control(identity) == control for identity in ids)
            and _value(payload.get("idCompra")) == key
        )

    scoped = [row for row in result.data if in_scope(row)]
    if len(scoped) == len(result.data):
        return result
    return result.model_copy(
        update={
            "data": scoped,
            "partial": True,
            "diagnostic": "linhas retornadas sem identidade oficial da compra consultada",
            "availability": result.availability
            if scoped
            else ConnectorAvailability.TEMPORARY_ERROR,
        }
    )


async def _compras_case(
    compras: ComprasGovConnector, row: RawProcurement
) -> dict[str, ConnectorResult[Any]]:
    key = _purchase_key(row)
    if key is None:
        return {}
    detail = await compras.fetch_procurement(key)
    if (
        detail.data is None
        or detail.data.external_id != key
        or _control(detail.data.pncp_control_number) != _control(row.pncp_control_number)
    ):
        return {"detalhe": detail}
    items, results = await asyncio.gather(compras.fetch_items(key), compras.fetch_results(key))
    control = _control(row.pncp_control_number)
    return {
        "detalhe": detail,
        "itens": _validate_compras_rows(items, key, control),
        "resultados": _validate_compras_rows(results, key, control),
    }


def _supplier_ids(result: ConnectorResult[Any] | None) -> list[str]:
    return sorted(
        {
            cnpj
            for row in (result.data if result else [])
            if to_int(row.item_number) == PILOT_ITEM
            and not row.is_cancelled
            and not nonaward_supplier(row.supplier_name)
            and (cnpj := _cnpj(row.supplier_cnpj))
        }
    )


def _item_summary(
    result: ConnectorResult[Any] | None, *, compras: bool
) -> dict[int, dict[str, Any]]:
    by_number: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in result.data if result else []:
        number = to_int(row.item_number)
        if number is None:
            continue
        observation = {
            "situacao": row.result_status,
            "catalogo": row.raw_payload.get("codItemCatalogo" if compras else "catalogoCodigoItem"),
            "categoria": row.raw_payload.get("itemCategoriaNome"),
        }
        if observation not in by_number[number]:
            by_number[number].append(observation)
    return {
        number: values[0] if len(values) == 1 else {"observacoes_divergentes": values}
        for number, values in by_number.items()
    }


async def _origin_view(
    archive: _Archive, row: RawProcurement, observation: OriginObservation | None
) -> dict[str, Any]:
    if observation is None:
        return {"estado": "nao_observada"}
    key = _purchase_key(row)
    if key is None or _origin_key(observation.url) != key:
        return {"estado": "url_sem_vinculo"}
    verified = _control(observation.controle_exibido) == _control(row.pncp_control_number)
    item_verified = _origin_key(observation.url_item, item=True) == key
    content = observation.model_dump(mode="json")
    if not verified:
        content = {
            field: content[field]
            for field in ("controle_pncp", "controle_exibido", "url", "observado_em", "diagnostico")
        }
    elif not item_verified:
        for field in (
            "cnpjs",
            "propostas_visiveis",
            "rotulos",
            "termo_julgamento",
            "termo_homologacao",
            "arquivo_julgamento_sha256",
            "arquivo_julgamento_paginas",
        ):
            content.pop(field, None)
    record = RawSourceRecord.build(
        source="compras_gov_origem",
        endpoint=observation.url,
        request_parameters={"modo": "observacao_navegador"},
        raw_payload=content,
        http_status=0,
        collected_at=observation.observado_em,
    )
    info = await archive.save(
        ConnectorResult(
            data=None,
            raw_records=[record],
            availability=(
                ConnectorAvailability.AVAILABLE
                if verified
                else ConnectorAvailability.TEMPORARY_ERROR
            ),
            partial=not item_verified,
            diagnostic=observation.diagnostico,
        )
    )
    return {"estado": "confirmada" if verified else "sem_confirmacao", **content, **info}


async def _compare_case(
    archive: _Archive,
    row: RawProcurement,
    pncp: dict[str, ConnectorResult[Any]],
    compras: dict[str, ConnectorResult[Any]],
    tce: dict[SincContrataResource, ConnectorResult[list[SincContrataRow]]],
    observation: OriginObservation | None,
    pncp_base: str,
    compras_base: str,
) -> dict[str, Any]:
    reference = parse_pncp_external_id(row.external_id)
    pncp_url = f"{pncp_base}/orgaos/{reference.cnpj}/compras/{reference.year}/{reference.sequence}"
    operations = {
        "itens": "itens",
        "documentos": "arquivos",
        "resultados_item_1": f"itens/{PILOT_ITEM}/resultados",
    }
    pncp_info = {
        name: await archive.save(
            result,
            attempt=_attempt("pncp", f"{pncp_url}/{operations[name]}", {})
            if result.availability.value in _INCOMPLETE
            else None,
        )
        for name, result in pncp.items()
    }
    compras_paths = {
        "detalhe": ComprasGovConnector.PROCUREMENT_ID_PATH,
        "itens": ComprasGovConnector.ITEM_ID_PATH,
        "resultados": ComprasGovConnector.RESULT_ID_PATH,
    }
    compras_info = {
        name: await archive.save(
            result,
            attempt=_attempt(
                "compras_gov",
                f"{compras_base}/{compras_paths[name].lstrip('/')}",
                {"tipo": "idCompra", "codigo": _purchase_key(row)},
            )
            if result.availability.value in _INCOMPLETE
            else None,
        )
        for name, result in compras.items()
    }
    origin_view = await _origin_view(archive, row, observation)
    active = [doc for doc in pncp["documentos"].data if doc.active is not False]
    documents = [
        {
            "titulo": doc.title,
            "tipo": doc.document_type,
            "url": doc.download_url,
            "registro_fonte": archive.references.get(doc.source_record_ref or ""),
        }
        for doc in active
    ]
    judgment = any(
        _JUDGMENT.search(normalized_text(value))
        for doc in active
        for value in (doc.title, doc.document_type)
    )
    pncp_items = _item_summary(pncp["itens"], compras=False)
    compras_items = _item_summary(compras.get("itens"), compras=True)
    pncp_suppliers = _supplier_ids(pncp["resultados_item_1"])
    compras_suppliers = _supplier_ids(compras.get("resultados"))
    extra_catalog = [
        number
        for number in sorted(pncp_items.keys() & compras_items.keys())
        if compras_items[number].get("catalogo")
        and "catalogo" in pncp_items[number]
        and not pncp_items[number]["catalogo"]
    ]
    different_categories = [
        number
        for number in sorted(pncp_items.keys() & compras_items.keys())
        if (pncp_category := pncp_items[number].get("categoria"))
        and (compras_category := compras_items[number].get("categoria"))
        and normalized_text(pncp_category) != normalized_text(compras_category)
    ]
    pncp_results_observed = (
        pncp["itens"].is_complete
        and not pncp["resultados_item_1"].partial
        and pncp["resultados_item_1"].availability.value in {"available", "not_published", "empty"}
        and PILOT_ITEM in pncp_items
    )
    return {
        "controle_pncp": row.pncp_control_number,
        "numero": row.purchase_number,
        "ano": row.purchase_year,
        "publicacao": row.publication_at.isoformat() if row.publication_at else None,
        "registro_descoberta": archive.references.get(row.source_record_ref or ""),
        "url_origem_pncp": row.raw_payload.get("linkSistemaOrigem"),
        "pncp": {
            "documentos": documents,
            "titulo_de_julgamento": judgment,
            "itens": pncp_items,
            "cnpjs_resultado_item_1": pncp_suppliers,
            "resultado_item_1_observado": pncp_results_observed,
            "consultas": pncp_info,
        },
        "tce_ma": _tce_case(row, tce, archive.references),
        "compras_gov": {
            "id_compra": _purchase_key(row),
            "vinculo_exato": "itens" in compras,
            "itens": compras_items,
            "itens_com_codigo_catalogo_adicional": extra_catalog,
            "itens_com_categoria_divergente": different_categories,
            "cnpjs_resultado_item_1": compras_suppliers,
            "cnpjs_resultado_item_1_adicionais": sorted(
                set(compras_suppliers) - set(pncp_suppliers)
            )
            if pncp_results_observed
            else None,
            "consultas": compras_info,
            "pagina_publica": origin_view,
        },
    }


async def run_tce_ma_pilot(
    *,
    settings: object,
    output: Path,
    start: date,
    end: date,
    limit: int,
    observations: list[OriginObservation],
    progress: Callable[[str], None],
) -> dict[str, Any]:
    """Create an isolated audit database and return the source comparison."""
    if start > end or start.year != end.year or not 1 <= limit <= 20:
        raise ValueError("use um período dentro do mesmo ano e de 1 a 20 pregões")
    by_control = {observation.controle_pncp: observation for observation in observations}
    if len(by_control) != len(observations):
        raise ValueError("observações da origem contêm número de controle repetido")
    output.mkdir(parents=True, exist_ok=False)
    engine = create_database_engine(f"sqlite:///{output / 'fontes.sqlite3'}")
    sessions = create_session_factory(engine)
    scope = {
        "ente_ibge": ENTE,
        "cnpj": CNPJ,
        "uasg": UASG,
        "inicio_publicacao": start.isoformat(),
        "fim_publicacao": end.isoformat(),
        "limite": limit,
        "item_amostrado": PILOT_ITEM,
    }
    started = datetime.now(UTC)
    run_id: UUID | None = None
    status = CrawlRunStatus.FAILED
    diagnostic: str | None = None
    cases: list[dict[str, Any]] = []
    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        async with sessions.begin() as session:
            run = CrawlRun(
                connector="piloto_tce_ma",
                started_at=started,
                status=CrawlRunStatus.RUNNING,
                filters=scope,
            )
            session.add(run)
            await session.flush()
            run_id = run.id
        archive = _Archive(sessions, run_id)
        http = AsyncHTTPClient(settings, public_only=True, timeout_seconds=20, max_retries=1)
        try:
            pncp = PNCPConnector(settings, http_client=http)
            compras = ComprasGovConnector(settings, http_client=http)
            tce = TCEMAConnector(http)
            progress("Consultando o acervo municipal do TCE-MA e a publicação no PNCP")
            resources = tuple(SincContrataResource)

            async def fetch_inventory() -> dict[
                SincContrataResource, ConnectorResult[list[SincContrataRow]]
            ]:
                resources_data = await asyncio.gather(
                    *(tce.fetch(resource, ente_id=ENTE) for resource in resources)
                )
                return dict(zip(resources, resources_data, strict=True))

            discovery, tce_data = await asyncio.gather(
                pncp.discover_procurements(
                    ProcurementFilters(
                        start_date=start,
                        end_date=end,
                        uf="MA",
                        municipality_code=str(ENTE),
                        agency_cnpj=CNPJ,
                        unit_code=UASG,
                        modalities=[6],
                    )
                ),
                fetch_inventory(),
            )
            discovery_info = await archive.save(
                discovery,
                attempt=_attempt(
                    "pncp",
                    f"{pncp.consulta_base_url}/contratacoes/publicacao",
                    {
                        "dataInicial": start.strftime("%Y%m%d"),
                        "dataFinal": end.strftime("%Y%m%d"),
                        "codigoMunicipioIbge": str(ENTE),
                        "cnpj": CNPJ,
                        "codigoUnidadeAdministrativa": UASG,
                        "codigoModalidadeContratacao": 6,
                        "uf": "MA",
                    },
                )
                if discovery.availability.value in _INCOMPLETE
                else None,
            )
            tce_info = {
                resource.value: {
                    **await archive.save(result),
                    "linhas_lidas": len(result.data),
                    "total_fonte": result.pagination.total_records if result.pagination else None,
                }
                for resource, result in tce_data.items()
            }
            progress(
                "TCE-MA: "
                + ", ".join(
                    f"{resource} {info['linhas_lidas']}" for resource, info in tce_info.items()
                )
            )
            local_timezone = ZoneInfo("America/Fortaleza")
            selected = sorted(
                (
                    row
                    for row in discovery.data
                    if _control(row.pncp_control_number)
                    and row.agency_cnpj == CNPJ
                    and row.uasg == UASG
                    and row.municipality_code == str(ENTE)
                    and row.modality_code == 6
                    and row.purchase_year == start.year
                    and row.publication_at is not None
                    and start <= row.publication_at.astimezone(local_timezone).date() <= end
                ),
                key=lambda row: (row.publication_at or started, row.external_id),
            )[:limit]

            async def collect(
                row: RawProcurement,
            ) -> tuple[dict[str, ConnectorResult[Any]], dict[str, ConnectorResult[Any]]]:
                return await asyncio.gather(_pncp_case(pncp, row), _compras_case(compras, row))

            by_id = {}
            stream = bounded_map(selected, collect, capacity=4)
            try:
                async for row, (pncp_data, compras_data) in stream:
                    by_id[row.external_id] = await _compare_case(
                        archive,
                        row,
                        pncp_data,
                        compras_data,
                        tce_data,
                        by_control.get(row.pncp_control_number or ""),
                        pncp.integration_base_url,
                        compras.base_url,
                    )
                    progress(
                        f"Pregão {row.purchase_number}/{row.purchase_year} conferido ({len(by_id)}/{len(selected)})"
                    )
            finally:
                await stream.aclose()
            cases = [by_id[row.external_id] for row in selected]
            incomplete = (
                not discovery.is_complete
                or len(cases) != limit
                or any(not result.is_complete for result in tce_data.values())
                or any(
                    info["parcial"] or info["disponibilidade"] in _INCOMPLETE
                    for case in cases
                    for source in ("pncp", "compras_gov")
                    for info in case[source]["consultas"].values()
                )
                or any(not case["compras_gov"]["vinculo_exato"] for case in cases)
            )
            status = CrawlRunStatus.PARTIAL if incomplete else CrawlRunStatus.COMPLETED
        finally:
            await http.aclose()
    except asyncio.CancelledError:
        status = CrawlRunStatus.CANCELLED
        raise
    except Exception as exc:
        diagnostic = type(exc).__name__
        raise
    finally:
        try:
            if run_id:
                async with sessions.begin() as session:
                    stored = await session.get(CrawlRun, run_id)
                    assert stored is not None
                    stored.status = status
                    stored.finished_at = datetime.now(UTC)
                    stored.records_found = len(cases)
                    stored.diagnostic = diagnostic
        finally:
            await engine.dispose()
    return {
        "escopo": scope,
        "execucao": str(run_id),
        "estado": status.value,
        "inicio_coleta_utc": started.isoformat(),
        "pncp_descoberta": discovery_info,
        "tce_ma_coleta": tce_info,
        "pregoes": cases,
    }
