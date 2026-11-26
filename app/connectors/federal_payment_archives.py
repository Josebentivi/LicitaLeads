"""Read the CGU's daily SIAFI payment files with bounded, public-only downloads."""

from __future__ import annotations

import asyncio
import codecs
import csv
import hashlib
import re
import stat
import unicodedata
from collections import Counter, defaultdict
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from io import TextIOWrapper
from tempfile import TemporaryFile
from typing import BinaryIO, Literal, cast
from urllib.parse import urljoin, urlsplit
from zipfile import BadZipFile, ZipExtFile, ZipFile, ZipInfo
from zlib import error as DeflateError

import httpx

from app.config import Settings
from app.connectors.bounded_http import (
    SourceDownloadError,
    pin_public_transport,
    validate_public_url,
)
from app.services.documents.security import (
    ArchiveBudget,
    ArchiveLimits,
    UnsafeArchiveError,
    safe_archive_name,
    validate_zip_info,
)
from app.services.identifiers import is_valid_cnpj, normalize_cnpj
from app.services.parallel import joined_thread

FEDERAL_PAYMENT_CATALOGUE = "https://portaldatransparencia.gov.br/download-de-dados/despesas/"
_OFFICIAL_HOSTS = {"portaldatransparencia.gov.br", "dadosabertos-download.cgu.gov.br"}
_CATALOGUE_LIMIT = 4 * 1024 * 1024
_MAX_EXPANDED_BYTES = 512 * 1024 * 1024
_MAX_MATCHING_ROWS = 100_000
_CHUNK_SIZE = 256 * 1024
_MAX_REDIRECTS = 3
_REDIRECTS = {301, 302, 303, 307, 308}
_ARCHIVE_LIMITS = ArchiveLimits(
    max_members=32,
    max_uncompressed_bytes=_MAX_EXPANDED_BYTES,
    max_member_bytes=_MAX_EXPANDED_BYTES,
)
_CATALOGUE_ENTRY = re.compile(
    r'arquivos\.push\(\s*\{\s*"ano"\s*:\s*"([0-9]{4})"\s*,'
    r'\s*"mes"\s*:\s*"([0-9]{2})"\s*,\s*"dia"\s*:\s*"([0-9]{2})"\s*,'
    r'\s*"origem"\s*:\s*"Despesas"\s*\}\s*\)\s*;'
)
_DOCUMENT = re.compile(r"[0-9]{15}[A-Z]{2}[0-9]{6}")
_CREDITORS_LIST = re.compile(r"[A-Za-z0-9._/-]{1,80}")
_CNPJ = re.compile(r"[A-Za-z0-9./-]{14,18}")
_MONEY = re.compile(r"-?(?:[0-9]{1,3}(?:\.[0-9]{3})+|[0-9]+)(?:,[0-9]{1,6})?")
_CENT = Decimal("0.01")
_AMOUNT_BOUND = Decimal("100000000000000000")

# Keep only fields needed to support the observations, without free-form
# descriptions or data about subjects outside the requested CNPJ scope.
_COMMON_COLUMNS = {
    "document": "Código Pagamento",
    "issued_on": "Data Emissão",
    "cnpj": "Código Favorecido",
    "supplier_name": "Favorecido",
}
_ISSUED_COLUMNS = {
    **_COMMON_COLUMNS,
    "amount": "Valor do Pagamento Convertido pra R$",
    "agency_name": "Órgão",
    "management_unit": "Código Unidade Gestora",
    "process_number": "Processo",
}
_FINAL_COLUMNS = {
    **_COMMON_COLUMNS,
    "amount": "Valor do Pagamento em R$",
    "list_code": "Código Lista",
}
_REQUIRED_COLUMNS = {"document", "issued_on", "cnpj", "amount"}


class FederalArchiveUnavailable(ValueError):
    """The official source could not return the catalogue or advertised archive."""

    def __init__(self, message: str, *, http_status: int | None = None) -> None:
        super().__init__(message)
        self.http_status = http_status


class FederalArchiveInvalid(ValueError):
    """The source returned data that cannot prove coverage of a day."""


@dataclass(frozen=True, slots=True)
class FederalArchiveReference:
    member: str
    line_number: int
    fields: dict[str, str]


@dataclass(frozen=True, slots=True)
class FederalArchivePayment:
    document: str
    cnpj: str
    role: Literal["issued", "final"]
    issued_on: date
    amount: Decimal
    member: str
    line_numbers: tuple[int, ...]
    raw_rows: tuple[dict[str, str], ...]
    supplier_name: str | None = None
    agency_name: str | None = None
    management_unit: str | None = None
    process_number: str | None = None
    list_code: str | None = None
    is_intermediary: bool | None = None
    related_evidence: tuple[FederalArchiveReference, ...] = ()


@dataclass(frozen=True, slots=True)
class FederalArchiveReview:
    reason: str
    member: str
    line_numbers: tuple[int, ...]
    raw_rows: tuple[dict[str, str], ...]


@dataclass(frozen=True, slots=True)
class FederalPaymentArchive:
    day: date
    url: str
    resolved_url: str
    sha256: str
    collected_at: datetime
    records: tuple[FederalArchivePayment, ...]
    issues: tuple[str, ...] = ()
    review_rows: tuple[FederalArchiveReview, ...] = ()


class FederalPaymentArchivesConnector:
    """Read advertised daily archives and return matched corporate observations."""

    def __init__(self, settings: Settings, *, client: httpx.AsyncClient | None = None) -> None:
        self._owns_client = client is None
        self.catalogue_sha256: str | None = None
        self.catalogue_collected_at: datetime | None = None
        self._timeout = max(60.0, min(300.0, settings.transparencia_timeout_seconds * 4))
        self._max_bytes = settings.federal_archive_max_bytes
        self._archive_limits = replace(
            _ARCHIVE_LIMITS,
            max_uncompressed_bytes=settings.federal_archive_max_expanded_bytes,
            max_member_bytes=settings.federal_archive_max_expanded_bytes,
        )
        self._client = client or httpx.AsyncClient(
            headers={"User-Agent": settings.http_user_agent, "Accept-Encoding": "identity"},
            timeout=settings.transparencia_timeout_seconds,
            trust_env=False,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def list_days(self) -> frozenset[date]:
        self.catalogue_sha256 = None
        self.catalogue_collected_at = None
        async with self._open(FEDERAL_PAYMENT_CATALOGUE) as response:
            content = bytearray()
            async for chunk in _bounded_body(response, _CATALOGUE_LIMIT):
                content.extend(chunk)
        collected_at = datetime.now(UTC)
        days = await joined_thread(_catalogue_days, content)
        self.catalogue_sha256 = hashlib.sha256(content).hexdigest()
        self.catalogue_collected_at = collected_at
        return days

    async def fetch(
        self,
        day: date,
        cnpjs: frozenset[str],
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> FederalPaymentArchive:
        start_date, end_date = start_date or day, end_date or day
        if not start_date <= day <= end_date:
            raise ValueError("A data do arquivo precisa estar no período da coleta.")
        targets = frozenset(
            cnpj
            for value in cnpjs
            if (cnpj := normalize_cnpj(value)) is not None and is_valid_cnpj(cnpj)
        )
        url = federal_archive_url(day)
        digest = hashlib.sha256()
        with TemporaryFile(mode="w+b") as temporary:
            handle = cast(BinaryIO, getattr(temporary, "file", temporary))
            async with self._open(url) as response:
                resolved_url = str(response.url)
                async for chunk in _bounded_body(response, self._max_bytes):
                    digest.update(chunk)
                    await joined_thread(handle.write, chunk)
            collected_at = datetime.now(UTC)
            records, review = await joined_thread(
                _read_archive, handle, day, targets, start_date, end_date, self._archive_limits
            )
        issues = Counter(item.reason for item in review)
        return FederalPaymentArchive(
            day=day,
            url=url,
            resolved_url=resolved_url,
            sha256=digest.hexdigest(),
            collected_at=collected_at,
            records=tuple(records),
            issues=tuple(f"{reason} ({count})" for reason, count in sorted(issues.items())),
            review_rows=tuple(review),
        )

    @asynccontextmanager
    async def _open(self, url: str) -> AsyncIterator[httpx.Response]:
        current = url
        try:
            async with asyncio.timeout(self._timeout):
                for redirect in range(_MAX_REDIRECTS + 1):
                    try:
                        origin = urlsplit(current)
                        port = origin.port
                    except ValueError as exc:
                        raise FederalArchiveUnavailable("Destino oficial da CGU inválido.") from exc
                    if (
                        origin.scheme != "https"
                        or origin.hostname not in _OFFICIAL_HOSTS
                        or port not in {None, 443}
                    ):
                        raise FederalArchiveUnavailable(
                            "Destino externo ao domínio oficial da CGU."
                        )
                    await validate_public_url(current)
                    request = self._client.build_request(
                        "GET", current, headers={"Accept-Encoding": "identity"}
                    )
                    for header in (
                        "authorization",
                        "proxy-authorization",
                        "cookie",
                        "chave-api-dados",
                    ):
                        request.headers.pop(header, None)
                    pin_public_transport(self._client, request.url)
                    response = await self._client.send(
                        request, stream=True, follow_redirects=False, auth=None
                    )
                    try:
                        if response.status_code in _REDIRECTS:
                            location = response.headers.get("location")
                            if redirect == _MAX_REDIRECTS or not location:
                                raise FederalArchiveUnavailable(
                                    "Redirecionamento inválido do arquivo federal.",
                                    http_status=response.status_code,
                                )
                            current = urljoin(str(response.url), location)
                            continue
                        if response.status_code != 200:
                            raise FederalArchiveUnavailable(
                                f"Portal federal retornou HTTP {response.status_code}.",
                                http_status=response.status_code,
                            )
                        yield response
                        return
                    finally:
                        await response.aclose()
        except (SourceDownloadError, httpx.TransportError, TimeoutError) as exc:
            raise FederalArchiveUnavailable(
                "Não foi possível acessar a fonte oficial da CGU."
            ) from exc


def federal_archive_url(day: date) -> str:
    return f"{FEDERAL_PAYMENT_CATALOGUE}{day:%Y%m%d}"


async def _bounded_body(response: httpx.Response, limit: int) -> AsyncIterator[bytes]:
    if response.headers.get("content-encoding", "identity").lower() != "identity":
        raise FederalArchiveInvalid("Codificação HTTP inesperada na resposta federal.")
    raw_size = response.headers.get("content-length")
    declared_size: int | None = None
    if raw_size is not None:
        try:
            declared_size = int(raw_size)
        except ValueError as exc:
            raise FederalArchiveInvalid("Tamanho inválido na resposta federal.") from exc
        if not 0 <= declared_size <= limit:
            raise FederalArchiveInvalid("Download federal excede o limite de tamanho.")
    read = 0
    async for chunk in response.aiter_raw(chunk_size=_CHUNK_SIZE):
        read += len(chunk)
        if read > limit:
            raise FederalArchiveInvalid("Download federal excede o limite de tamanho.")
        yield chunk
    if declared_size is not None and read != declared_size:
        raise FederalArchiveInvalid("Resposta federal interrompida antes do tamanho declarado.")


def _catalogue_days(content: bytes | bytearray) -> frozenset[date]:
    try:
        days = frozenset(
            date(int(year), int(month), int(day))
            for year, month, day in _CATALOGUE_ENTRY.findall(content.decode("utf-8-sig"))
        )
    except ValueError as exc:
        raise FederalArchiveInvalid("Catálogo de arquivos federais inválido.") from exc
    if not days:
        raise FederalArchiveInvalid("Nenhuma data pôde ser confirmada no catálogo federal.")
    return days


def _read_archive(
    file: BinaryIO,
    day: date,
    targets: frozenset[str],
    start_date: date,
    end_date: date,
    limits: ArchiveLimits = _ARCHIVE_LIMITS,
) -> tuple[list[FederalArchivePayment], list[FederalArchiveReview]]:
    file.seek(0)
    if file.read(4) != b"PK\x03\x04":
        raise FederalArchiveInvalid("A fonte não retornou um ZIP de despesas.")
    try:
        with ZipFile(file) as archive:
            issued, finals = _payment_members(archive, day, limits)
            return _read_payments(archive, issued, finals, targets, start_date, end_date)
    except (
        BadZipFile,
        DeflateError,
        UnsafeArchiveError,
        csv.Error,
        UnicodeError,
        RuntimeError,
        EOFError,
    ) as exc:
        raise FederalArchiveInvalid(
            "O ZIP ou os CSVs de pagamentos federais são inválidos."
        ) from exc


def _payment_members(archive: ZipFile, day: date, limits: ArchiveLimits) -> tuple[ZipInfo, ZipInfo]:
    expected = (
        f"{day:%Y%m%d}_Despesas_Pagamento.csv",
        f"{day:%Y%m%d}_Despesas_Pagamento_FavorecidosFinais.csv",
    )
    members: dict[str, ZipInfo] = {}
    names: set[str] = set()
    budget = ArchiveBudget(limits)
    for member in archive.infolist():
        name = safe_archive_name(member.filename)
        if (
            ".." in member.filename.split("/")
            or "\\" in member.filename
            or name.casefold() in names
            or stat.S_ISLNK(member.external_attr >> 16)
        ):
            raise FederalArchiveInvalid("Nome inesperado ou repetido no ZIP de pagamentos.")
        names.add(name.casefold())
        validate_zip_info(member, limits)
        budget.reserve(member)
        if not member.is_dir():
            members[name] = member
    try:
        return members[expected[0]], members[expected[1]]
    except KeyError as exc:
        raise FederalArchiveInvalid("O ZIP não contém os dois CSVs oficiais do dia.") from exc


@dataclass(frozen=True, slots=True)
class _CsvRow:
    line: int
    values: dict[str, str]
    raw: dict[str, str]


def _column_key(value: str) -> str:
    value = unicodedata.normalize("NFKD", value.strip()).casefold()
    return " ".join(
        "".join(character for character in value if not unicodedata.combining(character)).split()
    )


def _member_encoding(sample: bytes) -> str:
    if sample.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    try:
        codecs.getincrementaldecoder("utf-8")().decode(sample, final=False)
    except UnicodeDecodeError:
        return "cp1252"
    return "utf-8"


def _csv_rows(
    archive: ZipFile,
    member: ZipInfo,
    columns: dict[str, str],
    required: set[str],
) -> Iterator[_CsvRow]:
    with cast(ZipExtFile, archive.open(member)) as buffer:
        with TextIOWrapper(
            buffer, encoding=_member_encoding(buffer.peek(8192)), newline=""
        ) as stream:
            reader = csv.reader(stream, delimiter=";", strict=True)
            header = next(reader, [])
            normalized = [_column_key(value) for value in header]
            if len(set(normalized)) != len(normalized):
                raise FederalArchiveInvalid("Colunas repetidas em CSV de pagamentos federais.")
            index = {name: number for number, name in enumerate(normalized)}
            selected = {
                name: index[key]
                for name, label in columns.items()
                if (key := _column_key(label)) in index
            }
            if not required <= selected.keys():
                raise FederalArchiveInvalid("Esquema inesperado no CSV de pagamentos federais.")
            for values in reader:
                if not values:
                    continue
                if len(values) != len(header):
                    raise FederalArchiveInvalid("Linha incompleta no CSV de pagamentos federais.")
                yield _CsvRow(
                    line=reader.line_num,
                    values={field: values[i].strip() for field, i in selected.items()},
                    raw={header[i]: values[i] for i in selected.values()},
                )


def _payment_amount(value: str) -> Decimal | None:
    if not _MONEY.fullmatch(value) or len(value) > 40:
        return None
    amount = Decimal(value.replace(".", "").replace(",", "."))
    if abs(amount) >= _AMOUNT_BOUND or amount != amount.quantize(_CENT):
        return None
    return amount.quantize(_CENT)


def _payment_date(value: str) -> date | None:
    try:
        return datetime.strptime(value, "%d/%m/%Y").date()
    except ValueError:
        return None


def _published_cnpj(value: str, targets: frozenset[str]) -> str | None:
    cnpj = normalize_cnpj(value) if _CNPJ.fullmatch(value) else None
    return cnpj if cnpj in targets else None


def _as_payment(
    row: _CsvRow,
    cnpj: str,
    role: Literal["issued", "final"],
    member: ZipInfo,
    start_date: date,
    end_date: date,
    reviews: list[FederalArchiveReview],
) -> FederalArchivePayment | None:
    issued_on = _payment_date(row.values["issued_on"])
    if issued_on and not start_date <= issued_on <= end_date:
        return None
    amount = _payment_amount(row.values["amount"])
    document = row.values["document"]
    list_code = row.values.get("list_code")
    if (
        issued_on is None
        or amount is None
        or not _DOCUMENT.fullmatch(document)
        or (role == "final" and not _CREDITORS_LIST.fullmatch(list_code or ""))
    ):
        reviews.append(
            FederalArchiveReview(
                reason="Identidade, data ou valor sem confirmação na linha oficial",
                member=member.filename,
                line_numbers=(row.line,),
                raw_rows=(row.raw,),
            )
        )
        return None
    return FederalArchivePayment(
        document=document,
        cnpj=cnpj,
        role=role,
        issued_on=issued_on,
        amount=amount,
        member=member.filename,
        line_numbers=(row.line,),
        raw_rows=(row.raw,),
        supplier_name=row.values.get("supplier_name") or None,
        agency_name=row.values.get("agency_name") or None,
        management_unit=row.values.get("management_unit") or None,
        process_number=row.values.get("process_number") or None,
        list_code=list_code,
    )


def _read_payments(
    archive: ZipFile,
    issued: ZipInfo,
    finals: ZipInfo,
    targets: frozenset[str],
    start_date: date,
    end_date: date,
) -> tuple[list[FederalArchivePayment], list[FederalArchiveReview]]:
    payments: list[FederalArchivePayment] = []
    reviews: list[FederalArchiveReview] = []
    intermediaries: dict[str, FederalArchiveReference] = {}
    final_documents: set[str] = set()
    for row in _csv_rows(archive, finals, _FINAL_COLUMNS, _REQUIRED_COLUMNS | {"list_code"}):
        document = row.values["document"]
        if _DOCUMENT.fullmatch(document) and _CREDITORS_LIST.fullmatch(row.values["list_code"]):
            if document not in intermediaries:
                intermediaries[document] = FederalArchiveReference(
                    finals.filename,
                    row.line,
                    {"Código Pagamento": document, "Código Lista": row.values["list_code"]},
                )
        if cnpj := _published_cnpj(row.values["cnpj"], targets):
            _check_match_budget(payments, reviews)
            if payment := _as_payment(row, cnpj, "final", finals, start_date, end_date, reviews):
                payments.append(payment)
                final_documents.add(document)

    issued_context: dict[str, FederalArchiveReference | None] = {}
    for row in _csv_rows(archive, issued, _ISSUED_COLUMNS, _REQUIRED_COLUMNS):
        document = row.values["document"]
        if document in final_documents:
            fields = {
                _ISSUED_COLUMNS[name]: row.values[name]
                for name in ("document", "agency_name", "management_unit", "process_number")
                if row.values.get(name)
            }
            if document not in issued_context:
                issued_context[document] = FederalArchiveReference(
                    issued.filename, row.line, fields
                )
            elif (prior := issued_context[document]) is not None and prior.fields != fields:
                issued_context[document] = None
        if cnpj := _published_cnpj(row.values["cnpj"], targets):
            _check_match_budget(payments, reviews)
            if payment := _as_payment(row, cnpj, "issued", issued, start_date, end_date, reviews):
                if evidence := intermediaries.get(document):
                    payment = replace(payment, is_intermediary=True, related_evidence=(evidence,))
                payments.append(payment)

    reconciled = _combine_rows(payments, reviews)
    for index, payment in enumerate(reconciled):
        if payment.role == "final" and (origin := issued_context.get(payment.document)) is not None:
            reconciled[index] = replace(
                payment,
                agency_name=origin.fields.get(_ISSUED_COLUMNS["agency_name"]),
                management_unit=origin.fields.get(_ISSUED_COLUMNS["management_unit"]),
                process_number=origin.fields.get(_ISSUED_COLUMNS["process_number"]),
                related_evidence=(origin,),
            )
    return reconciled, reviews


def _check_match_budget(
    payments: list[FederalArchivePayment], reviews: list[FederalArchiveReview]
) -> None:
    if len(payments) + len(reviews) >= _MAX_MATCHING_ROWS:
        raise FederalArchiveInvalid("O arquivo excede o limite de linhas dos CNPJs selecionados.")


def _combine_rows(
    payments: list[FederalArchivePayment], reviews: list[FederalArchiveReview]
) -> list[FederalArchivePayment]:
    groups: dict[tuple[str, str, str, str | None], list[FederalArchivePayment]] = defaultdict(list)
    for payment in payments:
        groups[(payment.role, payment.document, payment.cnpj, payment.list_code)].append(payment)
    combined: list[FederalArchivePayment] = []
    for group in groups.values():
        first = group[0]
        if len(group) == 1:
            combined.append(first)
            continue
        line_numbers = tuple(line for payment in group for line in payment.line_numbers)
        raw_rows = tuple(row for payment in group for row in payment.raw_rows)
        amount = _group_amount(group)
        if amount is None:
            reviews.append(
                FederalArchiveReview(
                    reason="Identidade repetida sem distinção comprovada no arquivo",
                    member=first.member,
                    line_numbers=line_numbers,
                    raw_rows=raw_rows,
                )
            )
            continue
        combined.append(replace(first, amount=amount, line_numbers=line_numbers, raw_rows=raw_rows))
    return combined


def _group_amount(group: list[FederalArchivePayment]) -> Decimal | None:
    first = group[0]
    if first.role == "issued":
        return first.amount if all(item.raw_rows == first.raw_rows for item in group) else None
    # The daily list has no line ID. Distinct amounts can be accumulated for
    # its one document/list/CNPJ identity. Repeated values need human review.
    if len({item.issued_on for item in group}) != 1:
        return None
    amounts = {item.amount for item in group}
    total = sum(amounts, start=Decimal("0.00"))
    return total if len(amounts) == len(group) and abs(total) < _AMOUNT_BOUND else None
