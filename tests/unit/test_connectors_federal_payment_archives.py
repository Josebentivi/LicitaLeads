"""Daily CGU archives keep document and final-beneficiary evidence separate."""

from __future__ import annotations

import csv
import hashlib
from datetime import date
from decimal import Decimal
from io import BytesIO, StringIO
from zipfile import ZIP_DEFLATED, ZipFile

import httpx
import pytest

from app.config import Settings
from app.connectors import federal_payment_archives as archives

DAY = date(2026, 11, 17)
ISSUED_NAME = "20261117_Despesas_Pagamento.csv"
FINAL_NAME = "20261117_Despesas_Pagamento_FavorecidosFinais.csv"
STORAGE = (
    "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/despesas/"
    "20261117_Despesas.zip"
)
DOCUMENT = "153173152532026OB008929"
OTHER_DOCUMENT = "153173152532026OB008930"
PAID_CNPJ = "52246769000198"
FINAL_CNPJ = "11222333000181"
CPF = "98765432101"
DATE = "17/11/2026"
_ISSUED_HEADER = (
    "Código Pagamento",
    "Código Favorecido",
    "Favorecido",
    "Data Emissão",
    "Valor do Pagamento Convertido pra R$",
    "Órgão",
    "Código Unidade Gestora",
    "Processo",
    "Observação",
)
_FINAL_HEADER = (
    "Código Pagamento",
    "Código Favorecido",
    "Favorecido",
    "Data Emissão",
    "Valor do Pagamento em R$",
    "Código Lista",
)


def _csv(header, rows, *, encoding="cp1252") -> bytes:
    stream = StringIO(newline="")
    writer = csv.writer(stream, delimiter=";")
    writer.writerow(header)
    writer.writerows(rows)
    return stream.getvalue().encode(encoding)


def _zip(*, issued=(), finals=(), extras=None, files=None) -> bytes:
    payload = BytesIO()
    selected = (
        {
            ISSUED_NAME: _csv(_ISSUED_HEADER, issued),
            FINAL_NAME: _csv(_FINAL_HEADER, finals, encoding="utf-8-sig"),
        }
        if files is None
        else files
    )
    with ZipFile(payload, "w", compression=ZIP_DEFLATED) as archive:
        for name, data in {**selected, **(extras or {})}.items():
            archive.writestr(name, data)
    return payload.getvalue()


def _issued(cnpj, *, document=DOCUMENT, issued_on=DATE, amount="1.234,56"):
    return (
        document,
        cnpj,
        "Fornecedor Ltda",
        issued_on,
        amount,
        "Órgão amostra",
        "153173",
        "2026/0008",
        f"Observação contém terceiro {CPF}",
    )


def _final(cnpj, amount, *, list_code="2026LC000051", document=DOCUMENT, issued_on=DATE):
    return document, cnpj, "Favorecido Ltda", issued_on, amount, list_code


def _http_response(content: bytes, *, headers=None) -> httpx.Response:
    return httpx.Response(200, stream=httpx.ByteStream(content), headers=headers)


@pytest.fixture
def public_addresses(monkeypatch) -> list[str]:
    validated = []

    async def validate(url, *, allow_http=False):
        assert not allow_http
        validated.append(url)

    monkeypatch.setattr(archives, "validate_public_url", validate)
    return validated


async def test_fetch_streams_official_zip_and_preserves_only_target_rows(public_addresses) -> None:
    content = _zip(
        issued=(
            _issued("52.246.769/0001-98"),
            _issued(CPF, document=OTHER_DOCUMENT, amount="35,00"),
        ),
        finals=(
            _final(FINAL_CNPJ, "10,05"),
            _final(FINAL_CNPJ, "20,10"),
            _final(FINAL_CNPJ, "1,20", list_code="2026LC000052", document=OTHER_DOCUMENT),
            _final(CPF, "99,99"),
        ),
        extras={"20261117_Despesas_Empenho.csv": b"outside scope"},
    )
    url = archives.federal_archive_url(DAY)

    def request_handler(request: httpx.Request) -> httpx.Response:
        assert "chave-api-dados" not in request.headers
        if str(request.url) == url:
            return httpx.Response(302, headers={"Location": STORAGE})
        assert str(request.url) == STORAGE
        return _http_response(content, headers={"Content-Length": str(len(content))})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(request_handler), headers={"chave-api-dados": "not-forwarded"}
    ) as client:
        connector = archives.FederalPaymentArchivesConnector(
            Settings(app_env="test"), client=client
        )
        result = await connector.fetch(DAY, frozenset({PAID_CNPJ, FINAL_CNPJ, CPF}))
        await connector.aclose()
        assert not client.is_closed

    assert public_addresses == [url, STORAGE]
    assert (result.day, result.url, result.resolved_url) == (DAY, url, STORAGE)
    assert result.sha256 == hashlib.sha256(content).hexdigest()
    issued = next(record for record in result.records if record.role == "issued")
    first_final, second_final = (row for row in result.records if row.role == "final")
    assert issued.cnpj == PAID_CNPJ
    assert issued.amount == Decimal("1234.56")
    assert issued.is_intermediary is True
    assert issued.supplier_name == "Fornecedor Ltda"
    assert first_final.cnpj == FINAL_CNPJ
    assert first_final.amount == Decimal("30.15")
    assert first_final.list_code == "2026LC000051"
    assert first_final.line_numbers == (2, 3)
    assert first_final.agency_name == second_final.agency_name == "Órgão amostra"
    assert second_final.amount == Decimal("1.20")
    assert issued.related_evidence[0].fields == {
        "Código Pagamento": DOCUMENT,
        "Código Lista": "2026LC000051",
    }
    assert second_final.related_evidence[0].member == ISSUED_NAME
    assert second_final.related_evidence[0].line_number == 3
    assert CPF not in str(second_final.related_evidence[0].fields)
    assert all(CPF not in str(row.raw_rows) for row in result.records)
    assert result.issues == result.review_rows == ()


async def test_catalogue_uses_only_advertised_expense_days(public_addresses) -> None:
    content = b"""
    <script>
    arquivos.push({"ano" : "2026", "mes" : "11", "dia" : "17", "origem" : "Despesas"});
    arquivos.push({"ano" : "2026", "mes" : "11", "dia" : "17", "origem" : "Despesas"});
    arquivos.push({"ano" : "2026", "mes" : "11", "dia" : "18", "origem" : "Outros"});
    </script>
    """
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: _http_response(content)),
    ) as client:
        connector = archives.FederalPaymentArchivesConnector(
            Settings(app_env="test"), client=client
        )
        assert connector.catalogue_sha256 is None
        assert connector.catalogue_collected_at is None
        assert await connector.list_days() == frozenset({DAY})
        assert connector.catalogue_sha256 == hashlib.sha256(content).hexdigest()
        assert connector.catalogue_collected_at is not None
        content = b"<html>Arquivo temporariamente indisponivel</html>"
        with pytest.raises(archives.FederalArchiveInvalid):
            await connector.list_days()
        assert connector.catalogue_sha256 is None
        assert connector.catalogue_collected_at is None
    assert public_addresses == [archives.FEDERAL_PAYMENT_CATALOGUE] * 2


@pytest.mark.parametrize("body", [b"<html>Manutencao</html>", b"\xff"])
def test_changed_or_unreadable_catalogue_does_not_prove_missing_days(body) -> None:
    with pytest.raises(archives.FederalArchiveInvalid):
        archives._catalogue_days(body)


@pytest.mark.parametrize("status", [204, 403, 404, 429, 503])
async def test_non_200_response_cannot_be_a_valid_empty_day(public_addresses, status) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status)),
    ) as client:
        connector = archives.FederalPaymentArchivesConnector(
            Settings(app_env="test"), client=client
        )
        with pytest.raises(archives.FederalArchiveUnavailable) as captured:
            await connector.fetch(DAY, frozenset({PAID_CNPJ}))
        assert captured.value.http_status == status


@pytest.mark.parametrize(
    "url", ["http://portaldatransparencia.gov.br/", "https://example.com/file"]
)
async def test_redirect_must_stay_on_public_official_https(public_addresses, url) -> None:
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(302, headers={"Location": url}),
        ),
    ) as client:
        connector = archives.FederalPaymentArchivesConnector(
            Settings(app_env="test"), client=client
        )
        with pytest.raises(archives.FederalArchiveUnavailable, match="domínio oficial"):
            await connector.fetch(DAY, frozenset({PAID_CNPJ}))
    assert public_addresses == [archives.federal_archive_url(DAY)]


async def test_headers_without_target_rows_prove_empty_day(public_addresses) -> None:
    content = _zip(issued=(_issued(CPF),))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: _http_response(content)),
    ) as client:
        connector = archives.FederalPaymentArchivesConnector(
            Settings(app_env="test"), client=client
        )
        result = await connector.fetch(DAY, frozenset({PAID_CNPJ}))
    assert result.records == result.issues == result.review_rows == ()


async def test_configured_expansion_limit_applies_even_to_members_outside_import(
    public_addresses,
) -> None:
    content = _zip(extras={"20261117_Despesas_Empenho.csv": bytes(range(256)) * 20})
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: _http_response(content)),
    ) as client:
        settings = Settings(app_env="test", federal_archive_max_expanded_bytes=1024)
        connector = archives.FederalPaymentArchivesConnector(settings, client=client)
        with pytest.raises(archives.FederalArchiveInvalid, match="ZIP"):
            await connector.fetch(DAY, frozenset({PAID_CNPJ}))


@pytest.mark.parametrize(
    "content",
    [
        b"<html>200 but not a ZIP</html>",
        _zip()[:-40],
        _zip(files={}),
        _zip(extras={"other/../../PAYMENT.csv": b"1"}),
        _zip(files={ISSUED_NAME: b"invalid;header", FINAL_NAME: _csv(_FINAL_HEADER, ())}),
        _zip(
            files={
                ISSUED_NAME: _csv(_ISSUED_HEADER, [(DOCUMENT,)]),
                FINAL_NAME: _csv(_FINAL_HEADER, ()),
            }
        ),
    ],
)
def test_unusable_zip_cannot_return_valid_empty_coverage(content) -> None:
    with pytest.raises(archives.FederalArchiveInvalid):
        archives._read_archive(BytesIO(content), DAY, frozenset({PAID_CNPJ}), DAY, DAY)


def test_scoped_amount_identity_and_multiple_lines_are_reviewed_conservatively() -> None:
    content = _zip(
        issued=(
            _issued(PAID_CNPJ, issued_on="16/11/2026"),
            _issued(PAID_CNPJ, document="2026OB008930"),
            _issued(PAID_CNPJ, document=OTHER_DOCUMENT, amount="0,0099"),
        ),
        finals=(_final(PAID_CNPJ, "1,00"), _final(PAID_CNPJ, "1,00")),
    )
    records, review = archives._read_archive(
        BytesIO(content), DAY, frozenset({PAID_CNPJ}), DAY, DAY
    )
    assert not records
    assert len(review) == 3
    assert review[-1].line_numbers == (2, 3)
    assert len(review[-1].raw_rows) == 2
    assert review[-1].reason.startswith("Identidade repetida")


@pytest.mark.parametrize(
    ("headers", "limit"),
    [
        ({"Content-Encoding": "gzip"}, 10),
        ({"Content-Length": "invalid"}, 10),
        ({"Content-Length": "8"}, 10),
        ({"Content-Length": "20"}, 10),
        ({}, 2),
    ],
)
async def test_http_body_rejects_forged_lengths_oversize_or_http_compression(
    headers, limit
) -> None:
    with pytest.raises(archives.FederalArchiveInvalid):
        async for _ in archives._bounded_body(_http_response(b"abc", headers=headers), limit):
            pass
