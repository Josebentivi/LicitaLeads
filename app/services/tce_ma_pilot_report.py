"""Render the per-pregão comparison and its coverage limits in Portuguese."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from app.connectors._common import to_int
from app.connectors.pncp import parse_pncp_external_id

_MATCH_LABELS = {
    "exato": "Controle PNCP exato",
    "exato_em_coleta_parcial": "Controle PNCP exato em coleta parcial",
    "ambiguo": "Identidade ambígua",
    "candidato_sem_vinculo": "Candidato sem vínculo",
    "nao_localizado": "Não localizado no acervo consultado",
    "consulta_incompleta": "Consulta incompleta",
}


def _link_count(tce: dict[str, Any]) -> int:
    return sum(
        bool(row.get("link_documento"))
        for group in ("procedimentolicitatorio", "resultado", "contrato")
        for row in tce.get(group, [])
    )


def _origin_extras(case: dict[str, Any]) -> int | None:
    origin = case["compras_gov"]["pagina_publica"]
    if "cnpjs" not in origin or not case["pncp"]["resultado_item_1_observado"]:
        return None
    return len(set(origin["cnpjs"]) - set(case["pncp"]["cnpjs_resultado_item_1"]))


def _identity_differences(case: dict[str, Any], agency_cnpj: str) -> list[str]:
    rows = [
        row
        for row in case["tce_ma"]["procedimentos"]
        if row.get("idContratacaoPncp") == case["controle_pncp"]
    ]
    differences = []
    if any(row.get("cnpjProcedimento") not in (None, "", agency_cnpj) for row in rows):
        differences.append("CNPJ do órgão")
    if any(
        (number := to_int(row.get("numeroProcedimento"))) is not None
        and number != to_int(case["numero"])
        for row in rows
    ):
        differences.append("número do pregão")
    return differences


def summarize_pilot(report: dict[str, Any]) -> dict[str, int]:
    cases = report["pregoes"]
    return {
        "pregoes": len(cases),
        "pncp_com_documentos": sum(bool(case["pncp"]["documentos"]) for case in cases),
        "pncp_com_titulo_julgamento": sum(case["pncp"]["titulo_de_julgamento"] for case in cases),
        "tce_vinculos_exatos": sum(case["tce_ma"]["vinculo"].startswith("exato") for case in cases),
        "tce_identidades_ambiguas": sum(case["tce_ma"]["vinculo"] == "ambiguo" for case in cases),
        "tce_cnpj_divergente": sum(
            "CNPJ do órgão" in _identity_differences(case, report["escopo"]["cnpj"])
            for case in cases
        ),
        "tce_numero_divergente": sum(
            "número do pregão" in _identity_differences(case, report["escopo"]["cnpj"])
            for case in cases
        ),
        "tce_candidatos_sem_vinculo": sum(
            case["tce_ma"]["vinculo"] == "candidato_sem_vinculo" for case in cases
        ),
        "tce_com_licitantes_pj": sum(
            bool(case["tce_ma"].get("cnpjs_licitantes")) for case in cases
        ),
        "tce_com_resultados": sum(bool(case["tce_ma"].get("resultado")) for case in cases),
        "tce_com_contratos": sum(bool(case["tce_ma"].get("contrato")) for case in cases),
        "tce_registros_resultado": sum(len(case["tce_ma"].get("resultado", [])) for case in cases),
        "tce_registros_contrato": sum(len(case["tce_ma"].get("contrato", [])) for case in cases),
        "tce_contratos_para_revisao": sum(
            bool(contract.get("revisao"))
            for case in cases
            for contract in case["tce_ma"].get("contrato", [])
        ),
        "tce_com_referencia_documental": sum(_link_count(case["tce_ma"]) > 0 for case in cases),
        "compras_gov_vinculos_exatos": sum(case["compras_gov"]["vinculo_exato"] for case in cases),
        "compras_gov_com_catalogo_adicional": sum(
            bool(case["compras_gov"]["itens_com_codigo_catalogo_adicional"]) for case in cases
        ),
        "compras_gov_com_categoria_divergente": sum(
            bool(case["compras_gov"]["itens_com_categoria_divergente"]) for case in cases
        ),
        "compras_gov_com_cnpj_adicional_item_1": sum(
            bool(case["compras_gov"]["cnpjs_resultado_item_1_adicionais"]) for case in cases
        ),
        "origem_paginas_confirmadas": sum(
            case["compras_gov"]["pagina_publica"]["estado"] == "confirmada" for case in cases
        ),
        "origem_itens_com_propostas": sum(
            bool(case["compras_gov"]["pagina_publica"].get("propostas_visiveis")) for case in cases
        ),
    }


def write_pilot_report(report: dict[str, Any], output: Path) -> None:
    summary, scope = report["resumo"], report["escopo"]
    state = {"completed": "concluídas", "partial": "parciais", "failed": "com falha"}.get(
        report["estado"], report["estado"]
    )
    lines = [
        "# Piloto TCE-MA em pregões de São Luís",
        "",
        f"Foram comparados **{summary['pregoes']}** pregões eletrônicos da Prefeitura de São Luís "
        f"(UASG {scope['uasg']}). Seleção pelos primeiros registros em ordem de publicação no PNCP, "
        f"entre {scope['inicio_publicacao']} e {scope['fim_publicacao']}, com desempate pelo número de controle.",
        "",
        f"Coleta iniciada em {report['inicio_coleta_utc']}. Consultas às APIs: {state}. "
        f"Identificador de auditoria: {report['execucao']}.",
        "",
        f"- **PNCP.** {summary['pncp_com_documentos']} pregões com arquivos ativos listados. "
        f"{summary['pncp_com_titulo_julgamento']} com título ou tipo de julgamento identificado na listagem.",
        f"- **TCE-MA.** {summary['tce_vinculos_exatos']} vínculos pelo controle PNCP e "
        f"{summary['tce_candidatos_sem_vinculo']} candidatos para conferência. "
        f"Identidades ambíguas: {summary['tce_identidades_ambiguas']}. Entre os registros com o "
        f"mesmo controle, há {summary['tce_cnpj_divergente']} casos com CNPJ do órgão divergente e "
        f"{summary['tce_numero_divergente']} com número do pregão divergente. "
        f"Nos vínculos exatos, {summary['tce_com_licitantes_pj']} têm licitantes com CNPJ válido, "
        f"{summary['tce_com_resultados']} têm registros de resultado, "
        f"{summary['tce_com_contratos']} têm registros de contrato e "
        f"{summary['tce_com_referencia_documental']} têm referência documental listada. "
        f"Foram {summary['tce_registros_resultado']} registros de resultado e "
        f"{summary['tce_registros_contrato']} de contrato. Contratos para revisão: "
        f"{summary['tce_contratos_para_revisao']}.",
        f"- **Compras.gov.br.** {summary['compras_gov_vinculos_exatos']} vínculos exatos na API. "
        f"{summary['compras_gov_com_catalogo_adicional']} pregões com código de catálogo adicional "
        f"em item correspondente, {summary['compras_gov_com_categoria_divergente']} com categoria "
        f"divergente entre as APIs e {summary['compras_gov_com_cnpj_adicional_item_1']} com "
        f"CNPJ adicional em resultado do item 1. Sessões com controle confirmado na página pública: "
        f"{summary['origem_paginas_confirmadas']}. Sessões com propostas visíveis no item 1: "
        f"{summary['origem_itens_com_propostas']}.",
        "",
        "| Pregão | PNCP arquivos ativos | TCE-MA | Compras.gov API | Sessão pública, item 1 |",
        "|---|---:|---|---|---|",
    ]
    for case in report["pregoes"]:
        reference = parse_pncp_external_id(case["controle_pncp"])
        link = f"https://pncp.gov.br/app/editais/{reference.cnpj}/{reference.year}/{reference.sequence}"
        pncp, tce, compras = case["pncp"], case["tce_ma"], case["compras_gov"]
        tce_label = _MATCH_LABELS[tce["vinculo"]]
        if tce["vinculo"] == "ambiguo" and (fields := _identity_differences(case, scope["cnpj"])):
            tce_label += " · controle coincide; diverge " + " e ".join(fields)
        if tce["vinculo"].startswith("exato"):
            tce_label += (
                f" · {len(tce['cnpjs_licitantes'])} PJ · {len(tce['resultado'])} resultados "
                f"· {len(tce['contrato'])} contratos · {_link_count(tce)} refs. de documentos"
            )
            if any(contract.get("revisao") for contract in tce["contrato"]):
                tce_label += " · ano de contrato para revisão"
        api_label = "Sem vínculo confirmado"
        if compras["vinculo_exato"]:
            api_label = (
                f"{len(compras['itens'])} itens · "
                f"{len(compras['itens_com_codigo_catalogo_adicional'])} códigos adicionais"
            )
            if compras["cnpjs_resultado_item_1_adicionais"] is not None:
                api_label += f" · {len(compras['cnpjs_resultado_item_1_adicionais'])} CNPJs adicionais no resultado"
            if compras["itens_com_categoria_divergente"]:
                api_label += f" · {len(compras['itens_com_categoria_divergente'])} itens com categoria divergente"
        origin = compras["pagina_publica"]
        session_label = "Sem observação direta"
        if origin["estado"] == "confirmada":
            session_label = f"[Página conferida]({origin.get('url_item') or origin['url']})"
            if origin.get("propostas_visiveis") is not None:
                session_label += f" · {origin['propostas_visiveis']} propostas visíveis"
                if (extras := _origin_extras(case)) is not None:
                    session_label += f" · {extras} CNPJs além do resultado PNCP"
            if origin.get("termo_julgamento"):
                session_label += " · julgamento/habilitação listado"
            if origin.get("arquivo_julgamento_sha256") and origin.get("arquivo_julgamento_paginas"):
                session_label += (
                    f" · PDF conferido ({origin['arquivo_julgamento_paginas']} páginas)"
                )
        elif origin["estado"] != "nao_observada":
            session_label = "Página sem confirmação"
        docs = pncp["consultas"]["documentos"]
        pncp_label = str(len(pncp["documentos"]))
        if docs["parcial"] or docs["disponibilidade"] in {"temporary_error", "access_restricted"}:
            pncp_label += " (consulta incompleta)"
        lines.append(
            f"| [{case['numero']}/{case['ano']}]({link}) | {pncp_label} | {tce_label} "
            f"| {api_label} | {session_label} |"
        )
    lines.extend(
        [
            "",
            "Os CNPJs de resultado foram comparados no item 1. Os licitantes do TCE-MA são "
            "listados no nível do procedimento. Candidatos por CNPJ do órgão, número, ano e "
            "modalidade ficam sem vínculo quando falta o controle PNCP ou a UASG. As referências "
            "documentais do TCE-MA são contagens de registros com link publicado. As propostas "
            "da sessão são os cartões visíveis na consulta assistida.",
            "",
            "O [comparativo em JSON](comparativo.json) referencia os IDs de SourceRecord no "
            "arquivo [fontes.sqlite3](fontes.sqlite3). O arquivo preserva os campos usados na "
            "comparação e o hash das respostas originais. As observações da sessão têm URL e "
            "data de conferência. O piloto coleta evidências para revisão.",
            "",
            "[API do TCE-MA](https://tce-api.apps.tcema.tc.br/swagger-ui/index.html) · "
            "[API do PNCP](https://pncp.gov.br/api/consulta/swagger-ui/index.html) · "
            "[API do Compras.gov.br](https://dadosabertos.compras.gov.br/swagger-ui/index.html)",
            "",
        ]
    )
    (output / "relatorio.md").write_text("\n".join(lines), encoding="utf-8")
