"""Search terms, published agency sphere, and reproducible collection filters."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.connectors.base import RawProcurement

GOVERNMENT_SPHERES = {"F": "Federal", "E": "Estadual", "M": "Municipal", "D": "Distrital"}
OBJECT_GROUPS = {
    "bens": "Bens",
    "servicos": "Serviços",
    "engenharia": "Serviços de engenharia",
    "obras": "Obras",
}
COLLECTION_MODALITIES = (
    ("pregao_eletronico", "Pregão eletrônico"),
    ("pregao_presencial", "Pregão presencial"),
    ("concorrencia_eletronica", "Concorrência eletrônica"),
    ("concorrencia_presencial", "Concorrência presencial"),
    ("concurso", "Concurso"),
    ("leilao_eletronico", "Leilão eletrônico"),
    ("leilao_presencial", "Leilão presencial"),
    ("dialogo_competitivo", "Diálogo competitivo"),
    ("dispensa", "Contratação direta por dispensa"),
    ("inexigibilidade", "Contratação direta por inexigibilidade"),
)


@dataclass(frozen=True, slots=True)
class ObjectPreset:
    key: str
    label: str
    groups: tuple[str, ...]
    terms: tuple[str, ...]


def _preset(key: str, label: str, groups: str, terms: str) -> ObjectPreset:
    return ObjectPreset(key, label, tuple(groups.split()), tuple(terms.split("|")))


OBJECT_PRESETS = (
    _preset(
        "expediente",
        "Material de expediente e papelaria",
        "bens",
        "material de expediente|papelaria|papel a4|canetas|grampeadores|caixas arquivo",
    ),
    _preset(
        "material-limpeza",
        "Material de limpeza e higienização",
        "bens",
        "material de limpeza|produtos de limpeza|detergente|desinfetante|agua sanitaria",
    ),
    _preset(
        "alimentos",
        "Gêneros alimentícios",
        "bens",
        "generos alimenticios|alimentos pereciveis|arroz|feijao|biscoitos",
    ),
    _preset(
        "suprimentos-ti",
        "Material e suprimentos de informática",
        "bens",
        "suprimentos de informatica|perifericos|teclados|mouses",
    ),
    _preset(
        "equipamentos-ti",
        "Equipamentos de informática",
        "bens",
        "equipamentos de informatica|computadores|notebooks|monitores|servidores de rede",
    ),
    _preset(
        "material-hospitalar",
        "Material médico-hospitalar",
        "bens",
        "material medico hospitalar|materiais hospitalares|seringas|gazes",
    ),
    _preset("medicamentos", "Medicamentos", "bens", "medicamentos|medicamento|farmacos"),
    _preset(
        "material-eletrico",
        "Material elétrico",
        "bens",
        "material eletrico|materiais eletricos|lampadas|disjuntores|tomadas",
    ),
    _preset(
        "material-construcao",
        "Material de construção e manutenção predial",
        "bens",
        "material de construcao|materiais de construcao|cimento|ferragens|tintas",
    ),
    _preset(
        "mobiliario",
        "Mobiliário e móveis de escritório",
        "bens",
        "mobiliario|moveis de escritorio|mesas de escritorio|cadeiras de escritorio",
    ),
    _preset(
        "pecas-automotivas",
        "Pneus, peças e acessórios automotivos",
        "bens",
        "pneus|pecas automotivas|acessorios automotivos|baterias automotivas",
    ),
    _preset(
        "combustiveis",
        "Combustíveis e lubrificantes",
        "bens",
        "combustiveis|combustivel|lubrificantes|gasolina|diesel|etanol",
    ),
    _preset(
        "cozinha",
        "Equipamentos e utensílios de cozinha",
        "bens",
        "equipamentos de cozinha|utensilios de cozinha|fogoes|liquidificadores",
    ),
    _preset(
        "climatizacao",
        "Eletrodomésticos e equipamentos de climatização",
        "bens",
        "eletrodomesticos|equipamentos de climatizacao|ar condicionado|ventiladores|bebedouros|refrigeradores",
    ),
    _preset(
        "pedagogico",
        "Materiais didáticos e pedagógicos",
        "bens",
        "materiais didaticos|material didatico|materiais pedagogicos|jogos educativos|materiais escolares",
    ),
    _preset(
        "uniformes",
        "Uniformes, fardamentos e EPIs",
        "bens",
        "uniformes|fardamentos|epis|epi|equipamentos de protecao individual",
    ),
    _preset(
        "esportes",
        "Materiais esportivos",
        "bens",
        "materiais esportivos|material esportivo|bolas esportivas|equipamentos esportivos",
    ),
    _preset(
        "audiovisual",
        "Equipamentos de áudio, vídeo e comunicação",
        "bens",
        "equipamentos de audio|equipamentos de video|equipamentos de comunicacao|microfones|caixas de som|projetores",
    ),
    _preset(
        "manutencao-veiculos",
        "Serviços de manutenção de veículos",
        "servicos",
        "manutencao de veiculos|manutencao veicular|reparos mecanicos|funilaria",
    ),
    _preset(
        "manutencao-predial",
        "Serviços de manutenção predial",
        "servicos engenharia",
        "servicos de manutencao predial|reparos prediais|manutencao preventiva predial",
    ),
    _preset(
        "limpeza",
        "Serviços de limpeza e conservação",
        "servicos",
        "servicos de limpeza|limpeza e conservacao|limpeza de predios|higienizacao de ambientes",
    ),
    _preset(
        "apoio",
        "Serviços de apoio administrativo",
        "servicos",
        "apoio administrativo|copeiragem|auxiliares administrativos|servicos de recepcao",
    ),
    _preset(
        "vigilancia",
        "Serviços de vigilância e segurança patrimonial",
        "servicos",
        "servicos de vigilancia|seguranca patrimonial|vigilancia armada|vigilancia desarmada",
    ),
    _preset(
        "servicos-ti",
        "Serviços de tecnologia da informação",
        "servicos",
        "servicos de tecnologia da informacao|suporte tecnico de informatica|manutencao de sistemas|infraestrutura de ti",
    ),
    _preset(
        "impressao",
        "Serviços de impressão e reprografia",
        "servicos",
        "servicos de impressao|reprografia|outsourcing de impressao|servico de digitalizacao",
    ),
    _preset(
        "locacao-veiculos",
        "Serviços de locação de veículos",
        "servicos",
        "locacao de veiculos|locacao de veiculo|aluguel de veiculos",
    ),
    _preset(
        "transporte",
        "Serviços de transporte e logística",
        "servicos",
        "servicos de transporte|transporte de pessoas|transporte de cargas|transporte de materiais|servicos de logistica",
    ),
    _preset(
        "pragas",
        "Serviços de dedetização e controle de pragas",
        "servicos",
        "dedetizacao|controle de pragas|desinsetizacao|desratizacao|controle de vetores",
    ),
    _preset(
        "saude-ocupacional",
        "Serviços de exames e saúde ocupacional",
        "servicos",
        "saude ocupacional|exames admissionais|exames demissionais|avaliacoes ocupacionais",
    ),
    _preset(
        "instalacoes",
        "Serviços de engenharia e manutenção de instalações",
        "servicos engenharia",
        "servicos de engenharia|manutencao de instalacoes|manutencao hidraulica|manutencao eletrica|sistemas prediais",
    ),
)

_GROUP_TERMS = {
    "bens": ("aquisição de bens", "aquisição de equipamentos", "fornecimento de materiais"),
    "servicos": ("prestação de serviços", "contratação de serviços"),
    "engenharia": ("serviço comum de engenharia", "serviços comuns de engenharia"),
    "obras": (
        "execução de obra",
        "execução de obras",
        "contratação de obra",
        "contratação de obras",
        "pavimentação",
        "construção de",
        "reforma de prédio",
        "reforma de escola",
        "ampliação de prédio",
    ),
}
_PRESETS_BY_KEY = {item.key: item for item in OBJECT_PRESETS}
_SPHERE_ALIASES = {label.casefold(): code for code, label in GOVERNMENT_SPHERES.items()}


def normalized_search_text(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value.casefold())
    text = "".join(character for character in decomposed if not unicodedata.combining(character))
    return " ".join(re.findall(r"[a-z0-9]+", text))


def indexed_search_text(value: str | None) -> str | None:
    """Pad normalized text so database searches can match complete words."""
    text = normalized_search_text(value) if value else ""
    return f" {text} " if text else None


def published_object_complement(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("informacaoComplementar")
    return (value.strip() or None) if isinstance(value, str) else None


def published_sphere(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if value.upper() in GOVERNMENT_SPHERES:
        return value.upper()
    return _SPHERE_ALIASES.get(value.casefold())


def published_scope_header(raw: RawProcurement) -> dict[str, str | None]:
    return {
        "object_complement": raw.object_complement or published_object_complement(raw.raw_payload),
        "government_sphere": published_sphere(raw.government_sphere),
    }


@dataclass(frozen=True, slots=True)
class ObjectScope:
    query: str = ""
    presets: tuple[str, ...] = ()
    group: str = ""
    government_sphere: str = ""
    preset_terms: tuple[str, ...] = ()
    group_terms: tuple[str, ...] = ()

    @classmethod
    def build(
        cls,
        object_query: str | None = None,
        object_presets: Iterable[str] = (),
        object_group: str | None = None,
        government_sphere: str | None = None,
    ) -> ObjectScope | None:
        query = (object_query or "").strip()
        if len(query) > 200 or (query and not normalized_search_text(query)):
            raise ValueError("Informe até 200 caracteres válidos para buscar o objeto.")
        selected = set(object_presets)
        if selected.difference(_PRESETS_BY_KEY):
            raise ValueError("Selecione objetos da lista de opções.")
        group = (object_group or "").strip()
        if group and group not in OBJECT_GROUPS:
            raise ValueError("Selecione um grupo de objetos válido.")
        sphere = published_sphere(government_sphere) if government_sphere else None
        if government_sphere and sphere is None:
            raise ValueError("Selecione uma esfera governamental válida.")
        if not (query or selected or group or sphere):
            return None

        presets = tuple(item.key for item in OBJECT_PRESETS if item.key in selected)
        preset_terms = (term for key in presets for term in _PRESETS_BY_KEY[key].terms)
        group_terms = (
            term for item in OBJECT_PRESETS if group in item.groups for term in item.terms
        )
        return cls(
            query=query,
            presets=presets,
            group=group,
            government_sphere=sphere or "",
            preset_terms=_normalized_terms(preset_terms),
            group_terms=_normalized_terms((*_GROUP_TERMS.get(group, ()), *group_terms)),
        )

    def as_json(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "presets": list(self.presets),
            "group": self.group,
            "government_sphere": self.government_sphere,
            "preset_terms": list(self.preset_terms),
            "group_terms": list(self.group_terms),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ObjectScope:
        """Use effective terms from the original collection rather than the current catalog."""
        return cls(
            query=value["query"],
            presets=tuple(value["presets"]),
            group=value["group"],
            government_sphere=value["government_sphere"],
            preset_terms=tuple(value["preset_terms"]),
            group_terms=tuple(value["group_terms"]),
        )

    def matches(self, sphere: object, *texts: str | None) -> bool:
        return self.matches_sphere(sphere) and self.matches_text(*texts)

    def matches_sphere(self, sphere: object) -> bool:
        return not self.government_sphere or published_sphere(sphere) == self.government_sphere

    def matches_text(self, *texts: str | None) -> bool:
        if not (self.query or self.preset_terms or self.group_terms):
            return True
        fields = [
            f" {normalized_search_text(value)} "
            for value in texts
            if isinstance(value, str) and value
        ]
        if self.query and not _matches_terms(fields, (normalized_search_text(self.query),)):
            return False
        return _matches_terms(fields, self.preset_terms) and _matches_terms(
            fields, self.group_terms
        )


def _normalized_terms(terms: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(normalized_search_text(term) for term in terms))


def _matches_terms(fields: list[str], terms: tuple[str, ...]) -> bool:
    return not terms or any(f" {term} " in field for term in terms for field in fields)
