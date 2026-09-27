from app.connectors.capabilities import (
    CapabilityAvailability,
    get_source_capabilities,
    render_capability_matrix_markdown,
)


def test_capability_registry_returns_independent_models() -> None:
    first = get_source_capabilities()
    second = get_source_capabilities()

    assert {source.id for source in first} == {"pncp", "compras_gov"}
    assert first[0] is not second[0]
    pncp = next(source for source in first if source.id == "pncp")
    events = next(item for item in pncp.capabilities if item.name == "Eventos da sessão")
    assert events.availability is CapabilityAvailability.NOT_SUPPORTED
    assert "historico" in events.notes


def test_markdown_matrix_is_rendered_from_the_same_registry() -> None:
    capabilities = get_source_capabilities()
    markdown = render_capability_matrix_markdown(capabilities)

    assert "GET /api/source-capabilities" in markdown
    for source in capabilities:
        assert f"## {source.name}" in markdown
        for capability in source.capabilities:
            assert capability.name in markdown
            for endpoint in capability.endpoints:
                assert endpoint.path in markdown
