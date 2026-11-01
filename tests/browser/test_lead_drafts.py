"""The standalone lead page creates drafts under the production browser policy."""

import asyncio
import sqlite3
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.browser


@pytest.mark.parametrize("javascript_enabled", [True, False])
def test_lead_draft_button_persists_and_displays_draft(browser, runtime, javascript_enabled):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    from app.database import create_database_engine, create_session_factory
    from tests.integration.test_api_leads import _full_graph

    async def prepare():
        engine = create_database_engine(runtime.env["DATABASE_URL"])
        try:
            return await _full_graph(SimpleNamespace(sessions=create_session_factory(engine)))
        finally:
            await engine.dispose()

    ids = asyncio.run(prepare())
    browser.execute_cdp_cmd(
        "Emulation.setScriptExecutionDisabled", {"value": not javascript_enabled}
    )
    browser.get(runtime.url + f"/leads/{ids.lead}")
    browser.find_element(By.XPATH, "//button[normalize-space()='Gerar rascunho']").click()
    wait = WebDriverWait(browser, 15)
    draft = wait.until(lambda d: d.find_element(By.CSS_SELECTOR, "pre.draft"))
    assert "Empresa Exemplo Ltda." in draft.text
    assert "Certidão exigida não apresentada." in draft.text
    assert "https://pncp.gov.br/ata/99" in draft.text
    assert browser.current_url == runtime.url + f"/leads/{ids.lead}#rascunho"
    browser.refresh()
    assert (
        "Certidão exigida não apresentada."
        in browser.find_element(By.CSS_SELECTOR, "pre.draft").text
    )
    with sqlite3.connect(runtime.root / "suite.sqlite3") as connection:
        assert connection.execute("select count(*) from outreach_drafts").fetchone()[0] == 1
        assert connection.execute("select approved,sent from outreach_drafts").fetchone() == (0, 0)
        assert connection.execute("select lead_status from leads").fetchone()[0] == "new"
