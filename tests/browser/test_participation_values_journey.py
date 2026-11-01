"""The corrected facts are visible in procurement and company journeys."""

import asyncio
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.browser


def test_reconciled_lot_values_and_invalid_history(browser, runtime):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    from app.database import create_database_engine, create_session_factory
    from tests.integration.test_participation_integrity import apply_repair, seed

    async def prepare():
        engine = create_database_engine(runtime.env["DATABASE_URL"])
        database = SimpleNamespace(engine=engine, sessions=create_session_factory(engine))
        try:
            settings, ids = await seed(database, runtime.root, reviewed=True, alias=True)
            await apply_repair(database, settings)
            return ids
        finally:
            await engine.dispose()

    ids = asyncio.run(prepare())
    wait = WebDriverWait(browser, 20)
    browser.get(runtime.url + f"/procurements/{ids['procurement']}")
    assert "R$ 480.823,20" in browser.page_source and "R$ 1.024.545,60" in browser.page_source
    assert "DPROSMED" not in browser.page_source
    browser.find_element(By.LINK_TEXT, "Mostrar histórico e registros invalidados").click()
    assert "DPROSMED" in browser.page_source
    browser.get(runtime.url + f"/leads/{ids['lead']}")
    assert "histórico" in browser.page_source and not browser.find_elements(
        By.XPATH, "//button[normalize-space()='Gerar rascunho']"
    )
    browser.get(runtime.url + "/empresas/10258066000130")
    browser.find_element(By.CSS_SELECTOR, "#company-procurements > summary").click()
    wait.until(
        lambda d: d.find_elements(By.CSS_SELECTOR, "#company-procurements .company-record summary")
    )
    browser.find_element(By.CSS_SELECTOR, "#company-procurements .company-record summary").click()
    wait.until(lambda d: "R$ 480.823,20" in d.find_element(By.ID, "company-procurements").text)
    assert "Proposta publicada" in browser.find_element(By.ID, "company-procurements").text
