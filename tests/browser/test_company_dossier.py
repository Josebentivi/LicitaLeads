"""The company journey uses only the temporary runtime and controlled sources."""

import asyncio
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.browser


def test_company_sections_review_draft_and_rescore(browser, runtime):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import Select, WebDriverWait

    from app.database import create_database_engine, create_session_factory
    from tests.integration.test_company_overview import CNPJ, seed

    async def prepare():
        engine = create_database_engine(runtime.env["DATABASE_URL"])
        try:
            return await seed(SimpleNamespace(sessions=create_session_factory(engine)))
        finally:
            await engine.dispose()

    ids = asyncio.run(prepare())

    def click(selector):
        element = browser.find_element(By.CSS_SELECTOR, selector)
        browser.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        element.click()

    browser.get(runtime.url + f"/empresas/{CNPJ}")
    wait = WebDriverWait(browser, 20)
    assert "Visão completa da empresa" in browser.page_source
    assert not browser.find_elements(By.ID, "company-list-contracts")
    browser.find_element(By.CSS_SELECTOR, "#company-contracts > summary").click()
    wait.until(lambda d: d.find_elements(By.ID, "company-list-contracts"))
    browser.find_element(By.CSS_SELECTOR, "#company-contracts .company-record summary").click()
    wait.until(lambda d: "R$ 200.000,00" in d.find_element(By.ID, "company-contracts").text)
    browser.find_element(By.CSS_SELECTOR, "#company-leads .company-record summary").click()
    selector = f"#company-detail-data-leads-{ids['lead']}"
    wait.until(lambda d: d.find_elements(By.CSS_SELECTOR, selector + " form[action$='/review']"))
    form = browser.find_element(By.CSS_SELECTOR, selector + " form[action$='/review']")
    Select(form.find_element(By.NAME, "decision")).select_by_value("approved")
    form.find_element(By.NAME, "reviewer").send_keys("Revisor do navegador")
    form.find_element(By.CSS_SELECTOR, "button[type=submit]").click()
    wait.until(lambda d: "Revisor do navegador" in d.find_element(By.CSS_SELECTOR, selector).text)
    click(selector + " form[action$='/draft'] button")
    wait.until(
        lambda d: (
            "Prezados" in d.find_element(By.CSS_SELECTOR, selector).text
            or "Rascunhos · 1" in d.find_element(By.CSS_SELECTOR, selector).text
        )
    )
    click(selector + " form[action$='/rescore'] button")
    wait.until(lambda d: "2.0" in d.find_element(By.CSS_SELECTOR, selector).text)
    assert browser.current_url == runtime.url + f"/empresas/{CNPJ}"
    assert browser.find_element(By.ID, "company-contracts").get_attribute("open") is not None
    import sqlite3

    with sqlite3.connect(runtime.root / "suite.sqlite3") as connection:
        assert (
            connection.execute(
                "select count(*) from lead_reviews where reviewer='Revisor do navegador'"
            ).fetchone()[0]
            == 1
        )
        assert connection.execute("select count(*) from outreach_drafts").fetchone()[0] == 1
        assert connection.execute("select count(*) from lead_score_revisions").fetchone()[0] == 1
    import tempfile
    from pathlib import Path

    browser.execute_script("window.scrollTo(0, 0)")
    browser.save_screenshot(str(Path(tempfile.gettempdir()) / "licitaleads-company-preview.png"))
    browser.set_window_size(390, 844)
    browser.get(runtime.url + f"/empresas/{CNPJ}")
    assert browser.execute_script(
        "return document.documentElement.scrollWidth <= window.innerWidth"
    )
