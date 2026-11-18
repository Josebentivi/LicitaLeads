"""Real browser checks for calendar shortcuts, queue context and local preferences."""

import asyncio
import os
import sqlite3
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.browser


def _snapshot(browser, name):
    if directory := os.environ.get("LICITA_UX_SCREENSHOT_DIRECTORY"):
        browser.save_screenshot(str(Path(directory) / f"licita-ux-{name}.png"))


def test_calendar_shortcuts_and_mobile_layout(browser, runtime):
    from selenium.webdriver.common.by import By

    browser.get(runtime.url + "/dashboard")
    browser.execute_script("window.scrollTo(0, 0)")
    _snapshot(browser, "agenda")
    browser.get(runtime.url + "/crawls")
    form = browser.find_element(By.ID, "complete-collection-form")
    options = browser.find_element(By.ID, "escopo")
    assert not options.get_attribute("open")
    options.find_element(By.TAG_NAME, "summary").click()
    options.find_element(By.CSS_SELECTOR, "[data-period='30']").click()
    start = date.fromisoformat(options.find_element(By.NAME, "start_date").get_attribute("value"))
    end = date.fromisoformat(options.find_element(By.NAME, "end_date").get_attribute("value"))
    assert (end - start).days == 29
    assert "30 dia(s)" in options.find_element(By.CSS_SELECTOR, "[data-period-summary]").text
    options.find_element(By.CSS_SELECTOR, "[data-period=month]").click()
    selected = options.find_element(By.NAME, "start_date").get_attribute("value")
    assert selected.endswith("-01")
    (runtime.root / "hold").touch()
    browser.execute_script("arguments[0].requestSubmit(); arguments[0].requestSubmit();", form)
    from selenium.webdriver.support.ui import WebDriverWait

    WebDriverWait(browser, 15).until(lambda driver: "#coleta-" in driver.current_url)
    with sqlite3.connect(runtime.root / "suite.sqlite3") as connection:
        runs = connection.execute(
            "select json_extract(filters,'$.start_date') from crawl_runs where connector='complete'"
        ).fetchall()
        assert runs == [(selected,)]
    _snapshot(browser, "coletas")
    browser.execute_cdp_cmd(
        "Emulation.setDeviceMetricsOverride",
        {"width": 390, "height": 844, "deviceScaleFactor": 1, "mobile": True},
    )
    try:
        browser.get(runtime.url + "/atas")
        assert browser.execute_script(
            "return window.innerWidth === 390 && document.documentElement.scrollWidth <= 391"
        )
        _snapshot(browser, "mobile")
    finally:
        browser.execute_cdp_cmd("Emulation.clearDeviceMetricsOverride", {})


def test_saved_search_review_context_and_copy_feedback(browser, runtime):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import Select, WebDriverWait

    from app.database import create_database_engine, create_session_factory
    from tests.integration.test_api_leads import _full_graph

    async def prepare():
        engine = create_database_engine(runtime.env["DATABASE_URL"])
        try:
            return await _full_graph(SimpleNamespace(sessions=create_session_factory(engine)))
        finally:
            await engine.dispose()

    ids = asyncio.run(prepare())
    context = "/leads?q=Empresa"
    browser.get(runtime.url + context)
    _snapshot(browser, "queue")
    browser.find_element(By.CSS_SELECTOR, "[data-save-search]").click()
    assert (
        "Abrir busca guardada" in browser.find_element(By.CSS_SELECTOR, "[data-saved-search]").text
    )
    browser.find_element(By.CSS_SELECTOR, "tbody a").click()
    _snapshot(browser, "analysis")
    back = browser.find_element(By.CSS_SELECTOR, "nav[aria-label='Navegação da análise'] a")
    assert back.get_attribute("href") == runtime.url + context
    form = browser.find_element(By.CSS_SELECTOR, "form.review-form")
    Select(form.find_element(By.NAME, "decision")).select_by_value("approved")
    form.find_element(By.NAME, "reviewer").send_keys("Analista de teste")
    form.find_element(By.CSS_SELECTOR, "button[type=submit]").click()
    wait = WebDriverWait(browser, 15)
    wait.until(lambda driver: "Revisão registrada" in driver.find_element(By.TAG_NAME, "body").text)
    assert (
        browser.execute_script("return localStorage.getItem('licitalead:reviewer')")
        == "Analista de teste"
    )
    browser.find_element(By.XPATH, "//button[normalize-space()='Gerar rascunho']").click()
    wait.until(lambda driver: driver.find_element(By.CSS_SELECTOR, "[data-copy-target]"))
    browser.find_element(By.CSS_SELECTOR, "[data-copy-target]").click()
    wait.until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, "[data-copy-feedback]").text.strip()
    )
    browser.get(runtime.url + f"/leads/{ids.lead}")
    assert browser.find_element(By.NAME, "reviewer").get_attribute("value") == "Analista de teste"
