"""Browser-level journeys verify JavaScript/HTMX and their isolated DB effects."""

import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest

pytestmark = pytest.mark.browser


def test_complete_collection_updates_registry_catalog(browser, runtime):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    (runtime.root / "registries").touch()
    browser.get(runtime.url + "/atas")
    assert len(browser.find_elements(By.CSS_SELECTOR, "#complete-collection-form button")) == 1
    assert not browser.find_elements(By.CSS_SELECTOR, "form[action='/atas/collect']")
    browser.get(runtime.url + "/crawls")
    today = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    browser.execute_script(
        "document.querySelector('#escopo [name=start_date]').value=arguments[0];"
        "document.querySelector('#escopo [name=end_date]').value=arguments[1];",
        today.replace(month=1, day=1).isoformat(),
        today.isoformat(),
    )
    browser.find_element(By.CSS_SELECTOR, "#complete-collection-form button").click()
    wait = WebDriverWait(browser, 35)
    wait.until(lambda d: "#coleta-" in d.current_url)
    from tests.support.processes import wait_for

    def persisted():
        with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
            return db.execute("select count(*) from price_registries").fetchone()[0] == 1

    wait_for(persisted)
    with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
        filters = db.execute(
            "select filters from crawl_runs where json_extract(filters,'$.mode')='price_registries'"
        ).fetchone()[0]
    assert (
        '"start_date":' in filters
        and '"end_date":' in filters
        and "980921" in filters
        and "pncp" in filters
    )
    wait.until(
        lambda d: d.find_element(By.CSS_SELECTOR, ".complete-progress a[href^='/leads?run=']")
    )
    browser.get(runtime.url + "/atas")
    assert "0001" in browser.find_element(By.TAG_NAME, "body").text


def test_crawl_cancel_continue_history_and_evidence(browser, runtime):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    wait = WebDriverWait(browser, 35)
    (runtime.root / "hold").touch()
    browser.get(runtime.url + "/dashboard")
    browser.find_element(By.CSS_SELECTOR, "#complete-collection-form button").click()
    cancel = (By.CSS_SELECTOR, ".complete-progress form[action$='/cancel'] button")
    wait.until(EC.presence_of_element_located(cancel))
    wait.until(lambda d: d.find_element(By.CSS_SELECTOR, "[hx-get='/crawls/table']"))
    browser.find_element(*cancel).click()
    wait.until(EC.alert_is_present()).accept()
    wait.until(lambda d: "Cancelada" in d.find_element(By.TAG_NAME, "body").text)
    (runtime.root / "hold").unlink()
    response = httpx.post(
        runtime.url + "/crawls/run",
        headers={"Origin": runtime.url},
        data={"uf": "MA", "connector": "pncp", "days": "7"},
    )
    assert response.status_code == 303
    browser.get(runtime.url + "/crawls")
    from tests.support.processes import wait_for

    def limited_run():
        with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
            row = db.execute(
                "select id from crawl_runs where status='partial' order by created_at desc limit 1"
            ).fetchone()
            return row[0] if row else None

    limited_id = wait_for(limited_run)
    wait.until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, f"form[action='/crawls/{limited_id}/continue'] button")
        )
    )
    browser.find_element(
        By.CSS_SELECTOR, f"form[action='/crawls/{limited_id}/continue'] button"
    ).click()

    def resumed():
        with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
            return (
                db.execute(
                    "select count(*) from procurements where external_id != 'seed'"
                ).fetchone()[0]
                >= 2
            )

    wait_for(resumed)
    with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
        identifier = db.execute("select id from procurements where external_id='id-1'").fetchone()[
            0
        ]
    browser.get(runtime.url + "/procurements/" + identifier)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[id^='field-history-'] h2")))
    assert (
        "Histórico de alterações"
        in browser.find_element(By.CSS_SELECTOR, "[id^='field-history-'] h2").text
    )
    evidence_id = (runtime.root / "evidence-id").read_text()
    downloaded = browser.execute_async_script(
        "const done=arguments[arguments.length-1]; fetch('/api/evidence/' + arguments[0]).then(r=>r.json()).then(e=>fetch(e.document_view_url)).then(r=>r.text()).then(done);",
        evidence_id,
    )
    assert "Evidência sintética" in downloaded
    # Unknown resources and rejected retries preserve the confirmed records.
    response = httpx.post(
        runtime.url + "/crawls/00000000-0000-0000-0000-000000000001/retry-documents"
    )
    assert response.status_code == 404


def test_tests_screen_historical_progress_partial_cancel_and_current(browser, runtime):
    import json
    from datetime import UTC, datetime

    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    now = datetime.now(UTC).isoformat()
    (runtime.root / "test-report.json").write_text(
        json.dumps(
            {
                "run_id": "legacy",
                "mode": "full",
                "started_at": now,
                "finished_at": now,
                "results": [],
            }
        )
    )
    browser.get(runtime.url + "/testes")
    assert "Relatório histórico" in browser.page_source
    browser.find_element(
        By.CSS_SELECTOR, "form[action='/testes/executar'] input[value='full']"
    ).find_element(By.XPATH, "..").find_element(By.TAG_NAME, "button").click()
    wait = WebDriverWait(browser, 25)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-test-progress]")))
    browser.find_element(By.CSS_SELECTOR, "form[action='/testes/cancelar'] button").click()
    wait.until(lambda d: "resultados parciais" in d.page_source)
    assert "Resultado parcial controlado" in browser.page_source
    browser.refresh()
    browser.find_element(By.CSS_SELECTOR, "form[action='/testes/executar'] button").click()
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-report-current]")))
