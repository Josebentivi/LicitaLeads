"""Browser-level journeys verify JavaScript/HTMX and their isolated DB effects."""

import sqlite3

import httpx
import pytest

pytestmark = pytest.mark.browser


def test_registry_selector_collect_and_htmx_progress(browser, runtime):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import Select, WebDriverWait

    (runtime.root / "registries").touch()
    browser.get(runtime.url + "/atas")
    wait = WebDriverWait(browser, 35)
    Select(browser.find_element(By.ID, "ata-collection-uf")).select_by_value("PI")
    wait.until(lambda d: "Nenhum" in d.find_element(By.ID, "ata-scope-status").text)
    assert not browser.find_element(By.CSS_SELECTOR, "[data-registry-submit]").is_enabled()
    Select(browser.find_element(By.ID, "ata-collection-uf")).select_by_value("MA")
    wait.until(lambda d: len(Select(d.find_element(By.ID, "ata-collection-scope")).options) > 1)
    Select(browser.find_element(By.ID, "ata-collection-scope")).select_by_value(
        "MA:00000000000191:980921"
    )
    Select(browser.find_element(By.ID, "ata-collection-source")).select_by_value("pncp")
    form = browser.find_element(By.CSS_SELECTOR, "form[data-registry-collection]")
    form.find_element(By.NAME, "days").clear()
    form.find_element(By.NAME, "days").send_keys("7")
    form.find_element(By.CSS_SELECTOR, "button[type=submit]").click()
    wait.until(EC.url_contains("/crawls"))
    wait.until(
        lambda d: (
            "Ata" in d.find_element(By.TAG_NAME, "body").text
            or "Atas" in d.find_element(By.TAG_NAME, "body").text
        )
    )
    from tests.support.processes import wait_for

    def persisted():
        with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
            return db.execute("select count(*) from price_registries").fetchone()[0] == 1

    wait_for(persisted)
    with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
        filters = db.execute(
            "select filters from crawl_runs order by created_at desc limit 1"
        ).fetchone()[0]
    assert '"days": 7' in filters and "980921" in filters and "pncp" in filters
    browser.get(runtime.url + "/atas")
    assert "0001" in browser.find_element(By.TAG_NAME, "body").text


def test_crawl_cancel_continue_history_and_evidence(browser, runtime):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    wait = WebDriverWait(browser, 35)
    (runtime.root / "hold").touch()
    browser.get(runtime.url + "/dashboard")
    form = browser.find_element(By.CSS_SELECTOR, "form[action='/crawls/run']")
    form.find_element(By.NAME, "process_documents").click()
    form.find_element(By.CSS_SELECTOR, "button[type=submit]").click()
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "form[action$='/cancel'] button")))
    wait.until(lambda d: d.find_element(By.CSS_SELECTOR, "[hx-get='/crawls/table']"))
    browser.find_element(By.CSS_SELECTOR, "form[action$='/cancel'] button").click()
    wait.until(EC.alert_is_present()).accept()
    wait.until(lambda d: "Cancelada" in d.find_element(By.TAG_NAME, "body").text)
    (runtime.root / "hold").unlink()
    browser.get(runtime.url + "/dashboard")
    form = browser.find_element(By.CSS_SELECTOR, "form[action='/crawls/run']")
    form.find_element(By.NAME, "process_documents").click()
    form.find_element(By.CSS_SELECTOR, "button[type=submit]").click()
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
