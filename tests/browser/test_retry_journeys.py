"""Retentativas pelo navegador preservam contadores e não chamam provedores reais."""

import asyncio
import sqlite3
from types import SimpleNamespace

import pytest

from tests.support.processes import wait_for

pytestmark = pytest.mark.browser


def test_document_retry_and_ai_cancel_retry_forms(browser, runtime):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    from app.config import Settings
    from app.database import create_database_engine, create_session_factory
    from tests.integration.test_document_retry import seed as seed_documents
    from tests.integration.test_llm_queue import seed as seed_ai

    async def seed():
        engine = create_database_engine(runtime.env["DATABASE_URL"])
        context = SimpleNamespace(sessions=create_session_factory(engine))
        parent, docs = await seed_documents(context)
        settings = Settings(
            _env_file=None,
            llm_enabled=True,
            llm_provider="openai",
            llm_model="controlled",
            llm_api_key="synthetic",
            llm_min_chunk_characters=1,
            raw_data_storage_path=runtime.root / "raw",
        )
        await seed_ai(context, settings)
        await engine.dispose()
        return parent, docs[0], docs[1]

    parent, document, deferred = asyncio.run(seed())
    browser.get(runtime.url + f"/crawls/{parent}/documents")
    button = browser.find_element(By.CSS_SELECTOR, "form[action$='/retry-documents'] button")
    assert button.is_enabled()
    button.click()

    def processed():
        with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
            return (
                db.execute(
                    "select extraction_status from documents where id=?", (str(document),)
                ).fetchone()[0]
                == "extracted"
            )

    wait_for(processed, seconds=50)
    with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
        assert (
            db.execute(
                "select extraction_attempts from documents where id=?", (str(document),)
            ).fetchone()[0]
            == 0
        )
        assert db.execute(
            "select extraction_status,extraction_attempts from documents where id=?",
            (str(deferred),),
        ).fetchone() == ("failed", 1)
    (runtime.root / "enable-ai-controls").touch()
    browser.get(runtime.url + "/crawls")
    wait = WebDriverWait(browser, 30)
    wait.until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "form[action*='/crawls/llm/'][action$='/cancel'] button")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "form[action*='/crawls/llm/'][action$='/retry'] button")
        )
    ).click()

    def retry_confirmed():
        with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
            return (
                db.execute("select count(*) from llm_work_items where status='pending'").fetchone()[
                    0
                ]
                == 1
            )

    wait_for(retry_confirmed)
    with sqlite3.connect(runtime.root / "suite.sqlite3") as db:
        assert (
            db.execute("select count(*) from llm_work_items where status='pending'").fetchone()[0]
            == 1
        )
        assert db.execute("select count(*) from llm_attempts").fetchone()[0] == 0
