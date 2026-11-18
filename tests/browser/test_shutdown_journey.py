"""Shutdown confirmation, keyboard access and a surviving manual-browser tab."""

import pytest

from tests.support.processes import wait_for

pytestmark = pytest.mark.browser


def test_sidebar_shutdown_confirmation_and_completion(browser, runtime):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    owner = runtime.enable_managed()
    browser.get(runtime.url + "/dashboard")
    browser.set_window_size(390, 844)
    browser.find_element(By.CSS_SELECTOR, "[data-sidebar-open]").click()
    link = browser.find_element(By.CSS_SELECTOR, ".sidebar-shutdown")
    assert link.get_attribute("aria-label") == "Encerrar a aplicação"
    link.send_keys(Keys.ENTER)
    wait = WebDriverWait(browser, 15)
    wait.until(EC.presence_of_element_located((By.NAME, "token")))
    assert "Os dados já gravados serão preservados" in browser.page_source
    assert owner.poll() is None
    browser.save_screenshot(str(runtime.root / "encerrar-mobile.png"))
    browser.find_element(By.CSS_SELECTOR, 'form[action="/application/shutdown"] button').click()
    try:
        wait.until(
            EC.text_to_be_present_in_element(
                (By.CSS_SELECTOR, "[data-shutdown-title]"), "Aplicação encerrada"
            )
        )
    except Exception:
        browser.save_screenshot(str(runtime.root / "encerrar-failure.png"))
        pytest.fail(browser.find_element(By.TAG_NAME, "main").text[:500])
    wait_for(lambda: owner.poll() is not None)
    # Tabs opened by a human/browser automation cannot normally close themselves.
    assert "Aplicação encerrada" in browser.page_source
    assert browser.find_element(By.CSS_SELECTOR, "[data-shutdown-status]").text
