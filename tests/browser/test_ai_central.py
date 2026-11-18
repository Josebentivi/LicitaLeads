import json

import pytest

pytestmark = pytest.mark.browser


def test_ai_navigation_controls_and_small_screen(browser, runtime, tmp_path):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    browser.get(runtime.url + "/ia")
    assert browser.find_element(By.TAG_NAME, "h1").text == "Central de IA"
    for tab, heading in [
        ("documents", "Nenhum registro"),
        ("suggestions", "Nenhum registro"),
        ("cost", "Consumo registrado"),
        ("configuration", "Configuração efetiva"),
    ]:
        browser.get(runtime.url + "/ia?tab=" + tab)
        assert heading in browser.find_element(By.TAG_NAME, "main").text
    browser.get(runtime.url + "/ia")
    browser.find_element(By.CSS_SELECTOR, 'form[action="/ia/controls/llm"] button').click()
    WebDriverWait(browser, 10).until(
        EC.text_to_be_present_in_element(
            (By.CSS_SELECTOR, 'form[action="/ia/controls/llm"] button'), "Retomar"
        )
    )
    state = json.loads((runtime.root / "scheduler.controls.json").read_text())
    assert state == {"llm": True, "collections": False}
    browser.get(runtime.url + "/ia?tab=documents&source=pncp")
    assert browser.find_element(By.NAME, "source").get_attribute("value") == "pncp"
    browser.set_window_size(430, 950)
    assert browser.find_element(By.CSS_SELECTOR, "#ai-filters button").is_displayed()
    browser.save_screenshot(str(tmp_path / "central-ia-mobile.png"))
