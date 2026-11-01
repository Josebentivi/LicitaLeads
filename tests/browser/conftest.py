import tempfile

import pytest

from app.services.test_browser import browser_readiness
from tests.support.processes import Runtime


@pytest.fixture
def runtime(tmp_path):
    instance = Runtime(tmp_path)
    try:
        yield instance
    finally:
        instance.close()


@pytest.fixture
def browser():
    ready = browser_readiness()
    if not ready["available"]:
        pytest.skip(ready["detail"])
    from selenium import webdriver

    with tempfile.TemporaryDirectory(prefix="licitaleads-browser-profile-") as profile:
        if ready["browser"] == "edge":
            from selenium.webdriver.edge.service import Service

            options = webdriver.EdgeOptions()
            factory = webdriver.Edge
        else:
            from selenium.webdriver.chrome.service import Service

            options = webdriver.ChromeOptions()
            factory = webdriver.Chrome
        options.binary_location = ready["binary"]
        for argument in (
            "--headless=new",
            "--disable-gpu",
            "--no-first-run",
            "--window-size=1440,1100",
            f"--user-data-dir={profile}",
        ):
            options.add_argument(argument)
        instance = factory(service=Service(executable_path=ready["driver"]), options=options)
        instance.set_page_load_timeout(30)
        try:
            yield instance
        finally:
            instance.quit()
