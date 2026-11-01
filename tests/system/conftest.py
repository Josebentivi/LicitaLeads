import pytest

from tests.support.processes import Runtime


@pytest.fixture
def runtime(tmp_path):
    instance = Runtime(tmp_path)
    try:
        yield instance
    finally:
        instance.close()
