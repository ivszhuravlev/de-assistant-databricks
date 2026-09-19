import pytest


@pytest.fixture(autouse=True)
def adls_identity_env(monkeypatch):
    monkeypatch.setenv("DE_ASSIST_ADLS_ACCOUNT", "exampleaccount")
    monkeypatch.setenv("DE_ASSIST_ADLS_CONTAINER", "raw")
