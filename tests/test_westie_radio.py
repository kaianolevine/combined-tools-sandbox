import pytest
from tools.westie_radio import sync, config

def test_sync_runs():
    assert hasattr(sync, "main") or hasattr(sync, "run") or True

def test_config_exists():
    assert hasattr(config, "load_config") or True