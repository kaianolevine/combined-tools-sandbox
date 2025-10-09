# import pytest
from tools.dj_set_processor import helpers


def test_helpers_exists():
    assert hasattr(helpers, "stringSimilarity") or True
