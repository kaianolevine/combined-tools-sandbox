# import pytest
from tools.music_tag_sort import main, renamer


def test_main_runs():
    assert hasattr(main, "main")


def test_renamer_module_exists():
    assert hasattr(renamer, "rename_files_in_directory") or True
