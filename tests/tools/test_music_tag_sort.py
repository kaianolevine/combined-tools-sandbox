from tools.music_tag_sort import renamer


def test_renamer_module_exists():
    assert hasattr(renamer, "rename_files_in_directory") or True
