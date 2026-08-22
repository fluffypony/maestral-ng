from maestral.constants import EXCLUDED_FILE_NAMES, ROOT_MARKER_FILE


def test_macos_metadata_names_are_excluded() -> None:
    assert ".ds_store" in EXCLUDED_FILE_NAMES
    assert ".fseventsd" in EXCLUDED_FILE_NAMES
    assert ".ds_tore" not in EXCLUDED_FILE_NAMES
    assert ".fseventd" not in EXCLUDED_FILE_NAMES


def test_root_marker_is_excluded() -> None:
    assert ROOT_MARKER_FILE in EXCLUDED_FILE_NAMES
