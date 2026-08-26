def test_package_has_version() -> None:
    from gds_pipeline import __version__

    assert __version__ == "0.1.0"
