"""Tests for ``python -m sfmc_api`` entry point."""

from unittest.mock import MagicMock, patch


@patch("sfmc_api.cli.main")
def test_main_module_calls_cli_main(mock_main: MagicMock) -> None:
    """Running ``python -m sfmc_api`` calls cli.main()."""
    import importlib

    import sfmc_api.__main__ as mod

    # Reset from any prior import, then reload
    mock_main.reset_mock()
    importlib.reload(mod)
    mock_main.assert_called_once()
