#!/usr/bin/env pytest -vs
"""Tests for the findings_data_import module."""

# Standard Python Libraries
import sys
from unittest.mock import patch

# Third-Party Libraries
import pytest

# cisagov Libraries
from fdi import findings_data_import as fdi

log_levels = (
    "debug",
    "info",
    "warning",
    "error",
    "critical",
    pytest.param("critical2", marks=pytest.mark.xfail),
)

PROJECT_VERSION = fdi.__version__


def test_stdout_version(capsys):
    """Verify that the version string sent to stdout agrees with the module version."""
    with pytest.raises(SystemExit):
        with patch.object(sys, "argv", ["exe_name", "--version"]):
            fdi.main()
            captured = capsys.readouterr()
            assert (
                captured.out == f"{PROJECT_VERSION}\n"
            ), "standard output by '--version' should agree with module.__version__"
