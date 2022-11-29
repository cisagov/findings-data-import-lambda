#!/usr/bin/env pytest -vs
"""Tests for the findings_data_import module."""

# Standard Python Libraries
import logging
import os
import sys
import json
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

# define sources of version strings
RELEASE_TAG = os.getenv("RELEASE_TAG")
PROJECT_VERSION = fdi.__version__


def test_stdout_version(capsys):
    """Verify that version string sent to stdout agrees with the module version."""
    with pytest.raises(SystemExit):
        with patch.object(sys, "argv", ["bogus", "--version"]):
            fdi.main()
    captured = capsys.readouterr()
    assert (
        captured.out == f"{PROJECT_VERSION}\n"
    ), "standard output by '--version' should agree with module.__version__"


def test_running_as_module(capsys):
    """Verify that the __main__.py file loads correctly."""
    with pytest.raises(SystemExit):
        with patch.object(sys, "argv", ["bogus", "--version"]):
            # F401 is a "Module imported but unused" warning. This import
            # emulates how this project would be run as a module. The only thing
            # being done by __main__ is importing the main entrypoint of the
            # package and running it, so there is nothing to use from this
            # import. As a result, we can safely ignore this warning.
            # cisagov Libraries
            import fdi.__main__  # noqa: F401
    captured = capsys.readouterr()
    assert (
        captured.out == f"{PROJECT_VERSION}\n"
    ), "standard output by '--version' should agree with module.__version__"


@pytest.mark.skipif(
    RELEASE_TAG in [None, ""], reason="this is not a release (RELEASE_TAG not set)"
)
def test_release_version():
    """Verify that release tag version agrees with the module version."""
    assert (
        RELEASE_TAG == f"v{PROJECT_VERSION}"
    ), "RELEASE_TAG does not match the project version"


@pytest.mark.parametrize("level", log_levels)
def test_log_levels(level):
    """Validate commandline log-level arguments."""
    with patch.object(logging.root, "handlers", []):
        assert (
            logging.root.hasHandlers() is False
        ), "root logger should not have handlers yet"
        return_code = fdi.setup_logging(level)
        assert (
            logging.root.hasHandlers() is True
        ), "root logger should now have a handler"
        assert return_code == 0, "setup_logging() should return success (0)"



def test_basic_validation():
    """Test basic low level field validation for extract_findings."""
    with open("tests/artifacts/field_map.json","r") as fm_file:
        basic_field_map = json.load(fm_file)

    print(basic_field_map)
    valid_findings = [{'RVA ID': 'RV1234','NCATS ID': 'foo'}]

    #all of the below should be invalid
    invalid_findings = [        
            {'Foo': 'bar'},
            {'RVA ID': 'RV1234','foo':'bar'},
            {'RVA ID': 'abcdef','NCATS ID': 1},
            {'NCATS ID': '1234','foo':'bar'},
            None
        
    ]
    # the valid finding should make it through and come out of extract_findings
    result = fdi.extract_findings(valid_findings,field_map_dict=basic_field_map)
    assert len(result) == 1

    # none of the invalid should make it through
    result = fdi.extract_findings(invalid_findings,field_map_dict=basic_field_map)
    assert len(result) == 0

    # combine both lists, and ensure only one output is received
    combined_findings = valid_findings + invalid_findings
    result = fdi.extract_findings(combined_findings,field_map_dict=basic_field_map)
    assert len(result) == 1

    #finally, test our sample json and expect 25 findings

    with open("tests/artifacts/expected_dhs_v1.json","r") as json_file:
        expected_findings = json.load(json_file)
        result = fdi.extract_findings(findings_data=expected_findings,field_map_dict=basic_field_map)

        # right now, this ingest tool ignores everything but the specific findings. Is this intentional?
        assert len(result) == 5
    
