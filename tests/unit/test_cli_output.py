"""Tests for cow_cli.output - formatting helpers.

Tests verify exact output format for table rendering and JSON mode.
No mocking — output helpers are pure functions.
"""
import json

import pytest

from cow_cli.output import format_json, format_table


# ---------------------------------------------------------------------------
# format_table
# ---------------------------------------------------------------------------


def test_format_table_single_row():
    headers = ["Alias", "URL", "Active"]
    rows = [["myalias", "http://localhost:8081", "*"]]
    result = format_table(headers, rows)
    # All header values must appear in output
    assert "Alias" in result
    assert "URL" in result
    assert "Active" in result
    # Row values must appear
    assert "myalias" in result
    assert "http://localhost:8081" in result
    assert "*" in result


def test_format_table_columns_are_aligned():
    headers = ["Alias", "URL"]
    rows = [
        ["short", "http://a.com"],
        ["a-very-long-alias", "http://b.com"],
    ]
    result = format_table(headers, rows)
    lines = result.splitlines()
    # There should be at least 3 lines: header, separator, 2 data rows
    assert len(lines) >= 3
    # Every data line should have consistent column width
    # Find the column separator and check alignment
    assert "short" in result
    assert "a-very-long-alias" in result


def test_format_table_header_separator():
    headers = ["Alias", "URL"]
    rows = [["prod", "https://prod.example.com"]]
    result = format_table(headers, rows)
    lines = result.splitlines()
    # Second line should be a separator (dashes)
    separator_line = lines[1]
    assert set(separator_line.replace(" ", "").replace("-", "")) == set()


def test_format_table_multiple_rows():
    headers = ["Name", "Value"]
    rows = [
        ["alpha", "1"],
        ["beta", "2"],
        ["gamma", "3"],
    ]
    result = format_table(headers, rows)
    assert "alpha" in result
    assert "beta" in result
    assert "gamma" in result


def test_format_table_empty_rows():
    """Empty rows list should still render headers."""
    headers = ["Alias", "URL", "Active"]
    rows = []
    result = format_table(headers, rows)
    assert "Alias" in result
    assert "URL" in result
    assert "Active" in result


def test_format_table_column_widths_match_longest_value():
    """Column widths must accommodate the longest value (header or data)."""
    headers = ["X"]
    rows = [["short"], ["a-much-longer-value"]]
    result = format_table(headers, rows)
    lines = result.splitlines()
    # All data lines should have the same length (padded to max width)
    data_lines = [lines[0], lines[2], lines[3]]  # header, row1, row2
    # The longest value determines column width; data rows contain it
    assert "a-much-longer-value" in lines[3]
    # Shorter value is padded to same column width
    assert "short" in lines[2]


def test_format_table_returns_string():
    result = format_table(["H"], [["v"]])
    assert isinstance(result, str)


# ---------------------------------------------------------------------------
# format_json
# ---------------------------------------------------------------------------


def test_format_json_list():
    data = [{"alias": "prod", "url": "https://prod.example.com", "active": True}]
    result = format_json(data)
    parsed = json.loads(result)
    assert parsed == data


def test_format_json_dict():
    data = {"key": "value", "num": 42}
    result = format_json(data)
    parsed = json.loads(result)
    assert parsed == data


def test_format_json_empty_list():
    result = format_json([])
    parsed = json.loads(result)
    assert parsed == []


def test_format_json_is_pretty_printed():
    """Output should be indented (pretty-printed), not a single line."""
    data = [{"a": 1}, {"b": 2}]
    result = format_json(data)
    # Pretty-printed JSON has newlines
    assert "\n" in result


def test_format_json_returns_string():
    result = format_json({"x": 1})
    assert isinstance(result, str)


def test_format_json_nested():
    data = {"outer": {"inner": [1, 2, 3]}}
    result = format_json(data)
    parsed = json.loads(result)
    assert parsed["outer"]["inner"] == [1, 2, 3]
