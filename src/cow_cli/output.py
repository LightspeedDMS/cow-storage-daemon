"""Output formatting helpers for cow-cli.

format_table -- aligned column table with header separator
format_json  -- pretty-printed JSON for --json mode
"""
import json
from typing import List


def format_table(headers: List[str], rows: List[List[str]]) -> str:
    """Render a left-aligned column table.

    Args:
        headers: Column header strings.
        rows: Each inner list is one row of string values.

    Returns:
        Multi-line string with header row, dash separator, and data rows.
        Column widths are sized to the widest value (header or data).
    """
    # Compute column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(cell))

    def _render_row(values: List[str]) -> str:
        parts = []
        for i, val in enumerate(values):
            width = col_widths[i] if i < len(col_widths) else len(val)
            parts.append(val.ljust(width))
        return "  ".join(parts).rstrip()

    header_line = _render_row(headers)
    separator_line = "  ".join("-" * w for w in col_widths)
    data_lines = [_render_row(row) for row in rows]

    return "\n".join([header_line, separator_line] + data_lines)


def format_json(data) -> str:
    """Serialize data to a pretty-printed JSON string.

    Args:
        data: Any JSON-serializable value (list, dict, etc.).

    Returns:
        Indented JSON string.
    """
    return json.dumps(data, indent=2)
