from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


DESTINATION_SCHEMA_PATTERN = re.compile(
    r"^\s*DESTINATION_SCHEMA\s*=\s*['\"]([^'\"]+)['\"]", re.MULTILINE
)
DESTINATION_ENTITY_PATTERN = re.compile(
    r"^\s*DESTINATION_ENTITY\s*=\s*['\"]([^'\"]+)['\"]", re.MULTILINE
)
SOURCE_TABLE_PATTERN = re.compile(
    r"spark\.sql\(\s*(?:f)?['\"]SELECT\s+\*\s+FROM\s+([A-Za-z0-9_\.]+)",
    re.IGNORECASE,
)
SOURCE_ASSIGNMENT_PATTERN = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*spark\.sql\(\s*(?:f)?['\"]"
    r"SELECT\s+\*\s+FROM\s+([A-Za-z0-9_\.]+)",
    re.MULTILINE | re.IGNORECASE,
)
ALIAS_BIND_PATTERN = re.compile(
    r"([A-Za-z_][A-Za-z0-9_]*)\.alias\(\s*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]\s*\)"
)
STRING_CONSTANT_PATTERN = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*['\"]([^'\"]+)['\"]\s*$",
    re.MULTILINE,
)
SELECT_PATTERN = re.compile(r"df_transformed\s*=.*?\.select\(", re.DOTALL)
F_COL_PATTERN = re.compile(
    r"F\.col\(\s*['\"]([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)['\"]\s*\)"
)
DF_BRACKET_PATTERN = re.compile(
    r"([A-Za-z_][A-Za-z0-9_]*)\s*\[\s*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]\s*\]"
)
DOT_STRING_PATTERN = re.compile(
    r"^['\"]([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)['\"]$"
)
ALIAS_CALL_PATTERN = re.compile(r"\.alias\(\s*([^)]+?)\s*\)\s*$")


@dataclass
class ColumnMapping:
    target_column: str
    source_columns: list[str]
    source_tables: list[str]
    expression: str


@dataclass
class LineageEntry:
    notebook_name: str
    notebook_path: str
    destination_schema: str
    destination_entity: str
    silver_sources: list[str]
    column_mappings: list[ColumnMapping]

    @property
    def target_table(self) -> str:
        return f"LH_Gold.{self.destination_schema}.{self.destination_entity}"


def _find_notebooks(notebooks_root: Path) -> list[Path]:
    return sorted(notebooks_root.glob("**/*.Notebook/notebook-content.py"))


def _extract_first(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    return match.group(1) if match else None


def _unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _extract_source_assignments(content: str) -> dict[str, str]:
    return {variable: table for variable, table in SOURCE_ASSIGNMENT_PATTERN.findall(content)}


def _extract_string_constants(content: str) -> dict[str, str]:
    return {name: value for name, value in STRING_CONSTANT_PATTERN.findall(content)}


def _extract_alias_table_mapping(
    content: str, var_to_table: dict[str, str]
) -> dict[str, str]:
    alias_to_table: dict[str, str] = {}
    for variable, alias in ALIAS_BIND_PATTERN.findall(content):
        table = var_to_table.get(variable)
        if table:
            alias_to_table[alias] = table
    return alias_to_table


def _extract_select_body(content: str) -> str | None:
    select_match = SELECT_PATTERN.search(content)
    if not select_match:
        return None

    start_index = select_match.end()
    depth = 1
    index = start_index

    while index < len(content):
        char = content[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return content[start_index:index]
        index += 1

    return None


def _normalize_expression(raw_expression: str) -> str:
    expression = " ".join(raw_expression.split())
    expression = expression.lstrip("\\").strip()
    expression = expression.rstrip("\\").strip()
    return expression


def _split_top_level_arguments(select_body: str) -> list[str]:
    arguments: list[str] = []
    current: list[str] = []
    depth = 0

    for char in select_body:
        if char == "(":
            depth += 1
            current.append(char)
            continue
        if char == ")":
            depth -= 1
            current.append(char)
            continue
        if char == "," and depth == 0:
            expression = _normalize_expression("".join(current))
            if expression:
                arguments.append(expression)
            current = []
            continue
        current.append(char)

    trailing = _normalize_expression("".join(current))
    if trailing:
        arguments.append(trailing)

    return arguments


def _extract_target_column(expression: str, constants: dict[str, str]) -> str:
    alias_match = ALIAS_CALL_PATTERN.search(expression)
    if alias_match:
        alias_argument = alias_match.group(1).strip()
        if alias_argument.startswith(("'", '"')) and alias_argument.endswith(("'", '"')):
            return alias_argument[1:-1]
        return constants.get(alias_argument, alias_argument)

    dot_string_match = DOT_STRING_PATTERN.match(expression)
    if dot_string_match:
        return dot_string_match.group(1).split(".")[-1]

    bracket_match = DF_BRACKET_PATTERN.search(expression)
    if bracket_match:
        return bracket_match.group(2)

    return expression.replace("\n", " ").strip()


def _extract_source_columns(expression: str) -> list[str]:
    source_columns: list[str] = []
    source_columns.extend(F_COL_PATTERN.findall(expression))

    for dataframe_var, column in DF_BRACKET_PATTERN.findall(expression):
        source_columns.append(f"{dataframe_var}.{column}")

    dot_string_match = DOT_STRING_PATTERN.match(expression.strip())
    if dot_string_match:
        source_columns.append(dot_string_match.group(1))

    return _unique_preserve_order(source_columns)


def _resolve_source_tables(
    source_columns: list[str],
    alias_to_table: dict[str, str],
    var_to_table: dict[str, str],
) -> list[str]:
    resolved_tables: list[str] = []
    for source_column in source_columns:
        prefix = source_column.split(".")[0]
        table = alias_to_table.get(prefix) or var_to_table.get(prefix)
        if table:
            resolved_tables.append(table)
    return _unique_preserve_order(resolved_tables)


def _extract_column_mappings(
    content: str,
    alias_to_table: dict[str, str],
    var_to_table: dict[str, str],
    constants: dict[str, str],
) -> list[ColumnMapping]:
    select_body = _extract_select_body(content)
    if not select_body:
        return []

    mappings: list[ColumnMapping] = []
    for expression in _split_top_level_arguments(select_body):
        target_column = _extract_target_column(expression, constants)
        source_columns = _extract_source_columns(expression)
        source_tables = _resolve_source_tables(
            source_columns, alias_to_table, var_to_table
        )
        mappings.append(
            ColumnMapping(
                target_column=target_column,
                source_columns=source_columns,
                source_tables=source_tables,
                expression=_normalize_expression(expression),
            )
        )
    return mappings


def _parse_notebook(path: Path, repo_root: Path) -> LineageEntry | None:
    content = path.read_text(encoding="utf-8")

    destination_schema = _extract_first(DESTINATION_SCHEMA_PATTERN, content)
    destination_entity = _extract_first(DESTINATION_ENTITY_PATTERN, content)
    if not destination_schema or not destination_entity:
        return None

    all_sources = SOURCE_TABLE_PATTERN.findall(content)
    silver_sources = _unique_preserve_order(
        [source for source in all_sources if source.startswith("LH_Silver.")]
    )

    var_to_table = _extract_source_assignments(content)
    constants = _extract_string_constants(content)
    alias_to_table = _extract_alias_table_mapping(content, var_to_table)
    column_mappings = _extract_column_mappings(
        content=content,
        alias_to_table=alias_to_table,
        var_to_table=var_to_table,
        constants=constants,
    )

    notebook_name = path.parent.name.replace(".Notebook", "")
    notebook_path = path.relative_to(repo_root).as_posix()

    return LineageEntry(
        notebook_name=notebook_name,
        notebook_path=notebook_path,
        destination_schema=destination_schema,
        destination_entity=destination_entity,
        silver_sources=silver_sources,
        column_mappings=column_mappings,
    )


def _to_node_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", value)


def _build_schema_markdown(schema_name: str, entries: list[LineageEntry]) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines: list[str] = []
    lines.append(f"# Data Lineage – Silver to Gold ({schema_name})")
    lines.append("")
    lines.append(f"Generated: {timestamp}")
    lines.append("")
    lines.append("## Übersicht")
    lines.append("")
    lines.append("| Notebook | Silver Sources | Gold Target |")
    lines.append("|---|---|---|")

    for entry in entries:
        sources = (
            "<br>".join(entry.silver_sources)
            if entry.silver_sources
            else "(keine Silver-Quelle erkannt)"
        )
        notebook_link = f"[{entry.notebook_name}]({entry.notebook_path})"
        lines.append(f"| {notebook_link} | {sources} | {entry.target_table} |")

    lines.append("")
    lines.append("## Lineage Graph")
    lines.append("")
    lines.append("::: mermaid")
    lines.append("flowchart LR")

    emitted_edges: set[tuple[str, str]] = set()
    for entry in entries:
        target_node = _to_node_id(entry.target_table)
        lines.append(f'    {target_node}["{entry.target_table}"]')

        for source in entry.silver_sources:
            source_node = _to_node_id(source)
            lines.append(f'    {source_node}["{source}"]')
            edge = (source_node, target_node)
            if edge not in emitted_edges:
                emitted_edges.add(edge)
                lines.append(f"    {source_node} --> {target_node}")

    lines.append(":::")
    lines.append("")

    lines.append("## Column-Level Lineage")
    lines.append("")
    lines.append(
        "Die folgenden Mappings werden statisch aus den `df_transformed.select(...)` "
        "Ausdrücken der Notebooks abgeleitet."
    )
    lines.append("")

    for entry in entries:
        lines.append(f"### {entry.target_table}")
        lines.append("")
        lines.append(f"Notebook: [{entry.notebook_name}]({entry.notebook_path})")
        lines.append("")

        if not entry.column_mappings:
            lines.append("(Keine Spalten-Mappings erkannt)")
            lines.append("")
            continue

        lines.append("| Target Column | Source Column(s) | Source Table(s) | Expression |")
        lines.append("|---|---|---|---|")
        for mapping in entry.column_mappings:
            source_columns = (
                "<br>".join(mapping.source_columns)
                if mapping.source_columns
                else "(keine direkte Quellspalte erkannt)"
            )
            source_tables = (
                "<br>".join(mapping.source_tables)
                if mapping.source_tables
                else "(n/a)"
            )
            lines.append(
                "| "
                f"{mapping.target_column} | {source_columns} | "
                f"{source_tables} | {mapping.expression} |"
            )
        lines.append("")

    return "\n".join(lines)


def _slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower())
    return slug.strip("-") or "schema"


def _build_overview_markdown(schema_to_file: dict[str, str]) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines: list[str] = []
    lines.append("# Data Lineage – Silver to Gold")
    lines.append("")
    lines.append(f"Generated: {timestamp}")
    lines.append("")
    lines.append("## Schema-Dokumente")
    lines.append("")

    for schema in sorted(schema_to_file):
        lines.append(f"- [{schema}]({schema_to_file[schema]})")

    lines.append("")
    return "\n".join(lines)


def generate_documents(notebooks_root: Path, output_dir: Path, repo_root: Path) -> int:
    notebook_paths = _find_notebooks(notebooks_root)
    entries: list[LineageEntry] = []

    for notebook_path in notebook_paths:
        parsed = _parse_notebook(notebook_path, repo_root)
        if parsed is not None:
            entries.append(parsed)

    if not entries:
        raise RuntimeError("Es wurden keine transformierenden Silver2Gold-Notebooks gefunden.")

    output_dir.mkdir(parents=True, exist_ok=True)

    for stale_file in output_dir.glob("silver-to-gold-*.md"):
        stale_file.unlink(missing_ok=True)

    entries_by_schema: dict[str, list[LineageEntry]] = {}
    for entry in entries:
        entries_by_schema.setdefault(entry.destination_schema, []).append(entry)

    schema_to_file: dict[str, str] = {}
    for schema, schema_entries in sorted(entries_by_schema.items()):
        schema_file_name = f"silver-to-gold-{_slugify(schema)}.md"
        schema_path = output_dir / schema_file_name
        schema_markdown = _build_schema_markdown(
            schema_name=schema,
            entries=schema_entries,
        )
        schema_path.write_text(schema_markdown, encoding="utf-8")
        schema_to_file[schema] = schema_file_name

    overview_path = output_dir / "README.md"
    overview_markdown = _build_overview_markdown(schema_to_file=schema_to_file)
    overview_path.write_text(overview_markdown, encoding="utf-8")

    print(f"Lineage-Dokumente erzeugt unter: {output_dir}")
    print(f"Schemas: {len(schema_to_file)}")
    print(f"Verarbeitete Notebooks: {len(entries)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate Silver-to-Gold lineage markdown from Fabric notebooks."
    )
    parser.add_argument(
        "--notebooks-root",
        default="fabric/03_Silver2Gold",
        help="Path to the Silver2Gold notebook root folder.",
    )
    parser.add_argument(
        "--output-dir",
        default="doc/data-lineage",
        help="Output directory for generated markdown files.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    notebooks_root = (repo_root / args.notebooks_root).resolve()
    output_dir = (repo_root / args.output_dir).resolve()

    return generate_documents(
        notebooks_root=notebooks_root,
        output_dir=output_dir,
        repo_root=repo_root,
    )


if __name__ == "__main__":
    raise SystemExit(main())
