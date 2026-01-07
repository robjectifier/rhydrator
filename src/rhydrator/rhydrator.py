# ruff: noqa: T201, RUF059, F841, PERF102, PERF401
from pathlib import Path

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT, ROOTFile
from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.bootstrap.strings import RString
from rootfilespec.rntuple.pagelist import PageListEnvelope
from rootfilespec.rntuple.RNTuple import RNTuple, SchemaDescription
from rootfilespec.rntuple.schema import (
    FIELD_STRUCTURAL_ROLES,
    AliasColumnDescription,
    ColumnDescription,
    FieldDescription,
)
from rootfilespec.serializable import BufferContext, ReadBuffer


def get_field_to_page_map(
    schema_description: SchemaDescription,
    pagelist_envelopes: list[PageListEnvelope],
    filepath="",
) -> dict:
    def decode_string(rstring: RString) -> str:
        return rstring.fString.decode("utf-8")

    field_descriptions = schema_description.fieldDescriptions
    column_descriptions = schema_description.columnDescriptions
    alias_column_descriptions = schema_description.aliasColumnDescriptions
    extra_type_informations = schema_description.extraTypeInformations

    def get_field_tree(field_descriptions: list[FieldDescription], filepath="") -> dict:
        """
        Constructs a tree representation of field descriptions based on their parent-child relationships.
        Optionally writes the tree structure to a specified file.

        Args:
            field_descriptions (list): A list of FieldDescription objects.
            filepath (str, optional): The file path to write the tree structure. Defaults to an empty string (no file output).

        Returns:
            dict: A nested dictionary representing the tree structure of fields.
        """

        def decode_field_flags(field: FieldDescription) -> tuple[str, bool, bool, bool]:
            isRepetitive = bool(field.fFlags & 0x1)
            isProjected = bool(field.fFlags & 0x2)
            hasTypeChecksum = bool(field.fFlags & 0x4)
            flag_list = []
            if isRepetitive:
                flag_list.append(f"Repetitive (Array Size: {field.fArraySize})")
            if isProjected:
                flag_list.append(f"Projected (Source Field ID: {field.fSourceFieldID})")
            if hasTypeChecksum:
                flag_list.append(f"Type Checksum ({field.fTypeChecksum})")
            return (
                ", ".join(flag_list) if flag_list else "",
                isRepetitive,
                isProjected,
                hasTypeChecksum,
            )

        tree = {}
        # map of field_id -> node dict (allows attaching children to any ancestor, not only top-level)
        nodes = {}

        for field_id, field in enumerate(field_descriptions):
            parent_id = field.fParentFieldID
            field_structural_role = FIELD_STRUCTURAL_ROLES.get(
                field.fStructuralRole, "Unknown"
            )
            field_flags, isRepetitive, isProjected, hasTypeChecksum = (
                decode_field_flags(field)
            )

            field_name = decode_string(field.fFieldName)
            field_type = decode_string(field.fTypeName)
            field_type_alias = decode_string(field.fTypeAlias)
            if field_type_alias:
                field_type += f", alias: {field_type_alias}"
            field_description = decode_string(field.fFieldDescription)
            if field_description:
                field_type += f", desc: {field_description}"

            # create node for this field and store it in nodes so children or projections can find it later
            node = {
                "name": field_name,
                "role": field_structural_role,
                "flags": field_flags,
                "type": field_type,
                "children": {},
                "projections": {},
            }
            nodes[field_id] = node

            if parent_id == field_id:
                # This is a top-level field
                tree[field_id] = node
            else:
                # This is a child field; find its parent in the nodes map and add it
                parent_node = nodes.get(parent_id)
                if (
                    parent_node is None
                ):  # Parent not yet added; should not happen because children come after parents
                    msg = f"Error building field tree: Parent ID {parent_id} for field ID {field_id} not found."
                    raise ValueError(msg)
                parent_node["children"][field_id] = node  # type: ignore[index]

            # # below code for building tree with projections inline
            # if not isProjected: # These are physical fields in the tree
            #     if parent_id == field_id:
            #         # This is a top-level field
            #         tree[field_id] = node
            #     else:
            #         # This is a child field; find its parent in the nodes map and add it
            #         parent_node = nodes.get(parent_id)
            #         if parent_node is None: # Parent not yet added; should not happen because children come after parents
            #             msg = f"Error building field tree: Parent ID {parent_id} for field ID {field_id} not found."
            #             raise ValueError(msg)
            #         parent_node["children"][field_id] = node # type: ignore[index]
            # else: # These are virtual projections of physical fields, using the source field data but in a different type
            #     # Find the source field node and add this as a projection
            #     source_field_id = field.fSourceFieldID
            #     source_node = nodes.get(source_field_id)
            #     if source_node is None:
            #         msg = f"Error building field tree: Source Field ID {source_field_id} for projected field ID {field_id} not found."
            #         raise ValueError(msg)
            #     source_node["projections"][field_id] = node

        if filepath:
            filepath_field = f"{filepath}_field_tree.txt"

            def write_tree(node, indent=0):
                for fid, info in node.items():
                    # line = "  " * indent + f"- [{fid}] {info['name']}: Role: {info['role']}; ({info['type']})\n"
                    line = "  " * indent + f"- [{fid}] {info['name']}: {info['role']}"
                    if info["flags"]:
                        line += f" [{info['flags']}]"
                    if info["type"]:
                        line += f" ({info['type']})"
                    if info["projections"]:
                        line += " {Projections: "
                        proj_list = []
                        for pid, pinfo in info["projections"].items():
                            proj_str = f"[{pid}] {pinfo['name']}"
                            if pinfo["flags"]:
                                proj_str += f" [{pinfo['flags']}]"
                            if pinfo["type"]:
                                proj_str += f" ({pinfo['type']})"
                            proj_list.append(proj_str)
                        line += "; ".join(proj_list) + "}"
                    line += "\n"
                    f.write(line)
                    write_tree(info["children"], indent + 1)

            with Path(filepath_field).open("w") as f:
                write_tree(tree)
            print(f"Field tree written to '{filepath_field}'")
        return tree

    # end get_field_tree()

    field_tree = get_field_tree(field_descriptions, filepath=filepath)

    # assign columns to fields in the field tree. update the field tree nodes to include list of column IDs along with column info
    def map_columns_to_fields(
        column_descriptions: list[ColumnDescription],
        alias_column_descriptions: list[AliasColumnDescription],
        field_tree: dict,
        filepath="",
    ) -> dict:
        """
        Maps columns to fields in the field tree based on field IDs.
        Takes the output of get_field_tree and adds column information to the corresponding field nodes.
        """

        field_id_to_node = {}

        # recursive function to build a map of field_id to node in the field tree
        #   helps with handling nested fields
        def build_field_id_map(node):
            for fid, info in node.items():
                field_id_to_node[fid] = info
                build_field_id_map(info["children"])

        build_field_id_map(field_tree)

        def decode_column_flags(
            column: ColumnDescription,
        ) -> tuple[str, bool, bool, bool]:
            isDeferred = bool(column.fFlags & 0x1)
            isSuppressed = (
                False  # True if isDeferred and first element index is negative
            )
            hasValueRange = bool(column.fFlags & 0x2)
            flag_list = []
            if isDeferred:
                if column.fFirstElementIndex is None:
                    msg = "Column with deferred flag set but fFirstElementIndex is None"
                    raise ValueError(msg)
                isSuppressed = column.fFirstElementIndex < 0
                flag_list.append(
                    f"Deferred (first element ind: {column.fFirstElementIndex}{', suppressed' if isSuppressed else ''})"
                )
            if hasValueRange:
                flag_list.append(
                    f"Has Value Range (min: {column.fMinValue}, max: {column.fMaxValue})"
                )
            return (
                ", ".join(flag_list) if flag_list else "",
                isDeferred,
                isSuppressed,
                hasValueRange,
            )

        # map columns to fields in the field tree
        for col_id, column in enumerate(column_descriptions):
            column_type = repr(column.fColumnType)
            column_type = column_type.replace(
                "ColumnType.", ""
            )  # column_type always starts with "ColumnType.", strip that off for brevity
            column_field_id = column.fFieldID
            column_flags, isDeferred, isSuppressed, hasValueRange = decode_column_flags(
                column
            )
            column_rep_index = column.fRepresentationIndex
            if column_field_id in field_id_to_node:
                node = field_id_to_node[column_field_id]
                if "columns" not in node:
                    node["columns"] = []
                node["columns"].append(
                    {
                        "column_id": col_id,
                        "type": column_type,
                        "flags": column_flags,
                        "rep_index": column_rep_index,
                    }
                )
            else:
                msg = f"Error mapping column ID {col_id} to field ID {column_field_id}: Field ID not found in field tree."
                raise ValueError(msg)

        # map alias columns to physical columns / projected fields in the field tree
        for alias_column in alias_column_descriptions:
            physical_column_id = alias_column.fPhysicalColumnID
            projected_field_id = alias_column.fFieldID
            if projected_field_id in field_id_to_node:
                node = field_id_to_node[projected_field_id]
                if "alias_columns" not in node:
                    node["alias_columns"] = []
                node["alias_columns"].append({"physical_column_id": physical_column_id})
            else:
                msg = f"Error mapping alias column with physical column ID {physical_column_id} to projected field ID {projected_field_id}: Field ID not found in field tree."
                raise ValueError(msg)

        if filepath:
            filepath_column = f"{filepath}_column_tree.txt"
            with Path(filepath_column).open("w") as f:

                def write_columns(node, indent=0):
                    for fid, info in node.items():
                        if (indent == 0) and (fid not in field_tree):
                            continue  # skip non-top-level fields at top level (avoids duplicates)
                        line = "  " * indent + f"- [{fid}] {info['name']}"
                        if "columns" in info:
                            line += " {Columns: "
                            col_list = []
                            for col in info["columns"]:
                                col_str = f"[{col['column_id']}] Type: {col['type']}, RepIndex: {col['rep_index']}"
                                if col["flags"]:
                                    col_str += f" [{col['flags']}]"
                                col_list.append(col_str)
                            line += "; ".join(col_list) + "}"
                        if "alias_columns" in info:
                            line += " {AliasColumns: "
                            alias_col_list = []
                            for acol in info["alias_columns"]:
                                acol_str = f"[Physical Column ID: {acol['physical_column_id']}]"
                                alias_col_list.append(acol_str)
                            line += "; ".join(alias_col_list) + "}"
                        line += "\n"
                        f.write(line)
                        write_columns(info["children"], indent + 1)

                write_columns(field_id_to_node)
            print(f"Column mapping written to '{filepath_column}'")

        if filepath:
            # write field to column mapping to file
            # this should be just like the field tree written in get_field_tree(), but with columns info added
            filepath_field_column = f"{filepath}_field_column_tree.txt"

            def write_field_column_tree(node, indent=0):
                for fid, info in node.items():
                    if (indent == 0) and (fid not in field_tree):
                        continue  # skip non-top-level fields at top level (avoids duplicates)
                    line = "  " * indent + f"- [{fid}] {info['name']}: {info['role']}"
                    if info["flags"]:
                        line += f" [{info['flags']}]"
                    if info["type"]:
                        line += f" ({info['type']})"
                    if "columns" in info:
                        line += " {Columns: "
                        col_list = []
                        for col in info["columns"]:
                            col_str = f"[{col['column_id']}] Type: {col['type']}, RepIndex: {col['rep_index']}"
                            if col["flags"]:
                                col_str += f" [{col['flags']}]"
                            col_list.append(col_str)
                        line += "; ".join(col_list) + "}"
                    if "alias_columns" in info:
                        line += " {AliasColumns: "
                        alias_col_list = []
                        for acol in info["alias_columns"]:
                            acol_str = (
                                f"[Physical Column ID: {acol['physical_column_id']}]"
                            )
                            alias_col_list.append(acol_str)
                        line += "; ".join(alias_col_list) + "}"
                    if info["projections"]:
                        line += " {Projections: "
                        proj_list = []
                        for pid, pinfo in info["projections"].items():
                            proj_str = f"[{pid}] {pinfo['name']}"
                            if pinfo["flags"]:
                                proj_str += f" [{pinfo['flags']}]"
                            if pinfo["type"]:
                                proj_str += f" ({pinfo['type']})"
                            proj_list.append(proj_str)
                        line += "; ".join(proj_list) + "}"
                    line += "\n"
                    f.write(line)
                    write_field_column_tree(info["children"], indent + 1)

            with Path(filepath_field_column).open("w") as f:
                write_field_column_tree(field_id_to_node)
            print(f"Field to column tree written to '{filepath_field_column}'")

        # if filepath:
        #     # write field to column mapping to file
        #     filepath_field_column = f"{filepath}_field_column_map.txt"
        #     with Path(filepath_field_column).open("w") as f:
        #         f.write("FieldID,FieldName,ColumnID,ColumnType,ColumnFlags,ColumnRepIndex\n")
        #         for fid, info in field_id_to_node.items():
        #             if "columns" in info:
        #                 for col in info["columns"]:
        #                     f.write(f"{fid},{info['name']},{col['column_id']},{col['type']},{col['flags']},{col['rep_index']}\n")
        #     print(f"Field to column mapping written to '{filepath_field_column}'")

        return field_id_to_node

    field_column_tree = map_columns_to_fields(
        column_descriptions, alias_column_descriptions, field_tree, filepath=filepath
    )

    # if extra_type_informations:
    #     # for now, just raise not implemented error so i know when we see one
    #     msg = f"ExtraTypeInformation handling not implemented yet (found {len(extra_type_informations)} entries):\n\t{extra_type_informations=}"
    #     raise NotImplementedError(msg)

    # now examine page list envelopes to map pages to columns/fields
    # first, check the length of pagelist_envelopes. length = number of cluster groups. if more than 1, need to handle that later
    if len(pagelist_envelopes) > 1:
        msg = f"Multiple PageListEnvelopes (cluster groups) not implemented yet (found {len(pagelist_envelopes)} entries)."
        raise NotImplementedError(msg)

    # for now, just handle the first (and only) pagelist envelope
    def map_pages_to_columns_to_fields(
        pagelist_envelope: PageListEnvelope,
        field_column_tree: dict,
        field_tree: dict,
        filepath="",
    ) -> dict:
        """
        Maps pages to columns and fields in the field-column tree based on page locations.
        Takes the output of map_columns_to_fields and adds page information to the corresponding column nodes.
        """

        cluster_summaries = pagelist_envelope.clusterSummaries
        page_locations = pagelist_envelope.pageLocations

        # build a map of column_id to list of page descriptions
        column_id_to_pages = {}  # type: ignore[var-annotated]

        # each column can span multiple clusters
        # for each column, get its pages for each cluster
        # make sure not to overwrite pages if multiple clusters
        # column_id_to_pages should take the column id as the first key, then cluster id as the second key
        #   the value for each cluster id will be the cluster summary info and list of page descriptions
        for cluster_id, cluster_summary in enumerate(cluster_summaries):
            first_entry_number = cluster_summary.fFirstEntryNumber
            n_entries = cluster_summary.fNEntries
            feature_flag = cluster_summary.fFeatureFlag

            for column_id, page_list in enumerate(page_locations[cluster_id]):
                # page_list is the ListFrame of PageLocations for this column in this cluster
                page_descriptions = page_list.items

                if column_id not in column_id_to_pages:
                    column_id_to_pages[column_id] = {}

                column_id_to_pages[column_id][cluster_id] = {
                    "cluster_firstEntryNumber": first_entry_number,
                    "cluster_nEntries": n_entries,
                    "cluster_feature_flag": feature_flag,
                    "pages": page_descriptions,
                }

        # now, map pages to columns in the field-column tree
        for _fid, field_info in field_column_tree.items():
            if "columns" in field_info:
                for col in field_info["columns"]:
                    col_id = col["column_id"]
                    if col_id in column_id_to_pages:
                        col["cluster_info"] = column_id_to_pages[col_id]
                    else:
                        msg = f"Error mapping pages to column ID {col_id}: Column ID not found in page locations."
                        raise ValueError(msg)

        if filepath:
            # write field to column to page mapping to file
            filepath_field_column_page = f"{filepath}_field_column_page_tree.txt"

            def write_field_column_page_tree(node, indent=0):
                for fid, info in node.items():
                    if (indent == 0) and (fid not in field_tree):
                        continue  # skip non-top-level fields at top level (avoids duplicates)
                    line = "  " * indent + f"- [{fid}] {info['name']}: {info['role']}"
                    if info["flags"]:
                        line += f" [{info['flags']}]"
                    if info["type"]:
                        line += f" ({info['type']})"
                    if "columns" in info:
                        line += " {Columns: "
                        col_list = []
                        for col in info["columns"]:
                            col_str = f"[{col['column_id']}] Type: {col['type']}, RepIndex: {col['rep_index']}"
                            if col["flags"]:
                                col_str += f" [{col['flags']}]"
                            if "cluster_info" in col:
                                col_str += ", Clusters: "
                                cluster_list = []
                                for cid, cinfo in col["cluster_info"].items():
                                    cluster_str = f"[Cluster ID: {cid}] FirstEntry: {cinfo['cluster_firstEntryNumber']}, NEntries: {cinfo['cluster_nEntries']}, FeatureFlag: {cinfo['cluster_feature_flag']}, Pages: {cinfo['pages']}"
                                    cluster_list.append(cluster_str)
                                col_str += "; ".join(cluster_list)
                            col_list.append(col_str)
                        line += "; ".join(col_list) + "}"
                    if "alias_columns" in info:
                        line += " {AliasColumns: "
                        alias_col_list = []
                        for acol in info["alias_columns"]:
                            acol_str = (
                                f"[Physical Column ID: {acol['physical_column_id']}]"
                            )
                            alias_col_list.append(acol_str)
                        line += "; ".join(alias_col_list) + "}"
                    if info["projections"]:
                        line += " {Projections: "
                        proj_list = []
                        for pid, pinfo in info["projections"].items():
                            proj_str = f"[{pid}] {pinfo['name']}"
                            if pinfo["flags"]:
                                proj_str += f" [{pinfo['flags']}]"
                            if pinfo["type"]:
                                proj_str += f" ({pinfo['type']})"
                            proj_list.append(proj_str)
                        line += "; ".join(proj_list) + "}"
                    line += "\n"
                    f.write(line)
                    write_field_column_page_tree(info["children"], indent + 1)

            with Path(filepath_field_column_page).open("w") as f:
                write_field_column_page_tree(field_column_tree)
            print(
                f"Field to column to page tree written to '{filepath_field_column_page}'"
            )

            def write_field_column_page_tree_html(node, field_tree, filepath=""):
                """
                Writes field to column to page mapping to an HTML file with accordion-style folds.
                """
                if not filepath:
                    return

                filepath_html = f"{filepath}_field_column_page_tree.html"
                filepath_trimmed = filepath.split("/")[-1].split(".")[
                    0
                ]  # Make this the header text

                html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{filepath_trimmed} Field Column Page Tree</title>
    <style>
        body {{ font-family: monospace; background-color: #f5f5f5; padding: 20px; }}
        .accordion {{ background-color: #e7e7e7; color: #444; cursor: pointer; padding: 12px; width: 100%; border: 1px solid #ddd; text-align: left; outline: none; font-size: 13px; margin-top: 2px; transition: 0.3s; }}
        .accordion.active, .accordion:hover {{ background-color: #ccc; }}
        .panel {{ padding: 0 12px; max-height: 0; overflow: hidden; transition: max-height 0.3s ease-out; background-color: #f9f9f9; }}
        .panel.active {{ max-height: 10000px; }}
        .indent-1 {{ margin-left: 20px; }}
        .indent-2 {{ margin-left: 40px; }}
        .indent-3 {{ margin-left: 60px; }}
        .indent-4 {{ margin-left: 80px; }}
        .field-name {{ color: #0066cc; font-weight: bold; }}
        .field-role {{ color: #cc6600; }}
        .field-type {{ color: #009900; }}
        .field-flags {{ color: #990099; }}
        .column-info {{ color: #cc0000; }}
        .cluster-info {{ color: #006666; }}
    </style>
</head>
<body>
    <h1>{filepath_trimmed} Field Column Page Tree</h1>
    <div id="tree"></div>
    <script>
"""

                def build_html_tree(node, indent=0):
                    html_lines = []
                    for fid, info in node.items():
                        if (indent == 0) and (fid not in field_tree):
                            continue

                        # Create accordion button
                        button_id = f"btn_{fid}_{id(info)}"
                        panel_id = f"panel_{fid}_{id(info)}"

                        # Build summary text
                        summary = f"[{fid}] {info['name']}: {info['role']}"
                        if info["flags"]:
                            summary += f" [{info['flags']}]"
                        if info["type"]:
                            summary += f" ({info['type']})"

                        html_lines.append(
                            f'<button class="accordion indent-{indent}" id="{button_id}">{summary}</button>'
                        )
                        html_lines.append(
                            f'<div class="panel indent-{indent}" id="{panel_id}">'
                        )

                        # Add column info
                        if "columns" in info:
                            html_lines.append('<div class="column-info">')
                            for col in info["columns"]:
                                col_str = f"[{col['column_id']}] Type: {col['type']}, RepIndex: {col['rep_index']}"
                                if col["flags"]:
                                    col_str += f" [{col['flags']}]"
                                html_lines.append(f"<div>Column: {col_str}</div>")

                                if "cluster_info" in col:
                                    for cid, cinfo in col["cluster_info"].items():
                                        cluster_str = f"Cluster ID: {cid}, FirstEntry: {cinfo['cluster_firstEntryNumber']}, NEntries: {cinfo['cluster_nEntries']}, FeatureFlag: {cinfo['cluster_feature_flag']}, Pages: {len(cinfo['pages'])}"
                                        html_lines.append(
                                            f"<div style='margin-left: 20px; color: #006666;'>{cluster_str}</div>"
                                        )
                                        for page in cinfo["pages"]:
                                            html_lines.append(
                                                f"<div style='margin-left: 40px; font-size: 11px;'>{page}</div>"
                                            )
                            html_lines.append("</div>")

                        # Add alias columns
                        if "alias_columns" in info:
                            html_lines.append('<div style="color: #0099cc;">')
                            for acol in info["alias_columns"]:
                                html_lines.append(
                                    f"<div>Alias Column - Physical Column ID: {acol['physical_column_id']}</div>"
                                )
                            html_lines.append("</div>")

                        # Add projections
                        if info["projections"]:
                            html_lines.append('<div style="color: #ff6600;">')
                            for pid, pinfo in info["projections"].items():
                                proj_str = f"[{pid}] {pinfo['name']}"
                                if pinfo["flags"]:
                                    proj_str += f" [{pinfo['flags']}]"
                                if pinfo["type"]:
                                    proj_str += f" ({pinfo['type']})"
                                html_lines.append(f"<div>Projection: {proj_str}</div>")
                            html_lines.append("</div>")

                        # Recurse to children
                        if info["children"]:
                            html_lines.extend(
                                build_html_tree(info["children"], indent + 1)
                            )

                        html_lines.append("</div>")

                    return html_lines

                html_tree = build_html_tree(node)
                html_tree_str = "\n        ".join(html_tree)

                html_content += f"""        document.getElementById('tree').innerHTML = `
        {html_tree_str}
        `;

        const accordions = document.querySelectorAll('.accordion');
        accordions.forEach(button => {{
            button.addEventListener('click', function() {{
                this.classList.toggle('active');
                const panel = this.nextElementSibling;
                panel.classList.toggle('active');
            }});
        }});
    </script>
</body>
</html>
"""

                with Path(filepath_html).open("w") as f:
                    f.write(html_content)
                print(f"Field to column to page tree written to '{filepath_html}'")

            write_field_column_page_tree_html(
                field_column_tree, field_tree, filepath=filepath
            )

        return field_column_tree

    if len(pagelist_envelopes) == 0:
        return {}

    return map_pages_to_columns_to_fields(
        pagelist_envelopes[0], field_column_tree, field_tree, filepath=filepath
    )

    # return field_column_page_tree


def print_members_recursive(obj):
    """
    Recursively print the members of a class instance, with color coding.
    Works for all classes that use the @serializable decorator.
    """

    # ANSI color codes for pretty printing
    COLOR_CLASS = "\033[1;34m"  # Bold blue
    COLOR_FIELD = "\033[1;32m"  # Bold green
    COLOR_TOP = "\033[1;35m"  # Bold magenta/pink
    COLOR_RESET = "\033[0m"
    COLOR_LIST = "\033[1;36m"  # Bold cyan

    def _oneline_repr(o):
        """Return a one-line color-coded repr of the class and its fields, recursively."""
        items = []
        for f in getattr(o, "__dataclass_fields__", {}):
            v = getattr(o, f)
            if f == "_unknown" and not v:
                continue
            # If the field is a dataclass, recurse one level for compactness
            if hasattr(v, "__dataclass_fields__"):
                items.append(f"{COLOR_FIELD}{f}{COLOR_RESET}={_oneline_repr(v)}")
            else:
                items.append(f"{COLOR_FIELD}{f}{COLOR_RESET}={v!r}")
        return f"{COLOR_CLASS}{o.__class__.__name__}({', '.join(items)}{COLOR_CLASS}){COLOR_RESET}"

    def _is_compact(o):
        """Decide if all fields can be printed on one line (no lists/tuples, <=5 fields)."""
        fields = getattr(o, "__dataclass_fields__", {})
        if len(fields) > 5:
            return False
        for f in fields:
            v = getattr(o, f)
            if isinstance(v, (list, tuple)):
                return False
        return True

    def _recurse(obj, indent, visited, mode, lines):
        prefix = " " * indent
        # Avoid infinite recursion on cyclic references
        obj_id = id(obj)
        if obj_id in visited:
            lines.append(f"{prefix}<recursion detected>")
            return
        visited.add(obj_id)

        # If dataclass, print fields
        if hasattr(obj, "__dataclass_fields__"):
            fields = list(obj.__dataclass_fields__)
            # Print class name if not suppressed
            if mode != "suppress_classname":
                if _is_compact(obj):
                    # Print all fields on one line
                    onelinerepr = _oneline_repr(obj)
                    if not lines or len(lines) == 1:
                        lines.append(onelinerepr)
                    else:
                        lines[-1] += onelinerepr
                    return
                if mode == "in_list":
                    lines[-1] += f"{COLOR_CLASS}{obj.__class__.__name__}:{COLOR_RESET}"
                else:
                    lines.append(
                        f"{prefix}{COLOR_CLASS}{obj.__class__.__name__}:{COLOR_RESET}"
                    )

            # Print each field, recursively if needed
            for f in fields:
                v = getattr(obj, f)
                if f == "_unknown" and not v:
                    continue
                lines.append(f"{prefix}  {COLOR_FIELD}{f}{COLOR_RESET} = ")
                # If the field is a dataclass, recurse
                if hasattr(v, "__dataclass_fields__"):
                    _recurse(v, indent + 4, visited, "", lines)
                # If the field is a list or tuple, print each item
                elif isinstance(v, (list, tuple)):
                    if v:
                        lines[-1] += (
                            f"{COLOR_LIST}[{len(v)} item list of {COLOR_CLASS}{v[0].__class__.__name__}{COLOR_LIST}]:{COLOR_RESET}"
                        )
                        for idx, item in enumerate(v):
                            lines.append(
                                f"{prefix}    [{COLOR_LIST}{idx}{COLOR_RESET}] "
                            )
                            _recurse(item, indent + 8, visited, "in_list", lines)
                    else:
                        lines[-1] += f"{COLOR_LIST}[empty list]{COLOR_RESET}"
                else:
                    # Print primitive field value
                    lines[-1] += repr(v)
        elif isinstance(obj, (list, tuple)):
            # If it's a list or tuple, print each item
            lines.append(f"{prefix}{COLOR_LIST}[{len(obj)} item list]:{COLOR_RESET}")
            for idx, item in enumerate(obj):
                lines.append(f"{prefix}  [{COLOR_LIST}{idx}{COLOR_RESET}] ")
                _recurse(item, indent + 4, visited, "in_list", lines)
        else:
            # Not a dataclass, just print the value
            lines.append(f"{prefix}{obj!r}")

    lines = []
    # Print class header
    lines.append(
        f"{COLOR_TOP}{'-' * 27} Printing {obj.__class__.__name__}: {'-' * 27}{COLOR_RESET}"
    )
    # Print the members recursively
    _recurse(obj, 0, set(), "", lines)
    # Print class footer
    lines.append(
        f"{COLOR_TOP}{'-' * 25} Done Printing {obj.__class__.__name__}! {'-' * 24}\n{COLOR_RESET}"
    )
    # Join all lines and print
    print("\n".join(lines))


if __name__ == "__main__":
    initial_read_size = 512
    # path = Path("../TTToSemiLeptonic_UL18JMENanoAOD-zstd.root")
    # path = Path(data_path("rntviewer-testfile-uncomp-single-rntuple-v1-0-0-0.root"))
    # path = Path(data_path("rntviewer-testfile-multiple-rntuples-v1-0-0-0.root"))

    # filename = "/Users/samantha/RNTuple/rntuples/DYJetsToLL_RunIIAutumn18NanoAODv7_RNTuple"
    # path = Path(f"{filename}.root")

    # filename = "/Users/samantha/RNTuple/rntuples/initialconversion_rntuple_mini24_lzma4_buffer4"
    filename = "/Users/samantha/RNTuple/rntuples/initialconversion_rntuple_minisim24_lzma4_buffer4"
    path = Path(f"{filename}.rntpl")
    print(f"\033[1;36mReading '{path}'...\n\033[0m")
    with path.open("rb") as filehandle:

        def fetch_data(seek: int, size: int):
            """Fetches data from a file at a specified position and size.

            Args:
                seek (int): The position in the file to start reading from.
                size (int): The number of bytes to read from the file.

            Returns:
                ReadBuffer: A buffer containing the read data, along with the seek position and an offset of 0.
            """
            # print(f"\033[3;33mfetch_data {seek=} {size=}\033[0m")
            filehandle.seek(seek)
            # return ReadBuffer(memoryview(filehandle.read(size)), seek, 0)
            return ReadBuffer(
                memoryview(filehandle.read(size)),
                0,
                BOOTSTRAP_CONTEXT,
                BufferContext(abspos=seek),
            )

        # Get TFile Header
        buffer = fetch_data(0, initial_read_size)
        file, _ = ROOTFile.read(buffer)
        print(f"\t{file}\n")

        def fetch_cached(seek: int, size: int):
            # print(f"\033[3;33mfetch_cached {seek=} {size=}\033[0m")
            if seek + size <= len(buffer):
                return buffer[seek : seek + size]
            print("Didn't find data in initial read buffer, fetching from file")
            return fetch_data(seek, size)

        # Get TFile object (root TDirectory)
        tfile = file.get_TFile(fetch_cached)
        print(f"\t{tfile}\n")

        # usually the directory tkeylist and the streamer info are adjacent at the end of the file
        keylist_start = tfile.rootdir.fSeekKeys
        keylist_stop = keylist_start + tfile.rootdir.header.fNbytesKeys
        print(f"KeyList at {keylist_start}:{keylist_stop}")
        streaminfo_start = file.header.fSeekInfo
        streaminfo_stop = streaminfo_start + file.header.fNbytesInfo
        print(f"StreamerInfo at {streaminfo_start}:{streaminfo_stop}")
        print(f"End of file at {file.header.fEND}")

        # Get TKeyList (List of all TKeys in the TDirectory)
        keylist = tfile.get_KeyList(fetch_data)

        # Print TKeyList
        msg = "\tTKey List Summary:\n"
        for name, key in keylist.items():
            msg += f"\t\tName: {name}; Class: {key.fClassName.fString}\n"
        print(msg)

        # # Get TStreamerInfo (List of classes used in the file)
        # streamerinfo = file.get_StreamerInfo(fetch_data)
        # assert streamerinfo is not None, "StreamerInfo is None"
        # classes = streamerinfo_to_classes(streamerinfo)
        # with Path("classes.py").open("w") as f:
        #     f.write(classes)
        # exec(classes, globals())

        ########################################################################################################################
        print(f"\033[1;31m\n/{'-' * 44} Begin Reading RNTuples {'-' * 44}/ \033[0m")

        #### Get RNTuple Info
        # Only RNTuple Anchor TKeys are visible (i.e. in TKeyList); ClassName = ROOT::RNTuple
        # anchor_keylist = [key for key in keylist.values() if key.fClassName.fString == b'ROOT::RNTuple']
        for name, tkey in keylist.items():
            # Check for RNTuple Anchors
            if tkey.fClassName.fString == b"ROOT::RNTuple":
                print(
                    f"\033[1;33m\n{'-' * 34} Begin Reading RNTuple: '{name}' {'-' * 34}\n\033[0m"
                )

                ### Get RNTuple Anchor Object
                anchor = tkey.read_object(fetch_data, ROOT3a3aRNTuple)
                print_members_recursive(anchor)

                ### Construct RNTuple from Anchor
                rntuple = RNTuple.from_anchor(anchor, fetch_data)
                # print_members_recursive(rntuple)
                # print_members_recursive(rntuple.headerEnvelope)

                # Print RNTuple attributes
                featureFlags = rntuple.featureFlags
                print_members_recursive(featureFlags)

                schemaDescription = rntuple.schemaDescription
                # print_members_recursive(schemaDescription)

                # Iterate through fieldDescriptions, printing only the top-level fields
                print("\nRNTuple Top-Level Fields:")
                nFields = len(schemaDescription.fieldDescriptions)
                topLevelFields_names = []
                for fieldID, fieldDescription in enumerate(
                    schemaDescription.fieldDescriptions
                ):
                    if fieldDescription.fParentFieldID == fieldID:
                        topLevelFields_names.append(fieldDescription.fFieldName.fString)

                        # print_members_recursive(fieldDescription)
                # print(f"\tTop-Level Field Names: {topLevelFields_names}")
                print(
                    f"\tTotal Top-Level Fields: {len(topLevelFields_names)} / {nFields}"
                )

                # Get field tree structure
                field_tree = get_field_to_page_map(
                    schemaDescription,
                    rntuple.pagelistEnvelopes,
                    filepath=f"{filename}_{name}",
                )
                # print(field_tree)

                # print(f"\n\t{len(schemaDescription.aliasColumnDescriptions)=}")
                # print(f"\n\t{len(schemaDescription.extraTypeInformations)=}")

                # # Print the number of clusters (number of pagelist envelopes)
                # print(f"Number of clusters (pagelist envelopes): {len(rntuple.pagelistEnvelopes)}")

                # # do hacky thing to get info from header envelope for field names
                # for pagelistEnvelope in rntuple.pagelistEnvelopes:
                #     for columnlist in pagelistEnvelope.pageLocations:
                #         for pagelist in columnlist:
                #             # check if any page has locator with offset 76688
                #             for page in pagelist:
                #                 if page.locator.offset == 76688:
                #                     print(f"Found page at offset 76688: {page}")
                #                     print(f"\tWith pageList: {pagelist=} \n")

                extended_page_descriptions = rntuple.get_extended_page_descriptions()
                # print_members_recursive(extended_page_descriptions)
                # page_counter = 0
                # size_total = 0
                # Collect page size and offset for CSV
                # # Write CSV file with page size, offset, fNElements, columnType
                # with open(f"{filename}.csv", "w", newline="") as csvfile:
                #     csvfile.write("#size,offset,fNElements,columnType\n")
                #     for i, pagelistenvelope in enumerate(rntuple.pagelistEnvelopes):
                #         csvfile.write(f"#Cluster Group (Page List Envelope) {i}\n")
                #         for i_cluster, columnlist in enumerate(pagelistenvelope.pageLocations):
                #             csvfile.write(f"\t#Cluster {i_cluster}\n")
                #             for i_column, pagelist in enumerate(columnlist):
                #                 csvfile.write(f"\t\t#Column {i_column}, elementoffset={pagelist.elementoffset}, compressionsettings={pagelist.compressionsettings}\n")
                #                 for i_page, page in enumerate(pagelist):
                #                     page_counter += 1
                #                     size_total += page.locator.size

                #                     # # print(f"Page {page_counter}: Cluster {i_cluster}, Column {i_column}, Page {i_page}: {page}")
                #                     # # print(f"\t{page=}")
                #                     csvfile.write(f"\t\t\t{page.locator.size},{page.locator.offset},{page.fNElements}\n")
                #                     # # Save size and offset for CSV
                #                     # page_info.append((
                #                     #     page.pageDescription.locator.size,
                #                     #     page.pageDescription.locator.offset,
                #                     #     page.pageDescription.fNElements,
                #                     #     repr(page.columnType)
                #                     # ))
                #                     # quit()
                #                     # if page.pageDescription.locator.offset == 76688:
                #                     #     print(f"Found page at offset 76688: {page}")

                # page_info = []
                # with open(f"{filename}.csv", "w", newline="") as csvfile:
                #     # csvfile.write("#size,offset,fNElements,columnType\n")
                #     for i, pagelistenvelope in enumerate(extended_page_descriptions):
                #         # csvfile.write(f"#Cluster Group (Page List Envelope) {i}\n")
                #         for i_cluster, columnlist in enumerate(pagelistenvelope):
                #             # csvfile.write(f"\t#Cluster {i_cluster}\n")
                #             for i_column, pagelist in enumerate(columnlist):
                #                 # csvfile.write(f"\t\t#Column {i_column}\n")
                #                 for i_page, page in enumerate(pagelist):
                #                     page_counter += 1
                #                     size_total += page.pageDescription.locator.size
                #                     # print(f"Page {page_counter}: Cluster {i_cluster}, Column {i_column}, Page {i_page}: {page}")
                #                     # print(f"\t{page=}")
                #                     # csvfile.write(f"\t\t\t{page.pageDescription.locator.size},{page.pageDescription.locator.offset},{page.pageDescription.fNElements},{repr(page.columnType)}\n")
                #                     # Save size and offset for CSV
                #                     page_info.append((
                #                         page.pageDescription.locator.size,
                #                         page.pageDescription.locator.offset,
                #                         page.pageDescription.fNElements,
                #                         repr(page.columnType)
                #                     ))
                #                     # quit()
                #                     # if page.pageDescription.locator.offset == 76688:
                #                     #     print(f"Found page at offset 76688: {page}")

                # print(f"Total Pages: {page_counter}")
                # print(f"Total Compressed Size: {size_total / (1024 * 1024):.2f} MB")
                # print(f"Average Compressed Size per Page: {size_total / page_counter / 1024:.2f} KB")

                # # Write CSV file with page size, offset, fNElements, columnType
                # with open(f"{filename}_all.csv", "w", newline="") as csvfile:
                #     csvfile.write("#size,offset,fNElements,columnType\n")
                #     for size, offset, fNElements, columnType in page_info:
                #         csvfile.write(f"{size},{offset},{fNElements},{columnType}\n")

                # # Write CSV file of unique pages (unique offset and size)
                # with open(f"{filename}_unique.csv", "w", newline="") as csvfile:
                #     csvfile.write("#size,offset,fNElements,columnType\n")
                #     unique_pages = set()
                #     for size, offset, fNElements, columnType in page_info:
                #         if (size, offset) not in unique_pages:
                #             unique_pages.add((size, offset))
                #             csvfile.write(f"{size},{offset},{fNElements},{columnType}\n")

                # print(f"\nTotal Unique Pages: {len(unique_pages)}")
                # print(f"Total Unique Compressed Size: {sum(size for size, offset in unique_pages) / (1024 * 1024):.2f} MB")
                # print(f"Average Unique Compressed Size per Page: {sum(size for size, offset in unique_pages) / len(unique_pages) / 1024:.2f} KB")

                # pages = [pagelistenvelope.get_pages(fetch_data) for pagelistenvelope in rntuple.pagelistEnvelopes]

                # # Print RNTuple Pages
                # for i, pagelistenvelope in enumerate(pages):
                #     print(f"\nRNTuple Page List Envelope {i}:")
                #     for i_cluster, columnlist in enumerate(pagelistenvelope):
                #         print(f"\tCluster {i_cluster}:")
                #         for i_column, pagelist in enumerate(columnlist):
                #             print(f"\t\tColumn {i_column}:")
                #             for i_page, page in enumerate(pagelist):
                #                 # print(f"\t\t\tPage {i_page}: {page}")
                #                 print(f"\t\t\tPage {i_page}")

                # print_members_recursive(rntuple.headerEnvelope)
                # print_members_recursive(rntuple.footerEnvelope)
                # for i, pagelist in enumerate(rntuple.pagelistEnvelopes):
                #     print(f"Page List Envelope {i}:")
                #     print_members_recursive(pagelist)

                # envelope_pages = rntuple.get_extended_page_descriptions()
                # for i_pagelist, pagelist in enumerate(envelope_pages):
                #     print(f"Envelope Page Description List {i_pagelist}:")
                #     for i_cluster, cluster in enumerate(pagelist):
                #         print(f"\tCluster {i_cluster}:")
                #         for i_column, column in enumerate(cluster):
                #             print(f"\t\tColumn {i_column}:")
                #             for i_page, page in enumerate(column):
                #                 print(f"\t\t\tPage {i_page}: {page}")
                """
                ### Get the RNTuple Header Envelope from the Anchor
                header = anchor.get_header(fetch_data)
                print_members_recursive(header)

                ### Get the RNTuple Footer Envelope from the Anchor
                footer = anchor.get_footer(fetch_data)
                print_members_recursive(footer)

                ### Get the RNTuple Page List Envelopes from the Footer Envelope
                page_location_lists = footer.get_pagelists(fetch_data)

                # Print attributes of the RNTuple Page List Envelopes
                for i, page_location_list in enumerate(page_location_lists):
                    print(f"Page List Envelope {i}:")
                    # print(f"\t{page_location_list=}\n")
                    print_members_recursive(page_location_list)

                ### Get the actual RNTuple Pages from the Page List Envelopes
                cluster_column_page_lists: list[list[list[RPage]]] = []
                for page_location_list in page_location_lists:
                    pages = page_location_list.get_pages(fetch_data)
                    cluster_column_page_lists.extend(pages)

                # Print attributes of the RNTuple Pages
                for i_cluster, column_page_lists in enumerate(
                    cluster_column_page_lists
                ):
                    for i_column, page_list in enumerate(column_page_lists):
                        for i_page, page in enumerate(page_list):
                            print(
                                f"Cluster {i_cluster}, Column {i_column}, Page {i_page}:"
                            )
                            print(f"\t{page=}\n")
               """
                print(
                    f"\033[1;33m{'-' * 34} Done Reading RNTuple: '{name}' {'-' * 34}\033[0m"
                )

    print(f"\n\033[1;32mClosing '{path}'\n\033[0m")
    # quit()

    # print(f"TStreamerInfo Summary:")
    # for item in streamerinfo.items:
    #     if isinstance(item, TStreamerInfo):
    #         print(f"\t{item.b_named.fName.fString}")
    #         for obj in item.fObjects.objects:
    #             # print(f"\t\t{obj.b_element.b_named.fName.fString}: {obj.b_element.b_named.b_object}")
    #             print(f"\t\t{obj.b_element.b_named.fName.fString}")
