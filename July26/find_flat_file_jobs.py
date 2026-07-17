#!/usr/bin/env python3
import os
import re
import csv
import sys
import bisect

def parse_xml_robust(filepath):
    """
    Parses Informatica XML file using regex to be completely immune to truncation errors.
    Returns a root element with hierarchical structure.
    """
    print(f"Reading {filepath}...", file=sys.stderr)
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        return None

    print(f"File loaded (length: {len(text)} characters). Scanning tags...", file=sys.stderr)

    # Build newline index for fast line number lookup
    newline_indices = [i for i, char in enumerate(text) if char == '\n']
    
    def get_line_num(pos):
        return bisect.bisect_left(newline_indices, pos) + 1

    class Element:
        def __init__(self, tag, attrs, line_num):
            self.tag = tag
            self.attrs = attrs
            self.line_num = line_num
            self.children = []

    root = Element("ROOT", {}, 0)
    stack = [root]

    # Regex patterns
    tag_pattern = re.compile(r'<([^>]+)>', re.DOTALL)
    attr_pattern = re.compile(r'([A-Za-z0-9_\$#\.\-\:]+)\s*=\s*(?:"([^"]*)"|\'([^\']*)\')')

    tag_count = 0
    for match in tag_pattern.finditer(text):
        content = match.group(1).strip()
        pos = match.start()
        
        # Skip comments, xml declarations, doctypes
        if content.startswith('!--') or content.startswith('?') or content.startswith('!DOCTYPE') or content.startswith('!doctype'):
            continue
            
        tag_count += 1
        line_num = get_line_num(pos)
        
        if content.startswith('/'):
            # Closing tag
            tag_name = content[1:].strip()
            # Pop stack up to matching tag name
            found_idx = -1
            for idx in reversed(range(1, len(stack))):
                if stack[idx].tag == tag_name:
                    found_idx = idx
                    break
            if found_idx != -1:
                while len(stack) > found_idx:
                    stack.pop()
        else:
            # Opening or self-closing tag
            is_self_closing = False
            if content.endswith('/'):
                is_self_closing = True
                content = content[:-1].strip()
            
            parts = content.split(None, 1)
            tag_name = parts[0]
            attrs_str = parts[1] if len(parts) > 1 else ""
            
            # Parse attributes
            attrs = {}
            for attr_match in attr_pattern.finditer(attrs_str):
                attr_name = attr_match.group(1)
                attr_val = attr_match.group(2) if attr_match.group(2) is not None else attr_match.group(3)
                if attr_val:
                    # Unescape HTML entities
                    attr_val = (attr_val
                                .replace('&apos;', "'")
                                .replace('&quot;', '"')
                                .replace('&amp;', '&')
                                .replace('&lt;', '<')
                                .replace('&gt;', '>')
                                .replace('&#xD;&#xA;', '\n')
                                .replace('&#x9;', '\t')
                                .replace('&#xa;', '\n'))
                attrs[attr_name] = attr_val
            
            elem = Element(tag_name, attrs, line_num)
            stack[-1].children.append(elem)
            
            if not is_self_closing:
                stack.append(elem)

    print(f"Parsed {tag_count} tags successfully.", file=sys.stderr)
    return root

def analyze_repository(root):
    """
    Walks the parsed XML tree to index:
    - Sources
    - Targets
    - Shortcuts
    - Mappings and their instances
    - Sessions (reusable and non-reusable)
    - Workflows & Worklets
    """
    sources = {}     # (folder, name) -> data
    targets = {}     # (folder, name) -> data
    shortcuts = {}   # (folder, name) -> data
    mappings = {}    # (folder, name) -> list of instances
    sessions = []    # list of session dicts
    workflows = []   # list of workflow dicts

    # Track traversal state
    current_folder = "Default"
    current_workflow = None
    current_worklet = None

    def walk(node):
        nonlocal current_folder, current_workflow, current_worklet
        
        tag = node.tag
        attrs = node.attrs

        # Keep track of containing folder, workflow, worklet
        if tag == "FOLDER":
            current_folder = attrs.get("NAME", "Default")
        elif tag == "WORKFLOW":
            current_workflow = attrs.get("NAME")
        elif tag == "WORKLET":
            current_worklet = attrs.get("NAME")

        # Extract Folder components
        if tag == "SOURCE":
            name = attrs.get("NAME")
            if name:
                sources[(current_folder, name)] = {
                    "database_type": attrs.get("DATABASETYPE"),
                    "dbdname": attrs.get("DBDNAME"),
                    "owner": attrs.get("OWNERNAME"),
                }
        elif tag == "TARGET":
            name = attrs.get("NAME")
            if name:
                targets[(current_folder, name)] = {
                    "database_type": attrs.get("DATABASETYPE"),
                }
        elif tag == "SHORTCUT":
            name = attrs.get("NAME")
            if name:
                shortcuts[(current_folder, name)] = {
                    "ref_object": attrs.get("REFOBJECTNAME"),
                    "ref_type": attrs.get("OBJECTTYPE"),
                    "ref_folder": attrs.get("FOLDERNAME"),
                }

        # Extract Mappings
        elif tag == "MAPPING":
            map_name = attrs.get("NAME")
            if map_name:
                instances = []
                for child in node.children:
                    if child.tag == "INSTANCE":
                        itype = child.attrs.get("TYPE")
                        if itype in ("SOURCE", "TARGET"):
                            instances.append({
                                "name": child.attrs.get("NAME"),
                                "transformation_name": child.attrs.get("TRANSFORMATION_NAME"),
                                "transformation_type": child.attrs.get("TRANSFORMATION_TYPE"),
                                "type": itype,
                                "dbdname": child.attrs.get("DBDNAME")
                            })
                mappings[(current_folder, map_name)] = instances

        # Extract Sessions
        elif tag == "SESSION":
            sess_name = attrs.get("NAME")
            map_name = attrs.get("MAPPINGNAME")
            if sess_name:
                sess_insts = []
                sess_exts = {}
                
                # Scan session children for instances and extensions
                for child in node.children:
                    if child.tag == "SESSTRANSFORMATIONINST":
                        t_type = child.attrs.get("TRANSFORMATIONTYPE")
                        if t_type in ("Source Definition", "Target Definition"):
                            inst_data = {
                                "name": child.attrs.get("SINSTANCENAME"),
                                "transformation_name": child.attrs.get("TRANSFORMATIONNAME"),
                                "transformation_type": t_type,
                                "is_flatfile": False,
                            }
                            # Check if it has a FLATFILE child element
                            for subchild in child.children:
                                if subchild.tag == "FLATFILE":
                                    inst_data["is_flatfile"] = True
                            sess_insts.append(inst_data)
                            
                    elif child.tag == "SESSIONEXTENSION":
                        ext_name = child.attrs.get("SINSTANCENAME")
                        if ext_name:
                            ext_data = {
                                "name": child.attrs.get("NAME"),
                                "subtype": child.attrs.get("SUBTYPE"),
                                "type": child.attrs.get("TYPE"), # READER or WRITER
                                "transformation_type": child.attrs.get("TRANSFORMATIONTYPE"),
                                "attributes": {},
                                "connections": []
                            }
                            # Scan attributes and connections
                            for subchild in child.children:
                                if subchild.tag == "ATTRIBUTE":
                                    a_name = subchild.attrs.get("NAME")
                                    a_val = subchild.attrs.get("VALUE")
                                    if a_name:
                                        ext_data["attributes"][a_name] = a_val
                                elif subchild.tag == "CONNECTIONREFERENCE":
                                    ext_data["connections"].append({
                                        "cnx_ref_name": subchild.attrs.get("CNXREFNAME"),
                                        "connection_name": subchild.attrs.get("CONNECTIONNAME"),
                                        "connection_type": subchild.attrs.get("CONNECTIONTYPE"),
                                        "variable": subchild.attrs.get("VARIABLE"),
                                    })
                            sess_exts[ext_name] = ext_data
                
                # Also extract general session attributes (like database connection overrides)
                sess_attrs = {}
                for child in node.children:
                    if child.tag == "ATTRIBUTE":
                        a_name = child.attrs.get("NAME")
                        a_val = child.attrs.get("VALUE")
                        if a_name:
                            sess_attrs[a_name] = a_val
                            
                sessions.append({
                    "folder": current_folder,
                    "workflow": current_workflow,
                    "worklet": current_worklet,
                    "name": sess_name,
                    "mapping_name": map_name,
                    "reusable": attrs.get("REUSABLE"),
                    "instances": sess_insts,
                    "extensions": sess_exts,
                    "attributes": sess_attrs
                })

        # Extract Workflow Task Instances
        elif tag == "WORKFLOW":
            wf_name = attrs.get("NAME")
            wf_data = {
                "folder": current_folder,
                "name": wf_name,
                "task_instances": []
            }
            # Scan workflow for task instances
            def scan_wf_tasks(n):
                for child in n.children:
                    if child.tag == "TASKINSTANCE":
                        wf_data["task_instances"].append({
                            "name": child.attrs.get("NAME"),
                            "task_name": child.attrs.get("TASKNAME"),
                            "task_type": child.attrs.get("TASKTYPE"),
                        })
                    elif child.tag == "WORKLET":
                        # Recursively scan worklet child tasks
                        scan_wf_tasks(child)
            scan_wf_tasks(node)
            workflows.append(wf_data)

        # Recursively visit children
        for child in node.children:
            walk(child)

        # Restore context
        if tag == "FOLDER":
            current_folder = "Default"
        elif tag == "WORKFLOW":
            current_workflow = None
        elif tag == "WORKLET":
            current_worklet = None

    walk(root)
    return sources, targets, shortcuts, mappings, sessions, workflows

def resolve_instance_details(folder, inst_name, t_type, direction, shortcuts, sources, targets):
    """
    Resolves base source/target metadata from folders, matching shortcuts where necessary.
    """
    ref_type = "SOURCE" if direction == "Source" else "TARGET"
    shortcut_key = (folder, inst_name)
    base_name = inst_name
    target_folder = folder
    db_type = None

    if shortcut_key in shortcuts:
        sc = shortcuts[shortcut_key]
        base_name = sc["ref_object"]
        target_folder = sc["ref_folder"] or folder
        ref_type = sc["ref_type"]

    if ref_type == "SOURCE":
        src_key = (target_folder, base_name)
        if src_key in sources:
            db_type = sources[src_key]["database_type"]
        else:
            # Fallback scan
            for (f, name), s_data in sources.items():
                if name == base_name:
                    db_type = s_data["database_type"]
                    target_folder = f
                    break
    else:
        tgt_key = (target_folder, base_name)
        if tgt_key in targets:
            db_type = targets[tgt_key]["database_type"]
        else:
            # Fallback scan
            for (f, name), t_data in targets.items():
                if name == base_name:
                    db_type = t_data["database_type"]
                    target_folder = f
                    break

    return db_type, base_name, target_folder

def process_jobs(sources, targets, shortcuts, mappings, sessions, workflows):
    """
    Processes all mappings and sessions to output CSV rows.
    Returns list of dictionaries representing the CSV rows.
    """
    rows = []
    
    # 1. Map reusable sessions to the workflows/worklets that run them
    reusable_sessions = {s["name"]: s for s in sessions if s["reusable"] == "YES"}
    non_reusable_sessions = [s for s in sessions if s["reusable"] != "YES"]
    
    # Let's collect all session executions from workflows
    session_executions = []
    
    # Track which sessions have been associated with workflows
    executed_session_keys = set()
    
    for wf in workflows:
        for ti in wf["task_instances"]:
            if ti["task_type"] == "Session":
                sess_name = ti["task_name"]
                inst_name = ti["name"]
                
                # Check if it matches a reusable session
                if sess_name in reusable_sessions:
                    base_sess = reusable_sessions[sess_name]
                    # Create a copy for this execution
                    execution_sess = base_sess.copy()
                    execution_sess["workflow"] = wf["name"]
                    execution_sess["session_instance_name"] = inst_name
                    session_executions.append(execution_sess)
                    executed_session_keys.add((base_sess["folder"], sess_name))
                else:
                    # Look up in non-reusable sessions defined in this workflow
                    # Non-reusable sessions already have their workflow set during walk
                    for nrs in non_reusable_sessions:
                        if nrs["name"] == sess_name and nrs["workflow"] == wf["name"]:
                            nrs["session_instance_name"] = inst_name
                            session_executions.append(nrs)
                            executed_session_keys.add((nrs["folder"], sess_name))
                            break

    # Add any sessions that weren't explicitly referenced in workflows (orphan sessions)
    for s in sessions:
        if (s["folder"], s["name"]) not in executed_session_keys:
            s["session_instance_name"] = s["name"]
            session_executions.append(s)

    # Track which mappings were processed via session runs
    processed_mappings = set()

    # Process Session executions
    for s in session_executions:
        folder = s["folder"]
        wf_name = s["workflow"] or ""
        wkt_name = s["worklet"] or ""
        sess_name = s["name"]
        map_name = s["mapping_name"]
        
        if map_name:
            processed_mappings.add((folder, map_name))
            
        # Get logical instances from the mapping definition if available
        logical_instances = mappings.get((folder, map_name), []) if map_name else []
        logical_map = {inst["name"]: inst for inst in logical_instances}
        
        # Combine all instance names from session details and logical mapping
        all_inst_names = set(s["extensions"].keys())
        all_inst_names.update(inst["name"] for inst in s["instances"])
        all_inst_names.update(logical_map.keys())
        
        for inst_name in all_inst_names:
            # Determine direction (Source or Target)
            direction = "Source"
            inst_type_logical = None
            
            # Check mapping definition first
            if inst_name in logical_map:
                inst_logical = logical_map[inst_name]
                direction = "Source" if inst_logical["type"] == "SOURCE" else "Target"
                inst_type_logical = inst_logical["transformation_type"]
            else:
                # Check session instance list
                for inst_sess in s["instances"]:
                    if inst_sess["name"] == inst_name:
                        direction = "Source" if "Source" in inst_sess["transformation_type"] else "Target"
                        inst_type_logical = inst_sess["transformation_type"]
                        break
                else:
                    # Check session extension
                    if inst_name in s["extensions"]:
                        ext = s["extensions"][inst_name]
                        direction = "Source" if ext["type"] == "READER" else "Target"
                        inst_type_logical = ext["transformation_type"]
            
            # Resolve database type and base name
            db_type, base_name, _ = resolve_instance_details(
                folder, inst_name, inst_type_logical, direction, shortcuts, sources, targets
            )
            
            # Initialize detail fields
            file_or_table = ""
            dir_or_conn = ""
            is_flat_file = "No"
            
            # Check flat file indicator from session instances
            is_ff_sess_inst = False
            for inst_sess in s["instances"]:
                if inst_sess["name"] == inst_name and inst_sess["is_flatfile"]:
                    is_ff_sess_inst = True
                    break

            # 1. Process Flat Files
            ext = s["extensions"].get(inst_name)
            is_ff_ext = False
            if ext:
                subtype = ext.get("subtype", "")
                ext_name = ext.get("name", "")
                if (subtype in ("File Reader", "File Writer", "Flat File Lookup") or 
                    "Sequential file" in subtype or "Flat File" in subtype or
                    "File Reader" in ext_name or "File Writer" in ext_name):
                    is_ff_ext = True

            if db_type == "Flat File" or is_ff_sess_inst or is_ff_ext:
                is_flat_file = "Yes"
                # Extract file details from session extension attributes
                if ext:
                    attrs = ext["attributes"]
                    file_or_table = (attrs.get("Source filename") or 
                                     attrs.get("Output filename") or 
                                     attrs.get("Merge File Name") or 
                                     attrs.get("Lookup source filename") or "")
                    dir_or_conn = (attrs.get("Source file directory") or 
                                   attrs.get("Output file directory") or 
                                   attrs.get("Merge File Directory") or 
                                   attrs.get("Lookup source file directory") or "")
                
                if not file_or_table:
                    # Fallback to base name
                    file_or_table = base_name
            else:
                # 2. Process Relational/Database
                if ext:
                    # Try connections first
                    connections = ext.get("connections", [])
                    conn_names = [c["connection_name"] for c in connections if c["connection_name"]]
                    # If empty, check variable connection
                    if not conn_names:
                        conn_names = [c["variable"] for c in connections if c["variable"]]
                    
                    dir_or_conn = ", ".join(conn_names) if conn_names else ""
                    
                    # Try lookup table name or override sql
                    attrs = ext["attributes"]
                    file_or_table = attrs.get("Lookup table name") or ""
                
                # Check session-level connection variables (e.g. $Source connection value)
                if not dir_or_conn:
                    if direction == "Source":
                        dir_or_conn = (s["attributes"].get("$Source connection value") or 
                                       s["attributes"].get("$Source") or "")
                    else:
                        dir_or_conn = (s["attributes"].get("$Target connection value") or 
                                       s["attributes"].get("$Target") or "")

                if not file_or_table:
                    file_or_table = base_name
                    
            rows.append({
                "Folder": folder,
                "Workflow": wf_name,
                "Worklet": wkt_name,
                "Session": sess_name,
                "Mapping": map_name or "",
                "Instance_Name": inst_name,
                "Direction": direction,
                "Type": db_type or (ext.get("subtype") if ext else "Unknown"),
                "Name": base_name,
                "File_or_Table_Name": file_or_table,
                "Directory_or_Connection": dir_or_conn,
                "Is_Flat_File": is_flat_file
            })

    # 3. Add any logical Mappings that were not associated with any Session in the XML
    for (folder, map_name), logical_insts in mappings.items():
        if (folder, map_name) not in processed_mappings:
            for inst in logical_insts:
                direction = "Source" if inst["type"] == "SOURCE" else "Target"
                db_type, base_name, _ = resolve_instance_details(
                    folder, inst["name"], inst["transformation_type"], direction, shortcuts, sources, targets
                )
                
                is_flat_file = "Yes" if db_type == "Flat File" else "No"
                
                rows.append({
                    "Folder": folder,
                    "Workflow": "",
                    "Worklet": "",
                    "Session": "",
                    "Mapping": map_name,
                    "Instance_Name": inst["name"],
                    "Direction": direction,
                    "Type": db_type or "Unknown",
                    "Name": base_name,
                    "File_or_Table_Name": base_name,
                    "Directory_or_Connection": "",
                    "Is_Flat_File": is_flat_file
                })

    return rows

def main():
    if len(sys.argv) < 2:
        print("Usage: python find_flat_file_jobs.py <input_xml_file> [output_csv_file]", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    
    # Default output CSV name: <input_basename>_mapping_details.csv
    if len(sys.argv) >= 3:
        output_file = sys.argv[2]
    else:
        base, _ = os.path.splitext(input_file)
        output_file = f"{base}_mapping_details.csv"

    # Parse and analyze
    root = parse_xml_robust(input_file)
    if not root:
        print("Failed to parse XML.", file=sys.stderr)
        sys.exit(1)

    sources, targets, shortcuts, mappings, sessions, workflows = analyze_repository(root)
    
    print(f"Extraction summary:", file=sys.stderr)
    print(f"  Sources parsed:   {len(sources)}", file=sys.stderr)
    print(f"  Targets parsed:   {len(targets)}", file=sys.stderr)
    print(f"  Shortcuts parsed: {len(shortcuts)}", file=sys.stderr)
    print(f"  Mappings parsed:  {len(mappings)}", file=sys.stderr)
    print(f"  Session templates parsed:  {len(sessions)}", file=sys.stderr)
    print(f"  Workflows parsed: {len(workflows)}", file=sys.stderr)

    # Process and build CSV records
    rows = process_jobs(sources, targets, shortcuts, mappings, sessions, workflows)
    
    # Write to CSV
    fields = [
        "Folder", "Workflow", "Worklet", "Session", "Mapping", 
        "Instance_Name", "Direction", "Type", "Name", 
        "File_or_Table_Name", "Directory_or_Connection", "Is_Flat_File"
    ]
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        print(f"Successfully wrote {len(rows)} records to {output_file}.", file=sys.stderr)
        
        # Display list of jobs that have flat files as source or target
        flat_file_jobs = set()
        for r in rows:
            if r["Is_Flat_File"] == "Yes":
                job_desc = []
                if r["Workflow"]:
                    job_desc.append(f"Workflow: {r['Workflow']}")
                if r["Session"]:
                    job_desc.append(f"Session: {r['Session']}")
                if r["Mapping"]:
                    job_desc.append(f"Mapping: {r['Mapping']}")
                if job_desc:
                    flat_file_jobs.add(" -> ".join(job_desc))
                    
        print(f"\n--- Jobs with Flat File as Source or Target ({len(flat_file_jobs)} unique jobs) ---")
        for job in sorted(flat_file_jobs):
            print(f"  * {job}")
            
    except Exception as e:
        print(f"Error writing CSV file: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
