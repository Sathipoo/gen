import xml.etree.ElementTree as ET
import json
import sys
import os

def xml_to_dict(element):
    """
    Convert an XML element and its children into a dictionary.
    """
    node = {}
    
    # Store attributes with '@' prefix
    for key, value in element.attrib.items():
        node[f"@{key}"] = value
        
    # Store text content if it exists
    if element.text and element.text.strip():
        node["#text"] = element.text.strip()
        
    # Group children by tag name
    for child in element:
        child_dict = xml_to_dict(child)
        tag = child.tag
        
        if tag not in node:
            node[tag] = child_dict
        else:
            # If tag already exists, convert to a list if not already
            if not isinstance(node[tag], list):
                node[tag] = [node[tag]]
            node[tag].append(child_dict)
            
    return node

def parse_informatica_json(json_data):
    """
    Parse the Informatica dictionary (converted from XML) and generate markdown content.
    """
    output_lines = []
    
    # Root can be a list or a single object under POWERMART
    repository = json_data.get('REPOSITORY', {})
    folders = repository.get('FOLDER', [])
    if isinstance(folders, dict):
        folders = [folders]
        
    for folder in folders:
        folder_name = folder.get('@NAME', 'Unknown')
        
        # Build Source lookup
        sources_data = folder.get('SOURCE', [])
        if isinstance(sources_data, dict):
            sources_data = [sources_data]
        sources_lookup = {s.get('@NAME'): s.get('@DATABASETYPE') for s in sources_data if s.get('@NAME')}
        
        # Build Target lookup
        targets_data = folder.get('TARGET', [])
        if isinstance(targets_data, dict):
            targets_data = [targets_data]
        targets_lookup = {t.get('@NAME'): t.get('@DATABASETYPE') for t in targets_data if t.get('@NAME')}
        
        # Process Workflows
        workflows = folder.get('WORKFLOW', [])
        if isinstance(workflows, dict):
            workflows = [workflows]
            
        for wf in workflows:
            wf_name = wf.get('@NAME', 'Unknown')
            
            sessions = wf.get('SESSION', [])
            if isinstance(sessions, dict):
                sessions = [sessions]
                
            for sess in sessions:
                sess_name = sess.get('@NAME', 'Unknown')
                map_name = sess.get('@MAPPINGNAME', 'Unknown')
                
                output_lines.append(f"Folder name: {folder_name}")
                output_lines.append(f"Workflow name: {wf_name}")
                output_lines.append(f"    - session name: {sess_name}")
                output_lines.append(f"    - mapping name: {map_name}")
                output_lines.append(f"    | in a table structure |")
                
                output_lines.append("| Type | Name | Type of source/target | Connection details if any |")
                output_lines.append("|---|---|---|---|")
                
                # Process transformation instances inside the session
                sess_insts = sess.get('SESSTRANSFORMATIONINST', [])
                if isinstance(sess_insts, dict):
                    sess_insts = [sess_insts]
                    
                for inst in sess_insts:
                    trans_type = inst.get('@TRANSFORMATIONTYPE')
                    inst_name = inst.get('@SINSTANCENAME')
                    
                    if trans_type in ('Source Definition', 'Target Definition'):
                        conn_info = ""
                        attrs = inst.get('ATTRIBUTE', [])
                        if isinstance(attrs, dict):
                            attrs = [attrs]
                            
                        for attr in attrs:
                            attr_name = attr.get('@NAME', '')
                            if attr_name == 'Connection Information' or 'Connection' in attr_name:
                                conn_info = attr.get('@VALUE', '')
                        
                        if trans_type == 'Source Definition':
                            stype = 'Source'
                            db_type = sources_lookup.get(inst_name, 'Unknown')
                        else:
                            stype = 'Target'
                            db_type = targets_lookup.get(inst_name, 'Unknown')
                            
                        output_lines.append(f"| {stype} | {inst_name} | {db_type} | {conn_info} |")
                
                output_lines.append("")
                
    return "\n".join(output_lines)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python parse_informatica_v2.py <xml_file>")
        sys.exit(1)
        
    xml_file = sys.argv[1]
    if not os.path.exists(xml_file):
        print(f"File not found: {xml_file}")
        sys.exit(1)
        
    # Phase 1: Convert XML to JSON structure
    print(f"Converting {xml_file} to JSON...")
    tree = ET.parse(xml_file)
    root = tree.getroot()
    data_dict = xml_to_dict(root)
    
    # Save the intermediate JSON for reference
    json_filename = os.path.splitext(xml_file)[0] + ".json"
    with open(json_filename, 'w') as jf:
        json.dump(data_dict, jf, indent=4)
    print(f"Intermediate JSON saved to: {json_filename}")
    
    # Phase 2: Use the JSON (dictionary) to generate the MD
    print("Generating Markdown from JSON data...")
    md_content = parse_informatica_json(data_dict)
    
    output_md = os.path.splitext(xml_file)[0] + "_from_json.md"
    with open(output_md, 'w') as f:
        f.write(md_content)
        
    print(f"Markdown file generated successfully: {output_md}")
