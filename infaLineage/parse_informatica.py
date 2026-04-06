import xml.etree.ElementTree as ET
import sys
import os

def parse_informatica_xml(xml_file_path):
    tree = ET.parse(xml_file_path)
    root = tree.getroot()
    
    output_lines = []
    
    for folder in root.iter('FOLDER'):
        folder_name = folder.get('NAME', 'Unknown')
        
        sources = {}
        for src in folder.findall('SOURCE'):
            name = src.get('NAME')
            db_type = src.get('DATABASETYPE')
            if name:
                sources[name] = db_type
                
        targets = {}
        for tgt in folder.findall('TARGET'):
            name = tgt.get('NAME')
            db_type = tgt.get('DATABASETYPE')
            if name:
                targets[name] = db_type
                
        for workflow in folder.findall('WORKFLOW'):
            wf_name = workflow.get('NAME', 'Unknown')
            
            for session in workflow.findall('SESSION'):
                sess_name = session.get('NAME', 'Unknown')
                map_name = session.get('MAPPINGNAME', 'Unknown')
                
                output_lines.append(f"Folder name: {folder_name}")
                output_lines.append(f"Workflow name: {wf_name}")
                output_lines.append(f"    - session name: {sess_name}")
                output_lines.append(f"    - mapping name: {map_name}")
                output_lines.append(f"    | in a table structure |")
                
                output_lines.append("| Type | Name | Type of source/target | Connection details if any |")
                output_lines.append("|---|---|---|---|")
                
                for sess_inst in session.findall('SESSTRANSFORMATIONINST'):
                    trans_type = sess_inst.get('TRANSFORMATIONTYPE')
                    inst_name = sess_inst.get('SINSTANCENAME')
                    
                    if trans_type in ('Source Definition', 'Target Definition'):
                        conn_info = ""
                        for attr in sess_inst.findall('ATTRIBUTE'):
                            if attr.get('NAME') == 'Connection Information' or 'Connection' in attr.get('NAME', ''):
                                conn_info = attr.get('VALUE', '')
                                
                        if trans_type == 'Source Definition':
                            stype = 'Source'
                            db_type = sources.get(inst_name, 'Unknown')
                        else:
                            stype = 'Target'
                            db_type = targets.get(inst_name, 'Unknown')
                            
                        output_lines.append(f"| {stype} | {inst_name} | {db_type} | {conn_info} |")
                
                output_lines.append("")
                
    return "\n".join(output_lines)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python parse_informatica.py <xml_file>")
        sys.exit(1)
        
    xml_file = sys.argv[1]
    if not os.path.exists(xml_file):
        print(f"File not found: {xml_file}")
        sys.exit(1)
        
    md_content = parse_informatica_xml(xml_file)
    
    output_filename = os.path.splitext(xml_file)[0] + ".md"
    with open(output_filename, 'w') as f:
        f.write(md_content)
        
    print(f"Markdown file generated successfully: {output_filename}")
