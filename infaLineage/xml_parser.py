import xml.etree.ElementTree as ET
import argparse
import os

class InformaticaLineageGenerator:
    def __init__(self, xml_path):
        self.xml_path = xml_path
        self.tree = ET.parse(xml_path)
        self.root = self.tree.getroot()
        self.repo = self.root.find('REPOSITORY')

    def get_folders(self, folder_name=None):
        folders = self.repo.findall('FOLDER')
        if folder_name:
            return [f for f in folders if f.get('NAME') == folder_name]
        return folders

    def get_workflows(self, folder_node, workflow_name=None):
        workflows = folder_node.findall('WORKFLOW')
        if workflow_name:
            return [w for w in workflows if w.get('NAME') == workflow_name]
        return workflows

    def get_sessions(self, workflow_node):
        # Sessions are TASKINSTANCEs of type "Session"
        return [t for t in workflow_node.findall('TASKINSTANCE') if t.get('TASKTYPE') == 'Session']

    def get_session_config(self, folder_node, session_name):
        # Look for SESSION element in the folder (handles reusable sessions)
        sessions = folder_node.findall('SESSION')
        for s in sessions:
            if s.get('NAME') == session_name:
                return s
        return None

    def get_definition_db_type(self, folder_node, name, type_tag):
        # 1. Check for SHORTCUT first
        shortcuts = folder_node.findall('SHORTCUT')
        for s in shortcuts:
            if s.get('NAME') == name:
                ref_name = s.get('REFOBJECTNAME')
                ref_folder = s.get('FOLDERNAME')
                # Find the folder referenced by the shortcut
                target_folder = None
                for f in self.repo.findall('FOLDER'):
                    if f.get('NAME') == ref_folder:
                        target_folder = f
                        break
                if target_folder is not None:
                    return self.get_definition_db_type(target_folder, ref_name, type_tag)
                else:
                    # Search all folders for the ref_name if folder not found
                    for f in self.repo.findall('FOLDER'):
                        definitions = f.findall(type_tag)
                        for d in definitions:
                            if d.get('NAME') == ref_name:
                                return d.get('DATABASETYPE') or "N/A"

        # 2. Look in the current folder for the actual definition
        definitions = folder_node.findall(type_tag)
        for d in definitions:
            if d.get('NAME') == name:
                return d.get('DATABASETYPE') or "N/A"
        
        # 3. If not found, search all folders (handles shortcuts defined elsewhere or global objects)
        for f in self.repo.findall('FOLDER'):
            # Also check shortcuts in this folder that might match the name
            shortcuts = f.findall('SHORTCUT')
            for s in shortcuts:
                if s.get('NAME') == name:
                    return self.get_definition_db_type(f, name, type_tag)
            
            definitions = f.findall(type_tag)
            for d in definitions:
                if d.get('NAME') == name:
                    return d.get('DATABASETYPE') or "N/A"
                    
        return "N/A"

    def get_mapping_details(self, folder_node, mapping_name):
        mappings = folder_node.findall('MAPPING')
        for m in mappings:
            if m.get('NAME') == mapping_name:
                sources = []
                targets = []
                # Find instances of type SOURCE and TARGET
                instances = m.findall('INSTANCE')
                for inst in instances:
                    inst_name = inst.get('NAME')
                    trans_name = inst.get('TRANSFORMATION_NAME')
                    inst_type = inst.get('TRANSFORMATION_TYPE')
                    actual_type = inst.get('TYPE')
                    
                    if actual_type == 'SOURCE':
                        db_type = self.get_definition_db_type(folder_node, trans_name, 'SOURCE')
                        sources.append({'name': inst_name, 'type': inst_type, 'db_type': db_type})
                    elif actual_type == 'TARGET':
                        db_type = self.get_definition_db_type(folder_node, trans_name, 'TARGET')
                        targets.append({'name': inst_name, 'type': inst_type, 'db_type': db_type})
                return sources, targets
        return [], []

    def get_associated_sq(self, folder_node, mapping_name, source_instance_name):
        mappings = folder_node.findall('MAPPING')
        for m in mappings:
            if m.get('NAME') == mapping_name:
                # Find the SQ that has this source as an associated instance
                for inst in m.findall('INSTANCE'):
                    if inst.get('TRANSFORMATION_TYPE') == 'Source Qualifier':
                        assoc_src = inst.find('ASSOCIATED_SOURCE_INSTANCE')
                        if assoc_src is not None and assoc_src.get('NAME') == source_instance_name:
                            return inst.get('NAME')
        return None

    def get_connection_info(self, folder_node, mapping_name, session_node, instance_name, is_source=False):
        if session_node is None:
            return "N/A"
        
        # 1. Check SESSIONEXTENSION (common for relational sources/targets)
        extensions = session_node.findall('SESSIONEXTENSION')
        
        # For sources, we might need to check the associated Source Qualifier if the source instance itself doesn't have the connection
        targets_to_check = [instance_name]
        if is_source:
            sq_name = self.get_associated_sq(folder_node, mapping_name, instance_name)
            if sq_name:
                targets_to_check.append(sq_name)
        
        for t_name in targets_to_check:
            for ext in extensions:
                if ext.get('SINSTANCENAME') == t_name or ext.get('DSQINSTNAME') == t_name:
                    for conn_ref in ext.findall('CONNECTIONREFERENCE'):
                        conn_name = conn_ref.get('CONNECTIONNAME')
                        if conn_name:
                            return conn_name

        # 2. Check SESSTRANSFORMATIONINST
        for inst in session_node.findall('SESSTRANSFORMATIONINST'):
            if inst.get('SINSTANCENAME') in targets_to_check:
                for conn_ref in inst.findall('CONNECTIONREFERENCE'):
                    conn_name = conn_ref.get('CONNECTIONNAME')
                    if conn_name:
                        return conn_name
                for attr in inst.findall('ATTRIBUTE'):
                    if 'Connection' in attr.get('NAME') and attr.get('VALUE'):
                        return attr.get('VALUE')
        
        # 3. Check SESSION attributes (fallback)
        for attr in session_node.findall('ATTRIBUTE'):
            if 'Connection' in attr.get('NAME') and attr.get('VALUE'):
                return attr.get('VALUE')
                
        return "N/A"

    def generate(self, workflow_name=None, folder_name=None):
        markdown_content = []
        
        folders = self.get_folders(folder_name)
        for folder in folders:
            f_name = folder.get('NAME')
            folder_added = False
            
            workflows = self.get_workflows(folder, workflow_name)
            for wf in workflows:
                wf_name = wf.get('NAME')
                wf_content = []
                wf_content.append(f"\n### Workflow: {wf_name}")
                
                sessions = self.get_sessions(wf)
                session_added = False
                for sess_task in sessions:
                    sess_name = sess_task.get('NAME')
                    sess_config = self.get_session_config(folder, sess_task.get('TASKNAME'))
                    if sess_config is None:
                        continue
                    
                    mapping_name = sess_config.get('MAPPINGNAME')
                    wf_content.append(f"\n#### Session: {sess_name} (Mapping: {mapping_name})")
                    
                    sources, targets = self.get_mapping_details(folder, mapping_name)
                    
                    if sources:
                        wf_content.append("\n**Sources:**")
                        wf_content.append("| Name | Type | Database Type | Connection |")
                        wf_content.append("| :--- | :--- | :--- | :--- |")
                        for src in sources:
                            conn = self.get_connection_info(folder, mapping_name, sess_config, src['name'], is_source=True)
                            wf_content.append(f"| {src['name']} | {src['type']} | {src['db_type']} | {conn} |")
                    
                    if targets:
                        wf_content.append("\n**Targets:**")
                        wf_content.append("| Name | Type | Database Type | Connection |")
                        wf_content.append("| :--- | :--- | :--- | :--- |")
                        for tgt in targets:
                            conn = self.get_connection_info(folder, mapping_name, sess_config, tgt['name'], is_source=False)
                            wf_content.append(f"| {tgt['name']} | {tgt['type']} | {tgt['db_type']} | {conn} |")
                    
                    session_added = True

                if session_added:
                    if not folder_added:
                        markdown_content.append(f"\n## Folder: {f_name}")
                        folder_added = True
                    markdown_content.extend(wf_content)
        
        return "\n".join(markdown_content)

def process_file(xml_path, workflow_name, folder_name):
    print(f"Processing XML: {xml_path}")
    try:
        generator = InformaticaLineageGenerator(xml_path)
        return generator.generate(workflow_name=workflow_name, folder_name=folder_name)
    except Exception as e:
        print(f"Error processing {xml_path}: {e}")
        return ""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Informatica XML Lineage to Markdown Generator')
    parser.add_argument('--xml', required=True, help='Path to Informatica XML file or directory')
    parser.add_argument('--workflow', help='Specific workflow name')
    parser.add_argument('--folder', help='Specific folder name')
    parser.add_argument('--output', help='Output Markdown file path')
    
    args = parser.parse_args()
    
    all_reports = []
    
    if os.path.isdir(args.xml):
        for root, dirs, files in os.walk(args.xml):
            for file in files:
                if file.endswith('.XML') or file.endswith('.xml'):
                    file_path = os.path.join(root, file)
                    report = process_file(file_path, args.workflow, args.folder)
                    if report:
                        all_reports.append(f"# XML File: {file}\n" + report)
    else:
        report = process_file(args.xml, args.workflow, args.folder)
        if report:
            all_reports.append(f"# XML File: {os.path.basename(args.xml)}\n" + report)
    
    if not all_reports:
        print("No lineage data found matching criteria.")
    else:
        full_report = "\n\n---\n\n".join(all_reports)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(full_report)
            print(f"Combined report generated: {args.output}")
        else:
            print(full_report)
