import xml.etree.ElementTree as ET
import argparse
import os
import pandas as pd

class InformaticaExcelGenerator:
    def __init__(self, xml_path):
        self.xml_path = xml_path
        self.tree = ET.parse(xml_path)
        self.root = self.tree.getroot()
        self.repo = self.root.find('REPOSITORY')
        self.repo_name = self.repo.get('NAME') if self.repo is not None else "Unknown"

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
        return [t for t in workflow_node.findall('TASKINSTANCE') if t.get('TASKTYPE') == 'Session']

    def get_session_config(self, folder_node, session_name):
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
                target_folder = None
                for f in self.repo.findall('FOLDER'):
                    if f.get('NAME') == ref_folder:
                        target_folder = f
                        break
                if target_folder is not None:
                    return self.get_definition_db_type(target_folder, ref_name, type_tag)
                else:
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
                for inst in m.findall('INSTANCE'):
                    if inst.get('TRANSFORMATION_TYPE') == 'Source Qualifier':
                        assoc_src = inst.find('ASSOCIATED_SOURCE_INSTANCE')
                        if assoc_src is not None and assoc_src.get('NAME') == source_instance_name:
                            return inst.get('NAME')
        return None

    def get_connection_info(self, folder_node, mapping_name, session_node, instance_name, is_source=False):
        if session_node is None:
            return "N/A"
        
        extensions = session_node.findall('SESSIONEXTENSION')
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

        for inst in session_node.findall('SESSTRANSFORMATIONINST'):
            if inst.get('SINSTANCENAME') in targets_to_check:
                for conn_ref in inst.findall('CONNECTIONREFERENCE'):
                    conn_name = conn_ref.get('CONNECTIONNAME')
                    if conn_name:
                        return conn_name
                for attr in inst.findall('ATTRIBUTE'):
                    if 'Connection' in attr.get('NAME') and attr.get('VALUE'):
                        return attr.get('VALUE')
        
        for attr in session_node.findall('ATTRIBUTE'):
            if 'Connection' in attr.get('NAME') and attr.get('VALUE'):
                return attr.get('VALUE')
                
        return "N/A"

    def get_lineage_rows(self, workflow_name=None, folder_name=None):
        rows = []
        xml_file = os.path.basename(self.xml_path)
        folders = self.get_folders(folder_name)
        for folder in folders:
            f_name = folder.get('NAME')
            workflows = self.get_workflows(folder, workflow_name)
            for wf in workflows:
                wf_name = wf.get('NAME')
                sessions = self.get_sessions(wf)
                for sess_task in sessions:
                    sess_name = sess_task.get('NAME')
                    sess_config = self.get_session_config(folder, sess_task.get('TASKNAME'))
                    if sess_config is None:
                        continue
                    
                    mapping_name = sess_config.get('MAPPINGNAME')
                    sources, targets = self.get_mapping_details(folder, mapping_name)
                    
                    for src in sources:
                        conn = self.get_connection_info(folder, mapping_name, sess_config, src['name'], is_source=True)
                        rows.append({
                            'Repository': self.repo_name,
                            'XML File': xml_file,
                            'Folder': f_name,
                            'Workflow Name': wf_name,
                            'Session Name': sess_name,
                            'Mapping Name': mapping_name,
                            'Object Type': 'Source',
                            'Object Name': src['name'],
                            'Object Type Detail': src['type'],
                            'Object Database Type': src['db_type'],
                            'Object Connection': conn
                        })
                    
                    for tgt in targets:
                        conn = self.get_connection_info(folder, mapping_name, sess_config, tgt['name'], is_source=False)
                        rows.append({
                            'Repository': self.repo_name,
                            'XML File': xml_file,
                            'Folder': f_name,
                            'Workflow Name': wf_name,
                            'Session Name': sess_name,
                            'Mapping Name': mapping_name,
                            'Object Type': 'Target',
                            'Object Name': tgt['name'],
                            'Object Type Detail': tgt['type'],
                            'Object Database Type': tgt['db_type'],
                            'Object Connection': conn
                        })
        return rows

def process_file_to_df(xml_path, workflow_name, folder_name):
    print(f"Processing XML: {xml_path}")
    try:
        generator = InformaticaExcelGenerator(xml_path)
        rows = generator.get_lineage_rows(workflow_name=workflow_name, folder_name=folder_name)
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"Error processing {xml_path}: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Informatica XML Lineage to Excel Generator')
    parser.add_argument('--xml', required=True, help='Path to Informatica XML file or directory')
    parser.add_argument('--workflow', help='Specific workflow name')
    parser.add_argument('--folder', help='Specific folder name')
    parser.add_argument('--output', required=True, help='Output Excel file path (.xlsx)')
    
    args = parser.parse_args()
    
    all_dfs = []
    
    if os.path.isdir(args.xml):
        for root, dirs, files in os.walk(args.xml):
            for file in files:
                if file.endswith('.XML') or file.endswith('.xml'):
                    file_path = os.path.join(root, file)
                    df = process_file_to_df(file_path, args.workflow, args.folder)
                    if not df.empty:
                        all_dfs.append(df)
    else:
        df = process_file_to_df(args.xml, args.workflow, args.folder)
        if not df.empty:
            all_dfs.append(df)
    
    if not all_dfs:
        print("No lineage data found matching criteria.")
    else:
        final_df = pd.concat(all_dfs, ignore_index=True)
        # Reorder columns to user preference + extras
        cols = ['Repository', 'XML File', 'Folder', 'Workflow Name', 'Session Name', 'Mapping Name', 
                'Object Type', 'Object Name', 'Object Type Detail', 'Object Database Type', 'Object Connection']
        final_df = final_df[cols]
        
        final_df.to_excel(args.output, index=False)
        print(f"Excel report generated: {args.output}")
