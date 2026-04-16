import xml.etree.ElementTree as ET
import argparse
import os
import re
import pandas as pd
from itertools import zip_longest
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class InformaticaExcelGeneratorEnhanced:
    def __init__(self, xml_path):
        self.xml_path = xml_path
        self.tree = ET.parse(xml_path)
        self.root = self.tree.getroot()
        logger.debug(f"XML root tag is: <{self.root.tag}>")
        
        self.repo = self.root.find('REPOSITORY')
        if self.repo is None:
            logger.warning(f"No <REPOSITORY> tag found at root. If this is a Mapping-only or Workflow-only export, standard workflow-driven parsing might fail.")
            
        self.repo_name = self.repo.get('NAME') if self.repo is not None else "Unknown"

    def get_folders(self, folder_name=None):
        if self.repo is None:
            return []
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

    def resolve_object_name(self, folder_node, trans_name):
        for s in folder_node.findall('SHORTCUT'):
            if s.get('NAME') == trans_name:
                return s.get('REFOBJECTNAME')
        
        if self.repo is not None:
            for f in self.repo.findall('FOLDER'):
                for s in f.findall('SHORTCUT'):
                    if s.get('NAME') == trans_name:
                        return s.get('REFOBJECTNAME')
                    
        return trans_name

    def get_definition_db_type(self, folder_node, name, type_tag):
        shortcuts = folder_node.findall('SHORTCUT')
        for s in shortcuts:
            if s.get('NAME') == name:
                ref_name = s.get('REFOBJECTNAME')
                ref_folder = s.get('FOLDERNAME')
                target_folder = None
                if self.repo is not None:
                    for f in self.repo.findall('FOLDER'):
                        if f.get('NAME') == ref_folder:
                            target_folder = f
                            break
                if target_folder is not None:
                    return self.get_definition_db_type(target_folder, ref_name, type_tag)
                else:
                    if self.repo is not None:
                        for f in self.repo.findall('FOLDER'):
                            definitions = f.findall(type_tag)
                            for d in definitions:
                                if d.get('NAME') == ref_name:
                                    return d.get('DATABASETYPE') or "N/A"

        definitions = folder_node.findall(type_tag)
        for d in definitions:
            if d.get('NAME') == name:
                return d.get('DATABASETYPE') or "N/A"
        
        if self.repo is not None:
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
        mapping_node = None
        # 1. Search in current folder
        for m in folder_node.findall('MAPPING'):
            if m.get('NAME') == mapping_name:
                mapping_node = m
                break
        
        # 2. Search globally if not found in current folder
        if mapping_node is None and self.repo is not None:
            for f in self.repo.findall('FOLDER'):
                for m in f.findall('MAPPING'):
                    if m.get('NAME') == mapping_name:
                        mapping_node = m
                        folder_node = f # Update folder_node context to where mapping was actually found
                        break
                if mapping_node:
                    break

        if mapping_node:
            sources = []
            targets = []
            instances = mapping_node.findall('INSTANCE')
            for inst in instances:
                inst_name = inst.get('NAME')
                trans_name = inst.get('TRANSFORMATION_NAME')
                inst_type = inst.get('TRANSFORMATION_TYPE')
                actual_type = inst.get('TYPE')
                
                display_name = self.resolve_object_name(folder_node, trans_name)
                
                if actual_type == 'SOURCE':
                    db_type = self.get_definition_db_type(folder_node, trans_name, 'SOURCE')
                    sources.append({'inst_name': inst_name, 'display_name': display_name, 'type': inst_type, 'db_type': db_type})
                elif actual_type == 'TARGET':
                    db_type = self.get_definition_db_type(folder_node, trans_name, 'TARGET')
                    targets.append({'inst_name': inst_name, 'display_name': display_name, 'type': inst_type, 'db_type': db_type})
            return sources, targets, mapping_node
        return [], [], None

    def get_associated_sq(self, mapping_node, source_instance_name):
        if mapping_node is None:
            return None
        for inst in mapping_node.findall('INSTANCE'):
            if inst.get('TRANSFORMATION_TYPE') == 'Source Qualifier':
                assoc_src = inst.find('ASSOCIATED_SOURCE_INSTANCE')
                if assoc_src is not None and assoc_src.get('NAME') == source_instance_name:
                    return inst.get('NAME')
        return None

    def get_connection_info(self, mapping_node, session_node, instance_name, is_source=False):
        if session_node is None:
            return "N/A"
        
        extensions = session_node.findall('SESSIONEXTENSION')
        targets_to_check = [instance_name]
        if is_source:
            sq_name = self.get_associated_sq(mapping_node, instance_name)
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

    def get_sql_queries(self, mapping_node, session_node):
        sq_query = "N/A"
        pre_sql = "N/A"
        post_sql = "N/A"

        if session_node is not None:
            for ext in session_node.findall('SESSIONEXTENSION'):
                for attr in ext.findall('TABLEATTRIBUTE'):
                    name = attr.get('NAME')
                    val = attr.get('VALUE')
                    if name == 'Sql Query' and val:
                        sq_query = val
                    elif name == 'Pre SQL' and val:
                        pre_sql = val
                    elif name == 'Post SQL' and val:
                        post_sql = val
            
            for attr in session_node.findall('ATTRIBUTE'):
                name = attr.get('NAME')
                val = attr.get('VALUE')
                if name == 'Pre SQL' and val:
                    pre_sql = val
                elif name == 'Post SQL' and val:
                    post_sql = val

        if mapping_node is not None and sq_query == "N/A":
            for trans in mapping_node.findall('TRANSFORMATION'):
                if trans.get('TYPE') == 'Source Qualifier':
                    for attr in trans.findall('TABLEATTRIBUTE'):
                        if attr.get('NAME') == 'Sql Query' and attr.get('VALUE'):
                            sq_query = attr.get('VALUE')
                            break
        
        return sq_query, pre_sql, post_sql

    def extract_tables_from_query(self, query):
        if not query or query == "N/A":
            return "N/A"
            
        try:
            parsed = sqlparse.parse(query)
            tables = []
            
            def _extract_tables(stmt):
                from_seen = False
                for token in stmt.tokens:
                    if from_seen:
                        if token.ttype is Keyword and token.value.upper() in ['WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT']:
                            from_seen = False
                        elif isinstance(token, IdentifierList):
                            for identifier in token.get_identifiers():
                                if isinstance(identifier, Identifier):
                                    val = identifier.value.split()[0]
                                    if val.upper() not in ['SELECT']:
                                        tables.append(val)
                        elif isinstance(token, Identifier):
                            val = token.value.split()[0]
                            if val.upper() not in ['SELECT']:
                                tables.append(val)
                            
                    if token.ttype is Keyword and token.value.upper() in ['FROM', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'LEFT OUTER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN']:
                        from_seen = True
                        
                    if hasattr(token, 'tokens'):
                        _extract_tables(token)
                        
            for stmt in parsed:
                _extract_tables(stmt)
                
            result = list(dict.fromkeys(tables))
            return ", ".join(result) if result else "N/A"
        except Exception as e:
            logger.debug(f"Error while parsing SQL: {e}")
            return "N/A"

    def get_lineage_rows(self, workflow_name=None, folder_name=None):
        rows = []
        xml_file = os.path.basename(self.xml_path)
        
        # High-level XML check to help user
        mappings_overall = self.root.findall('.//MAPPING')
        workflows_overall = self.root.findall('.//WORKFLOW')
        logger.debug(f"Overall File Summary => <MAPPING> tags: {len(mappings_overall)}, <WORKFLOW> tags: {len(workflows_overall)}")
        if len(workflows_overall) == 0 and len(mappings_overall) > 0:
            logger.warning("WARNING: This XML has Mappings but 0 Workflows. It looks like a Mapping-only export. The script logic starts at the Workflow level, so it will yield 0 rows.")

        folders = self.get_folders(folder_name)
        if not folders:
             logger.debug(f"No <FOLDER> tags found (or none matched folder_name='{folder_name}').")

        for folder in folders:
            f_name = folder.get('NAME')
            logger.debug(f"Processing Folder: {f_name}")
            
            workflows = self.get_workflows(folder, workflow_name)
            logger.debug(f"  Folder '{f_name}': Found {len(workflows)} Workflow(s) (wf filter: {workflow_name})")
            
            for wf in workflows:
                wf_name = wf.get('NAME')
                sessions = self.get_sessions(wf)
                logger.debug(f"    Workflow '{wf_name}': Found {len(sessions)} Session task(s)")
                
                for sess_task in sessions:
                    sess_name = sess_task.get('NAME')
                    sess_config = self.get_session_config(folder, sess_task.get('TASKNAME'))
                    if sess_config is None:
                        logger.debug(f"      Session task '{sess_name}': Did not find corresponding <SESSION> config. Skipping.")
                        continue
                    
                    mapping_name = sess_config.get('MAPPINGNAME')
                    sources, targets, mapping_node = self.get_mapping_details(folder, mapping_name)
                    logger.debug(f"      Session '{sess_name}': Maps to '{mapping_name}'. Found {len(sources)} Source(s) and {len(targets)} Target(s).")
                    
                    sq_query, pre_sql, post_sql = self.get_sql_queries(mapping_node, sess_config)
                    sq_tables = self.extract_tables_from_query(sq_query)
                    
                    for src, tgt in zip_longest(sources, targets):
                        row = {
                            'Repository': self.repo_name,
                            'XML File': xml_file,
                            'Folder': f_name,
                            'Workflow Name': wf_name,
                            'Session Name': sess_name,
                            'Mapping Name': mapping_name,
                            'Source Name': src['display_name'] if src else "N/A",
                            'Source Type Detail': src['type'] if src else "N/A",
                            'Source Database Type': src['db_type'] if src else "N/A",
                            'Source Connection': self.get_connection_info(mapping_node, sess_config, src['inst_name'], True) if src else "N/A",
                            'target Name': tgt['display_name'] if tgt else "N/A",
                            'target Type Detail': tgt['type'] if tgt else "N/A",
                            'target Database Type': tgt['db_type'] if tgt else "N/A",
                            'target Connection': self.get_connection_info(mapping_node, sess_config, tgt['inst_name'], False) if tgt else "N/A",
                            'source qualifier query': sq_query,
                            'Source Qualifier Tables': sq_tables,
                            'pre  sql': pre_sql,
                            'post  sql': post_sql
                        }
                        rows.append(row)
        return rows

def process_file_to_df(xml_path, workflow_name, folder_name):
    logger.info(f"Parsing XML file: {xml_path}")
    try:
        generator = InformaticaExcelGeneratorEnhanced(xml_path)
        rows = generator.get_lineage_rows(workflow_name=workflow_name, folder_name=folder_name)
        if len(rows) == 0:
            logger.info(f"-> 0 lineage rows extracted from {xml_path}")
        else:
            logger.info(f"-> {len(rows)} lineage rows extracted from {xml_path}")
        return pd.DataFrame(rows)
    except Exception as e:
        logger.error(f"Error parsing {xml_path}: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Informatica XML Lineage to Enhanced Excel Generator with Logging')
    parser.add_argument('--xml', required=True, help='Path to Informatica XML file or directory')
    parser.add_argument('--workflow', help='Specific workflow name')
    parser.add_argument('--folder', help='Specific folder name')
    parser.add_argument('--output', required=True, help='Output Excel file path (.xlsx)')
    parser.add_argument('--debug', action='store_true', help='Enable detailed debug logging to see where data drops off')
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled.")
    
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
        logger.error("No lineage data found matching criteria.")
    else:
        final_df = pd.concat(all_dfs, ignore_index=True)
        cols = ['Repository', 'XML File', 'Folder', 'Workflow Name', 'Session Name', 'Mapping Name', 
                'Source Name', 'Source Type Detail', 'Source Database Type', 'Source Connection',
                'target Name', 'target Type Detail', 'target Database Type', 'target Connection',
                'source qualifier query', 'Source Qualifier Tables', 'pre  sql', 'post  sql']
        final_df = final_df[cols]
        final_df.to_excel(args.output, index=False)
        logger.info(f"Enhanced Excel report generated: {args.output}")

