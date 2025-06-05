import json
import networkx as nx
import pandas as pd
import argparse
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class PowerCenterLineage:
    def __init__(self, json_file_path, mapping_name):
        self.json_data = self._load_json(json_file_path)
        self.mapping_name = mapping_name
        self.lineage_data = []

    def _load_json(self, json_file_path):
        """Load and parse the JSON file."""
        try:
            with open(json_file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load JSON file: {e}")
            raise

    def _extract_mapping(self):
        """Extract the specified mapping from the JSON."""
        try:
            repository = self.json_data['POWERMART']['REPOSITORY']
            folder = repository['FOLDER']
            if isinstance(folder, dict):
                folder = [folder]
            mappings = []
            for f in folder:
                if 'MAPPING' in f:
                    mappings.extend(f['MAPPING'] if isinstance(f['MAPPING'], list) else [f['MAPPING']])
            mapping = next((m for m in mappings if m.get('@NAME') == self.mapping_name), None)
            if not mapping:
                logging.error(f"Mapping {self.mapping_name} not found in JSON")
                raise ValueError(f"Mapping {self.mapping_name} not found")
            logging.info(f"Found mapping: {self.mapping_name}")
            return mapping
        except Exception as e:
            logging.error(f"Error extracting mapping: {e}")
            raise

    def _build_lineage_graph(self, mapping):
        """Build a directed graph using connector information."""
        G = nx.DiGraph()
        mapping_name = mapping.get('@NAME', 'Unknown')
        logging.info(f"Building graph for mapping: {mapping_name}")

        # Extract connectors
        connectors = mapping.get('CONNECTOR', [])
        if not isinstance(connectors, list):
            connectors = [connectors] if connectors else []
            logging.warning(f"Connectors for {mapping_name} coerced to list: {connectors}")

        # Add nodes and edges from connectors
        for connector in connectors:
            if not isinstance(connector, dict):
                logging.warning(f"Skipping invalid connector in {mapping_name}: {connector}")
                continue
            from_field = connector.get('@FROMFIELD')
            to_field = connector.get('@TOFIELD')
            from_instance = connector.get('@FROMINSTANCE')
            to_instance = connector.get('@TOINSTANCE')
            from_instance_type = connector.get('@FROMINSTANCETYPE')
            to_instance_type = connector.get('@TOINSTANCETYPE')

            if not all([from_field, to_field, from_instance, to_instance, from_instance_type, to_instance_type]):
                logging.warning(f"Skipping connector with missing attributes in {mapping_name}: {connector}")
                continue

            # Normalize instance types for node categorization
            node_type_from = 'source' if from_instance_type == 'Source Definition' else \
                            'target' if from_instance_type == 'Target Definition' else 'transformation'
            node_type_to = 'source' if to_instance_type == 'Source Definition' else \
                          'target' if to_instance_type == 'Target Definition' else 'transformation'

            from_node = f"{from_instance_type}.{from_instance}.{from_field}"
            to_node = f"{to_instance_type}.{to_instance}.{to_field}"

            # Add nodes with attributes
            G.add_node(from_node, type=node_type_from, field=from_field, instance=from_instance,
                       instance_type=from_instance_type)
            G.add_node(to_node, type=node_type_to, field=to_field, instance=to_instance,
                       instance_type=to_instance_type)

            # Add edge
            G.add_edge(from_node, to_node)
            logging.debug(f"Added edge: {from_node} -> {to_node}")

        return G, mapping_name

    def _trace_lineage(self, graph, target_node):
        """Trace lineage for a target node back to source fields."""
        lineage = []
        visited = set()

        def dfs(node):
            if node in visited:
                return
            visited.add(node)
            node_data = graph.nodes.get(node, {})
            if not node_data:
                logging.warning(f"Node {node} has no attributes, skipping")
                return
            lineage.append({
                'node': node,
                'type': node_data.get('type', 'unknown'),
                'field': node_data.get('field', 'unknown'),
                'instance': node_data.get('instance', 'unknown'),
                'instance_type': node_data.get('instance_type', '')
            })
            for predecessor in graph.predecessors(node):
                dfs(predecessor)

        dfs(target_node)
        return lineage[::-1]  # Reverse to show source-to-target flow

    def generate_lineage(self):
        """Generate lineage for all target fields in the specified mapping."""
        try:
            mapping = self._extract_mapping()
            graph, mapping_name = self._build_lineage_graph(mapping)
            target_nodes = [node for node in graph.nodes if graph.nodes.get(node, {}).get('type') == 'target']
            if not target_nodes:
                logging.warning(f"No target nodes found in mapping {mapping_name}")
            for target_node in target_nodes:
                lineage = self._trace_lineage(graph, target_node)
                if lineage:
                    self.lineage_data.append({
                        'mapping': mapping_name,
                        'target_field': graph.nodes.get(target_node, {}).get('field', 'unknown'),
                        'target_instance': graph.nodes.get(target_node, {}).get('instance', 'unknown'),
                        'lineage': lineage
                    })
                    logging.info(f"Generated lineage for {target_node} in mapping {mapping_name}")
        except Exception as e:
            logging.error(f"Error processing mapping {self.mapping_name}: {e}")
            raise

    def export_lineage(self, output_format='csv', output_file='lineage_output.csv'):
        """Export lineage data to a file."""
        try:
            if output_format == 'json':
                with open(output_file, 'w') as f:
                    json.dump(self.lineage_data, f, indent=2)
                logging.info(f"Exported lineage to {output_file} in JSON format")
            elif output_format == 'csv':
                df = pd.DataFrame([
                    {
                        'mapping': entry['mapping'],
                        'target_field': entry['target_field'],
                        'target_instance': entry['target_instance'],
                        'lineage': ' -> '.join([
                            f"{l['type']}:{l['instance']}:{l['field']}[{l['instance_type']}]"
                            for l in entry['lineage']
                        ])
                    }
                    for entry in self.lineage_data
                ])
                df.to_csv(output_file, index=False)
                logging.info(f"Exported lineage to {output_file} in CSV format")
        except Exception as e:
            logging.error(f"Error exporting lineage: {e}")
            raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate column-level lineage for a PowerCenter mapping using connectors.")
    parser.add_argument('--json_file', default='/Users/sathishkumar/AllFiles/miami/XMLify/xmls/wf_complete.json', help='Path to the JSON file')
    parser.add_argument('--mapping_name', required=True, help='Name of the mapping to process (e.g., m_PNC_STG_MST_PNC_ACLS_ACCT_MSTR)')
    parser.add_argument('--output_file', default='lineage_output_conn.csv', help='Output file for lineage data')
    args = parser.parse_args()

    lineage_framework = PowerCenterLineage(args.json_file, args.mapping_name)
    lineage_framework.generate_lineage()
    lineage_framework.export_lineage(output_format='csv', output_file=args.output_file)

# python conn_mapping_linear.py --mapping_name m_PNC_STG_MST_PNC_ACLS_ACCT_MSTR
