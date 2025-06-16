import json
import csv
import pandas as pd
import networkx as nx
from typing import List, Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class ConnectorInfo:
    mapping_number: int
    mapping_name: str
    from_field: str
    from_instance: str
    from_instance_type: str
    to_field: str
    to_instance: str
    to_instance_type: str
    transformation_order: int = 0
    transformation_logic: str = ""

class InformaticaMappingExtractor:
    def __init__(self):
        self.connectors_data = []
        self.transformation_logic_cache = {}
        
    def extract_mappings_to_csv(self, json_file_path: str, output_csv_path: str = None) -> List[ConnectorInfo]:
        """
        Extract all mappings and their connectors from Informatica workflow JSON
        and optionally save to CSV with lineage analysis
        
        Args:
            json_file_path: Path to the workflow JSON file
            output_csv_path: Optional path to save CSV file
            
        Returns:
            List of ConnectorInfo objects
        """
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                workflow_data = json.load(file)
            
            return self._process_workflow_data(workflow_data, output_csv_path)
            
        except FileNotFoundError:
            print(f"File not found: {json_file_path}")
            return []
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return []
    
    def extract_mappings_from_dict(self, workflow_data: Dict[str, Any], output_csv_path: str = None) -> List[ConnectorInfo]:
        """
        Extract mappings and connectors from workflow data dictionary
        
        Args:
            workflow_data: Workflow data as dictionary
            output_csv_path: Optional path to save CSV file
            
        Returns:
            List of ConnectorInfo objects
        """
        return self._process_workflow_data(workflow_data, output_csv_path)
    
    def extract_mappings_from_string(self, json_string: str, output_csv_path: str = None) -> List[ConnectorInfo]:
        """
        Extract mappings and connectors from JSON string
        
        Args:
            json_string: Workflow JSON as string
            output_csv_path: Optional path to save CSV file
            
        Returns:
            List of ConnectorInfo objects
        """
        try:
            workflow_data = json.loads(json_string)
            return self._process_workflow_data(workflow_data, output_csv_path)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON string: {e}")
            return []
    
    def _process_workflow_data(self, workflow_data: Dict[str, Any], output_csv_path: str = None) -> List[ConnectorInfo]:
        """Process the workflow data and extract connector information with lineage analysis"""
        self.connectors_data = []
        
        # Navigate to mappings based on the structure shown in the image
        mappings = self._extract_mappings(workflow_data)
        
        if not mappings:
            print("No mappings found in the workflow data")
            return []
        
        print(f"Found {len(mappings)} mapping(s)")
        
        # Process each mapping
        for mapping_number, mapping_data in enumerate(mappings, 1):
            mapping_name = self._get_mapping_name(mapping_data)
            print(f"Processing Mapping {mapping_number}: {mapping_name}")
            
            # Extract transformation logic for this mapping
            self._extract_transformation_logic(mapping_data, mapping_name)
            
            # Extract connectors from this mapping
            connectors = self._extract_connectors_from_mapping(mapping_data)
            print(f"  Found {len(connectors)} connector(s)")
            
            # Build lineage graph and calculate transformation order
            transformation_orders = self._calculate_transformation_order(connectors, mapping_name)
            
            # Process each connector with order and logic
            for connector in connectors:
                connector_info = self._process_connector_with_lineage(
                    mapping_number, mapping_name, connector, transformation_orders
                )
                if connector_info:
                    self.connectors_data.append(connector_info)
        
        # Save to CSV if path provided
        if output_csv_path:
            self._save_to_csv(output_csv_path)
        
        return self.connectors_data
    
    def _extract_mappings(self, workflow_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract all mappings from the workflow data"""
        mappings = []
        
        try:
            # Based on the structure: POWERMART > REPOSITORY > FOLDER > MAPPING
            powermart = workflow_data.get('POWERMART', {})
            repository = powermart.get('REPOSITORY', {})
            folder_data = repository.get('FOLDER', {})
            
            # Handle FOLDER as either dict or list
            folders_to_process = []
            if isinstance(folder_data, dict):
                folders_to_process = [folder_data]
            elif isinstance(folder_data, list):
                folders_to_process = folder_data
            else:
                print("No FOLDER structure found in the expected location")
                return []
            
            # Process each folder to find mappings
            for folder in folders_to_process:
                mapping_data = folder.get('MAPPING', [])
                
                # Handle both single mapping (dict) and multiple mappings (list)
                if isinstance(mapping_data, dict):
                    mappings.append(mapping_data)
                elif isinstance(mapping_data, list):
                    mappings.extend(mapping_data)
            
            if not mappings:
                print("No MAPPING structure found in any folder")
                
        except Exception as e:
            print(f"Error extracting mappings: {e}")
            
        return mappings
    
    def _get_mapping_name(self, mapping_data: Dict[str, Any]) -> str:
        """Extract mapping name from mapping data"""
        name_fields = ['@NAME', 'NAME', '@name', 'name']
        for field in name_fields:
            if field in mapping_data:
                return str(mapping_data[field])
        return "Unknown Mapping"
    
    def _extract_transformation_logic(self, mapping_data: Dict[str, Any], mapping_name: str):
        """Extract transformation logic for all transformations in the mapping"""
        self.transformation_logic_cache[mapping_name] = {}
        
        # Look for TRANSFORMATION array
        transformations = mapping_data.get('TRANSFORMATION', [])
        if isinstance(transformations, dict):
            transformations = [transformations]
        
        for transformation in transformations:
            trans_name = self._get_transformation_name(transformation)
            trans_type = self._get_transformation_type(transformation)
            
            # Extract logic based on transformation type
            logic = self._extract_logic_by_type(transformation, trans_type)
            
            self.transformation_logic_cache[mapping_name][trans_name] = {
                'type': trans_type,
                'logic': logic
            }
    
    def _get_transformation_name(self, transformation: Dict[str, Any]) -> str:
        """Extract transformation name"""
        name_fields = ['@NAME', 'NAME', '@name', 'name']
        for field in name_fields:
            if field in transformation:
                return str(transformation[field])
        return "Unknown Transformation"
    
    def _get_transformation_type(self, transformation: Dict[str, Any]) -> str:
        """Extract transformation type"""
        type_fields = ['@TYPE', 'TYPE', '@type', 'type']
        for field in type_fields:
            if field in transformation:
                return str(transformation[field])
        return "Unknown Type"
    
    def _extract_logic_by_type(self, transformation: Dict[str, Any], trans_type: str) -> str:
        """Extract logic based on transformation type"""
        logic_parts = []
        
        # Get transformation fields for detailed logic
        transform_fields = transformation.get('TRANSFORMFIELD', [])
        if isinstance(transform_fields, dict):
            transform_fields = [transform_fields]
        
        if trans_type.upper() in ['EXPRESSION', 'EXPRESSION TRANSFORMATION']:
            # Extract expression logic
            for field in transform_fields:
                field_name = field.get('@NAME', field.get('NAME', ''))
                expression = field.get('@EXPRESSION', field.get('EXPRESSION', ''))
                if expression and expression.strip():
                    logic_parts.append(f"{field_name} = {expression}")
        
        elif trans_type.upper() in ['FILTER', 'FILTER TRANSFORMATION']:
            # Extract filter condition from multiple possible locations
            filter_condition = transformation.get('@FILTERCONDITION', 
                                                 transformation.get('FILTERCONDITION', ''))
            
            # If not found directly, look in TABLEATTRIBUTE
            if not filter_condition:
                table_attributes = transformation.get('TABLEATTRIBUTE', [])
                if isinstance(table_attributes, dict):
                    table_attributes = [table_attributes]
                
                for attr in table_attributes:
                    attr_name = attr.get('@NAME', attr.get('NAME', ''))
                    if attr_name and 'filter' in attr_name.lower():
                        attr_value = attr.get('@VALUE', attr.get('VALUE', ''))
                        if attr_value:
                            filter_condition = attr_value
                            break
            
            if filter_condition:
                logic_parts.append(f"Filter: {filter_condition}")
        
        elif trans_type.upper() in ['LOOKUP', 'LOOKUP TRANSFORMATION']:
            # Extract lookup condition
            lookup_condition = transformation.get('@CONDITION', 
                                                 transformation.get('CONDITION', ''))
            if lookup_condition:
                logic_parts.append(f"Lookup: {lookup_condition}")
        
        elif trans_type.upper() in ['AGGREGATOR', 'AGGREGATOR TRANSFORMATION']:
            # Extract group by and aggregate expressions
            for field in transform_fields:
                field_name = field.get('@NAME', field.get('NAME', ''))
                expression = field.get('@EXPRESSION', field.get('EXPRESSION', ''))
                group_by = field.get('@GROUPBY', field.get('GROUPBY', ''))
                if group_by == 'YES':
                    logic_parts.append(f"GROUP BY: {field_name}")
                elif expression and expression.strip():
                    logic_parts.append(f"{field_name} = {expression}")
        
        elif trans_type.upper() in ['SORTER', 'SORTER TRANSFORMATION']:
            # Extract sort keys
            for field in transform_fields:
                field_name = field.get('@NAME', field.get('NAME', ''))
                sort_key = field.get('@SORTKEY', field.get('SORTKEY', ''))
                if sort_key == 'YES':
                    logic_parts.append(f"SORT BY: {field_name}")
        
        else:
            # For other transformation types, extract basic field mappings and any table attributes
            for field in transform_fields:
                field_name = field.get('@NAME', field.get('NAME', ''))
                if field_name:
                    logic_parts.append(f"Field: {field_name}")
            
            # Also check for any important TABLEATTRIBUTE values
            table_attributes = transformation.get('TABLEATTRIBUTE', [])
            if isinstance(table_attributes, dict):
                table_attributes = [table_attributes]
            
            for attr in table_attributes:
                attr_name = attr.get('@NAME', attr.get('NAME', ''))
                attr_value = attr.get('@VALUE', attr.get('VALUE', ''))
                
                # Extract meaningful attributes (not just metadata)
                if attr_name and attr_value and len(attr_value.strip()) > 0:
                    # Skip common metadata attributes
                    skip_attrs = ['version', 'creation_date', 'modified_date', 'uuid', 'description']
                    if not any(skip in attr_name.lower() for skip in skip_attrs):
                        logic_parts.append(f"{attr_name}: {attr_value}")
        
        return "; ".join(logic_parts) if logic_parts else f"{trans_type} transformation"
    
    def _extract_connectors_from_mapping(self, mapping_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract all connectors from a specific mapping"""
        connectors = []
        
        # Look for CONNECTOR field (can be list or dict)
        connector_data = mapping_data.get('CONNECTOR', [])
        
        if isinstance(connector_data, dict):
            connectors = [connector_data]
        elif isinstance(connector_data, list):
            connectors = connector_data
        
        return connectors
    
    def _calculate_transformation_order(self, connectors: List[Dict[str, Any]], mapping_name: str) -> Dict[str, int]:
        """Calculate transformation order using NetworkX lineage analysis"""
        # Create directed graph
        G = nx.DiGraph()
        
        # Add nodes and edges from connectors
        all_instances = set()
        edges = []
        
        for connector in connectors:
            from_instance = connector.get('@FROMINSTANCE', connector.get('FROMINSTANCE', ''))
            to_instance = connector.get('@TOINSTANCE', connector.get('TOINSTANCE', ''))
            
            if from_instance and to_instance:
                all_instances.add(from_instance)
                all_instances.add(to_instance)
                edges.append((from_instance, to_instance))
        
        # Add all instances as nodes
        G.add_nodes_from(all_instances)
        G.add_edges_from(edges)
        
        print(f"  Created lineage graph with {len(G.nodes)} nodes and {len(G.edges)} edges")
        
        # Calculate transformation order
        transformation_orders = {}
        
        try:
            # Handle disconnected components
            connected_components = list(nx.weakly_connected_components(G))
            
            for component in connected_components:
                subgraph = G.subgraph(component)
                
                if len(component) == 1:
                    # Single node with no connections
                    node = list(component)[0]
                    transformation_orders[node] = 0
                    continue
                
                # Calculate levels using topological sort
                try:
                    # Get sources (nodes with no incoming edges)
                    sources = [n for n in subgraph.nodes() if subgraph.in_degree(n) == 0]
                    if not sources:
                        # Handle cycles by picking nodes with minimum in-degree
                        min_in_degree = min(subgraph.in_degree(n) for n in subgraph.nodes())
                        sources = [n for n in subgraph.nodes() if subgraph.in_degree(n) == min_in_degree]
                    
                    # BFS to assign levels
                    levels = {node: 0 for node in sources}
                    queue = sources.copy()
                    visited = set(sources)
                    
                    while queue:
                        current = queue.pop(0)
                        current_level = levels[current]
                        
                        for successor in subgraph.successors(current):
                            if successor not in visited:
                                levels[successor] = current_level + 1
                                queue.append(successor)
                                visited.add(successor)
                            else:
                                # Update level if we found a longer path
                                levels[successor] = max(levels[successor], current_level + 1)
                    
                    # Assign orders (add 1 to make it 1-based instead of 0-based)
                    for node, level in levels.items():
                        transformation_orders[node] = level + 1
                        
                except nx.NetworkXError:
                    # Fallback for problematic graphs
                    for i, node in enumerate(component):
                        transformation_orders[node] = i + 1
        
        except Exception as e:
            print(f"  Error calculating transformation order: {e}")
            # Fallback: assign sequential numbers
            for i, instance in enumerate(all_instances):
                transformation_orders[instance] = i + 1
        
        # Handle transformations not in connectors (unconnected)
        if mapping_name in self.transformation_logic_cache:
            for trans_name in self.transformation_logic_cache[mapping_name]:
                if trans_name not in transformation_orders:
                    transformation_orders[trans_name] = 0
        
        print(f"  Transformation orders: {transformation_orders}")
        return transformation_orders
    
    def _process_connector_with_lineage(self, mapping_number: int, mapping_name: str, 
                                      connector: Dict[str, Any], 
                                      transformation_orders: Dict[str, int]) -> Optional[ConnectorInfo]:
        """Process a single connector with lineage information"""
        try:
            # Extract connector information using @ prefix (XML attributes in JSON)
            from_field = connector.get('@FROMFIELD', '')
            from_instance = connector.get('@FROMINSTANCE', '')
            from_instance_type = connector.get('@FROMINSTANCETYPE', '')
            to_field = connector.get('@TOFIELD', '')
            to_instance = connector.get('@TOINSTANCE', '')
            to_instance_type = connector.get('@TOINSTANCETYPE', '')
            
            # Also try without @ prefix in case the JSON structure varies
            if not from_field:
                from_field = connector.get('FROMFIELD', '')
            if not from_instance:
                from_instance = connector.get('FROMINSTANCE', '')
            if not from_instance_type:
                from_instance_type = connector.get('FROMINSTANCETYPE', '')
            if not to_field:
                to_field = connector.get('TOFIELD', '')
            if not to_instance:
                to_instance = connector.get('TOINSTANCE', '')
            if not to_instance_type:
                to_instance_type = connector.get('TOINSTANCETYPE', '')
            
            # Get transformation order (use TO instance order as it represents where data flows to)
            trans_order = transformation_orders.get(to_instance, 0)
            
            # Get transformation logic
            trans_logic = ""
            if mapping_name in self.transformation_logic_cache:
                if to_instance in self.transformation_logic_cache[mapping_name]:
                    trans_logic = self.transformation_logic_cache[mapping_name][to_instance]['logic']
                # Also check from_instance for additional context
                elif from_instance in self.transformation_logic_cache[mapping_name]:
                    trans_logic = self.transformation_logic_cache[mapping_name][from_instance]['logic']
            
            return ConnectorInfo(
                mapping_number=mapping_number,
                mapping_name=mapping_name,
                from_field=from_field,
                from_instance=from_instance,
                from_instance_type=from_instance_type,
                to_field=to_field,
                to_instance=to_instance,
                to_instance_type=to_instance_type,
                transformation_order=trans_order,
                transformation_logic=trans_logic
            )
            
        except Exception as e:
            print(f"Error processing connector: {e}")
            return None
    
    def _save_to_csv(self, output_csv_path: str):
        """Save connector data to CSV file with lineage information"""
        try:
            with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'mapping_number', 'mapping_name', 'FROMFIELD', 'FROMINSTANCE', 
                    'FROMINSTANCETYPE', 'TOFIELD', 'TOINSTANCE', 'TOINSTANCETYPE',
                    'transformation_order', 'transformation_logic'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write data
                for connector in self.connectors_data:
                    writer.writerow({
                        'mapping_number': connector.mapping_number,
                        'mapping_name': connector.mapping_name,
                        'FROMFIELD': connector.from_field,
                        'FROMINSTANCE': connector.from_instance,
                        'FROMINSTANCETYPE': connector.from_instance_type,
                        'TOFIELD': connector.to_field,
                        'TOINSTANCE': connector.to_instance,
                        'TOINSTANCETYPE': connector.to_instance_type,
                        'transformation_order': connector.transformation_order,
                        'transformation_logic': connector.transformation_logic
                    })
            
            print(f"CSV file saved successfully: {output_csv_path}")
            print(f"Total records written: {len(self.connectors_data)}")
            
        except Exception as e:
            print(f"Error saving CSV file: {e}")
    
    def get_dataframe(self) -> pd.DataFrame:
        """Convert connector data to pandas DataFrame"""
        if not self.connectors_data:
            return pd.DataFrame()
        
        data_dict = {
            'mapping_number': [c.mapping_number for c in self.connectors_data],
            'mapping_name': [c.mapping_name for c in self.connectors_data],
            'FROMFIELD': [c.from_field for c in self.connectors_data],
            'FROMINSTANCE': [c.from_instance for c in self.connectors_data],
            'FROMINSTANCETYPE': [c.from_instance_type for c in self.connectors_data],
            'TOFIELD': [c.to_field for c in self.connectors_data],
            'TOINSTANCE': [c.to_instance for c in self.connectors_data],
            'TOINSTANCETYPE': [c.to_instance_type for c in self.connectors_data],
            'transformation_order': [c.transformation_order for c in self.connectors_data],
            'transformation_logic': [c.transformation_logic for c in self.connectors_data]
        }
        
        return pd.DataFrame(data_dict)
    
    def print_summary(self):
        """Print a summary of extracted data with lineage information"""
        if not self.connectors_data:
            print("No connector data found")
            return
        
        print(f"\n=== EXTRACTION SUMMARY ===")
        print(f"Total connectors extracted: {len(self.connectors_data)}")
        
        # Group by mapping
        mappings = {}
        for connector in self.connectors_data:
            if connector.mapping_name not in mappings:
                mappings[connector.mapping_name] = {'count': 0, 'max_order': 0}
            mappings[connector.mapping_name]['count'] += 1
            mappings[connector.mapping_name]['max_order'] = max(
                mappings[connector.mapping_name]['max_order'], 
                connector.transformation_order
            )
        
        print(f"Mappings processed: {len(mappings)}")
        for mapping_name, info in mappings.items():
            print(f"  - {mapping_name}: {info['count']} connectors, max order: {info['max_order']}")
        
        # Show sample data
        if len(self.connectors_data) > 0:
            print(f"\n=== SAMPLE DATA (First 3 records) ===")
            for i, connector in enumerate(self.connectors_data[:3]):
                print(f"\nRecord {i+1}:")
                print(f"  Mapping: {connector.mapping_name}")
                print(f"  From: {connector.from_instance}.{connector.from_field} ({connector.from_instance_type})")
                print(f"  To: {connector.to_instance}.{connector.to_field} ({connector.to_instance_type})")
                print(f"  Order: {connector.transformation_order}")
                print(f"  Logic: {connector.transformation_logic[:100]}{'...' if len(connector.transformation_logic) > 100 else ''}")
    
    def get_lineage_summary(self, mapping_name: str = None) -> Dict[str, Any]:
        """Get lineage summary for a specific mapping or all mappings"""
        if mapping_name:
            connectors = [c for c in self.connectors_data if c.mapping_name == mapping_name]
        else:
            connectors = self.connectors_data
        
        if not connectors:
            return {}
        
        # Group by transformation order
        order_groups = defaultdict(list)
        for connector in connectors:
            order_groups[connector.transformation_order].append(connector)
        
        summary = {
            'total_transformations': len(set(c.to_instance for c in connectors)),
            'max_order': max(c.transformation_order for c in connectors),
            'unconnected_count': len([c for c in connectors if c.transformation_order == 0]),
            'order_distribution': {order: len(instances) for order, instances in order_groups.items()}
        }
        
        return summary
    
    def filter_by_mapping(self, mapping_name: str) -> List[ConnectorInfo]:
        """Filter connectors by mapping name"""
        return [c for c in self.connectors_data if mapping_name.lower() in c.mapping_name.lower()]
    
    def filter_by_instance_type(self, instance_type: str) -> List[ConnectorInfo]:
        """Filter connectors by instance type (from or to)"""
        return [c for c in self.connectors_data 
                if instance_type.lower() in c.from_instance_type.lower() or 
                   instance_type.lower() in c.to_instance_type.lower()]
    
    def filter_by_order(self, order: int) -> List[ConnectorInfo]:
        """Filter connectors by transformation order"""
        return [c for c in self.connectors_data if c.transformation_order == order]

    def debug_structure(self, workflow_data: Dict[str, Any], max_depth: int = 3) -> None:
        """Debug method to inspect the structure of workflow data"""
        def inspect_structure(data, prefix="", depth=0):
            if depth > max_depth:
                return
            
            if isinstance(data, dict):
                for key, value in data.items():
                    type_info = type(value).__name__
                    if isinstance(value, (dict, list)):
                        size_info = f" (size: {len(value)})" if hasattr(value, '__len__') else ""
                        print(f"{prefix}{key}: {type_info}{size_info}")
                        if depth < max_depth:
                            inspect_structure(value, prefix + "  ", depth + 1)
                    else:
                        print(f"{prefix}{key}: {type_info} = {str(value)[:50]}{'...' if len(str(value)) > 50 else ''}")
            elif isinstance(data, list):
                print(f"{prefix}[List with {len(data)} items]")
                if data and depth < max_depth:
                    print(f"{prefix}  Sample item 0:")
                    inspect_structure(data[0], prefix + "    ", depth + 1)
        
        print("=== WORKFLOW DATA STRUCTURE DEBUG ===")
        inspect_structure(workflow_data)


# Example usage and testing
def example_usage():
    """Example of how to use the enhanced InformaticaMappingExtractor"""
    
    # Initialize the extractor
    extractor = InformaticaMappingExtractor()
    
    # Example workflow data structure with more complex lineage
    sample_workflow = {
        "POWERMART": {
            "REPOSITORY": {
                "FOLDER": {
                    "MAPPING": [
                        {
                            "@NAME": "m_UDM_STG_ACCOUNT_STATUS_FROM_PLD",
                            "@DESCRIPTION": "",
                            "@ISVALID": "YES",
                            "TRANSFORMATION": [
                                {
                                    "@NAME": "SQ_SOURCE",
                                    "@TYPE": "Source Qualifier",
                                    "TRANSFORMFIELD": [
                                        {"@NAME": "CUSTOMER_ID", "@EXPRESSION": ""},
                                        {"@NAME": "ACCOUNT_STATUS", "@EXPRESSION": ""}
                                    ]
                                },
                                {
                                    "@NAME": "EXP_TRANSFORM",
                                    "@TYPE": "Expression",
                                    "TRANSFORMFIELD": [
                                        {
                                            "@NAME": "O_CUSTOMER_ID",
                                            "@EXPRESSION": "LTRIM(RTRIM(CUSTOMER_ID))"
                                        },
                                        {
                                            "@NAME": "O_BATCH_ID", 
                                            "@EXPRESSION": "$$BATCH_ID"
                                        }
                                    ]
                                },
                                {
                                    "@NAME": "fil_new_rows",
                                    "@TYPE": "Filter",
                                    "@FILTERCONDITION": "NOT ISNULL(O_CUSTOMER_ID)"
                                }
                            ],
                            "CONNECTOR": [
                                {
                                    "@FROMFIELD": "CUSTOMER_ID",
                                    "@FROMINSTANCE": "SQ_SOURCE",
                                    "@FROMINSTANCETYPE": "Source Qualifier",
                                    "@TOFIELD": "CUSTOMER_ID",
                                    "@TOINSTANCE": "EXP_TRANSFORM",
                                    "@TOINSTANCETYPE": "Expression"
                                },
                                {
                                    "@FROMFIELD": "O_CUSTOMER_ID",
                                    "@FROMINSTANCE": "EXP_TRANSFORM",
                                    "@FROMINSTANCETYPE": "Expression",
                                    "@TOFIELD": "CUSTOMER_ID",
                                    "@TOINSTANCE": "fil_new_rows",
                                    "@TOINSTANCETYPE": "Filter"
                                },
                                {
                                    "@FROMFIELD": "O_BATCH_ID",
                                    "@FROMINSTANCE": "fil_new_rows",
                                    "@FROMINSTANCETYPE": "Filter",
                                    "@TOFIELD": "BATCH_ID",
                                    "@TOINSTANCE": "ACCOUNT_STATUS",
                                    "@TOINSTANCETYPE": "Target Definition"
                                }
                            ]
                        }
                    ]
                }
            }
        }
    }
    
    # Extract data with lineage analysis
    connectors = extractor.extract_mappings_from_dict(sample_workflow, 'enhanced_output.csv')
    
    # Print summary
    extractor.print_summary()
    
    # Get lineage summary
    lineage_summary = extractor.get_lineage_summary()
    print(f"\n=== LINEAGE SUMMARY ===")
    print(f"Total transformations: {lineage_summary.get('total_transformations', 0)}")
    print(f"Max order: {lineage_summary.get('max_order', 0)}")
    print(f"Unconnected transformations: {lineage_summary.get('unconnected_count', 0)}")
    print(f"Order distribution: {lineage_summary.get('order_distribution', {})}")
    
    # Get as DataFrame
    df = extractor.get_dataframe()
    print(f"\nDataFrame shape: {df.shape}")
    print(df[['FROMINSTANCE', 'TOINSTANCE', 'transformation_order', 'transformation_logic']].head())
    
    return connectors

if __name__ == "__main__":
    example_usage()