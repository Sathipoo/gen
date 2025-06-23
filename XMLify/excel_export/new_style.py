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
    parallel_group: str = ""
    execution_level: int = 0

class InfaLineageGenerator:
    def __init__(self, mapping_name: str, workflow_data: Dict[str, Any]):
        self.mapping_name = mapping_name
        self.workflow_data = workflow_data
        self.connectors = []
        self.transformations = []
        self.instances = []
        self.mapping_lineage_graph = None
        self.transformation_logic_cache = {}
        self.connectors_data = []
        self.mapping_exists,self.mapping_data,self.mapping_number = self.check_if_mapping_exists()
        if self.mapping_exists:
            print("Mapping exists")
        else:
            # print(f"Mapping {self.mapping_name} does not exist in the workflow")
            raise Exception(f"Mapping {self.mapping_name} does not exist in the workflow")
        self.main()

    def main(self):
        self.fetch_all_connectors()
        self.fetch_all_transformations()
        self.fetch_all_instances()
        self.create_basic_lineage_graph()
        self._calculate_transformation_order()
        self._extract_transformation_logic()
        self.process_mapping()

    def process_mapping(self):
        for connector in self.connectors:
            connector_info = self._process_connector_with_lineage(connector)
            if connector_info:
                self.connectors_data.append(connector_info)
        return self.connectors_data

    def _extract_mappings(self):
        """Extract all mappings from the workflow data"""
        mappings = []
        
        try:
            # Based on the structure: POWERMART > REPOSITORY > FOLDER > MAPPING
            powermart = self.workflow_data.get('POWERMART', {})
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
    
    def check_if_mapping_exists(self):
        mapping_name = self.mapping_name
        for i in range(len(self._extract_mappings())):
            mapping=self._extract_mappings()[i]
            if mapping_name == mapping.get('@NAME'):
                return True,mapping,i
        return False, None, None
    
    def fetch_all_connectors(self):
        for connector in self.mapping_data['CONNECTOR']:
            self.connectors.append(connector)
        return self.connectors
    
    def fetch_all_transformations(self):
        for transformation in self.mapping_data.get('TRANSFORMATION', []):
            self.transformations.append(transformation)
        return self.transformations
    
    def create_transform_type_acronym(self, transform_type: str):
        # 'Filter', 'Target Definition', 'Source Definition', 'App Multi-Group Source Qualifier', 'Source Qualifier', 'XML Source Qualifier', 'Custom Transformation', 'Aggregator', 'Update Strategy', 'Expression', 'Lookup Procedure', 'Sequence'
        if transform_type == 'Filter':
            return 'FILT_'
        elif transform_type == 'Target Definition':
            return 'TGT_'
        elif transform_type == 'Source Definition':
            return 'SRC_'
        elif transform_type == 'App Multi-Group Source Qualifier':  
            return 'SRCQ_'
        elif transform_type == 'Source Qualifier':
            return 'SRCQ_'
        elif transform_type == 'XML Source Qualifier':
            return 'XSRCQ_'
        elif transform_type == 'Custom Transformation': 
            return 'CTR_'
        elif transform_type == 'Aggregator':
            return 'AGGR_'
        elif transform_type == 'Update Strategy':
            return 'UPD_'
        elif transform_type == 'Expression':
            return 'EXPR_'
        elif transform_type == 'Lookup Procedure':
            return 'LOOK_'
        elif transform_type == 'Sequence':
            return 'SEQ_'
        else:
            print(f"Unknown transformation type: {transform_type}")
            return transform_type

    def fetch_all_instances(self):
        for instance in self.mapping_data.get('INSTANCE', []):
            self.instances.append(instance)
        return self.instances
    
    def create_basic_lineage_graph(self):
        G = nx.DiGraph()
        for instance in self.instances:
            G.add_node(f"{self.create_transform_type_acronym(instance['@TRANSFORMATION_TYPE'])}{instance['@NAME']}")

        for connector in self.connectors:
            # print(connector)
            # print(connector.keys())
            G.add_edge(f"{self.create_transform_type_acronym(connector['@FROMINSTANCETYPE'])}{connector['@FROMINSTANCE']}", f"{self.create_transform_type_acronym(connector['@TOINSTANCETYPE'])}{connector['@TOINSTANCE']}")
   
        self.mapping_lineage_graph = G
        return self.mapping_lineage_graph

    def _calculate_transformation_order(self):
        """
        Enhanced transformation order calculation that handles complex scenarios:
        - Multiple sources and targets
        - Parallel execution paths
        - Convergence points (joins/unions)
        - Diamond patterns (split and rejoin)
        """
        G = self.mapping_lineage_graph
        mapping_name = self.mapping_name
        
        # Calculate transformation order
        transformation_orders = {}
        parallel_groups = {}  # Track which transformations can run in parallel
        
        try:
            # Handle disconnected components
            connected_components = list(nx.weakly_connected_components(G))
            
            for component_idx, component in enumerate(connected_components):
                subgraph = G.subgraph(component)
                
                if len(component) == 1:
                    # Single node with no connections
                    node = list(component)[0]
                    transformation_orders[node] = 0
                    parallel_groups[node] = f"isolated_{component_idx}"
                    continue
                
                # Enhanced approach for complex graphs
                component_orders, component_parallel_groups = self._calculate_complex_order(subgraph, component_idx)
                transformation_orders.update(component_orders)
                parallel_groups.update(component_parallel_groups)
        
        except Exception as e:
            print(f"  Error calculating transformation order: {e}")
            # Fallback: assign sequential numbers
            for i, instance in enumerate(self.instances):
                node_name = f"{self.create_transform_type_acronym(instance['@TRANSFORMATION_TYPE'])}{instance['@NAME']}"
                transformation_orders[node_name] = i + 1
                parallel_groups[node_name] = f"fallback_{i}"
        
        # Handle transformations not in connectors (unconnected)
        if mapping_name in self.transformation_logic_cache:
            for trans_name in self.transformation_logic_cache[mapping_name]:
                if trans_name not in transformation_orders:
                    transformation_orders[trans_name] = 0
                    parallel_groups[trans_name] = "unconnected"
        
        print(f"  Transformation orders: {transformation_orders}")
        print(f"  Parallel groups: {parallel_groups}")
        
        self.transformation_orders = transformation_orders
        self.parallel_groups = parallel_groups
        return self.transformation_orders
    
    def _calculate_complex_order(self, subgraph, component_idx):
        """
        Calculate transformation order for complex graphs with multiple patterns
        """
        orders = {}
        parallel_groups = {}
        
        # Step 1: Identify different node types
        sources = [n for n in subgraph.nodes() if subgraph.in_degree(n) == 0]
        sinks = [n for n in subgraph.nodes() if subgraph.out_degree(n) == 0]
        intermediate = [n for n in subgraph.nodes() if subgraph.in_degree(n) > 0 and subgraph.out_degree(n) > 0]
        
        # Handle case with no sources (cycles)
        if not sources:
            min_in_degree = min(subgraph.in_degree(n) for n in subgraph.nodes())
            sources = [n for n in subgraph.nodes() if subgraph.in_degree(n) == min_in_degree]
        
        print(f"    Component {component_idx}: Sources={len(sources)}, Sinks={len(sinks)}, Intermediate={len(intermediate)}")
        
        # Step 2: Calculate levels using modified topological sort
        try:
            # Use longest path algorithm to handle convergence properly
            levels = self._calculate_longest_path_levels(subgraph, sources)
            
            # Step 3: Identify parallel execution groups
            level_groups = self._identify_parallel_groups(subgraph, levels)
            
            # Step 4: Assign final orders and parallel group IDs
            for node, level in levels.items():
                orders[node] = level + 1  # Convert to 1-based
                # Find which parallel group this node belongs to
                for group_id, nodes_in_group in level_groups.items():
                    if node in nodes_in_group:
                        parallel_groups[node] = f"comp_{component_idx}_level_{level}_group_{group_id}"
                        break
                else:
                    parallel_groups[node] = f"comp_{component_idx}_level_{level}_single"
            
        except Exception as e:
            print(f"    Error in complex order calculation: {e}")
            # Fallback to simple BFS
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
                        levels[successor] = max(levels[successor], current_level + 1)
            
            for node, level in levels.items():
                orders[node] = level + 1
                parallel_groups[node] = f"comp_{component_idx}_level_{level}"
        
        return orders, parallel_groups
    
    def _calculate_longest_path_levels(self, subgraph, sources):
        """
        Calculate levels using longest path algorithm to handle convergence points properly
        """
        # Initialize levels
        levels = {}
        
        # Topologically sort the nodes
        try:
            topo_order = list(nx.topological_sort(subgraph))
        except nx.NetworkXError:
            # Handle cycles by using a modified approach
            topo_order = self._handle_cyclic_graph(subgraph, sources)
        
        # Calculate longest path to each node
        for node in topo_order:
            if node in sources:
                levels[node] = 0
            else:
                # Find the maximum level among all predecessors
                predecessor_levels = [levels.get(pred, 0) for pred in subgraph.predecessors(node)]
                levels[node] = max(predecessor_levels) + 1 if predecessor_levels else 0
        
        return levels
    
    def _handle_cyclic_graph(self, subgraph, sources):
        """
        Handle graphs with cycles by breaking them intelligently
        """
        # Create a copy and remove problematic edges to break cycles
        temp_graph = subgraph.copy()
        
        # Try to remove back edges to break cycles
        try:
            # Find strongly connected components
            sccs = list(nx.strongly_connected_components(temp_graph))
            
            # For each SCC with more than one node, remove some edges
            for scc in sccs:
                if len(scc) > 1:
                    scc_subgraph = temp_graph.subgraph(scc)
                    # Remove the edge with the highest out-degree node to lowest in-degree node
                    max_out_node = max(scc, key=lambda n: scc_subgraph.out_degree(n))
                    min_in_node = min(scc, key=lambda n: scc_subgraph.in_degree(n))
                    if temp_graph.has_edge(max_out_node, min_in_node):
                        temp_graph.remove_edge(max_out_node, min_in_node)
            
            return list(nx.topological_sort(temp_graph))
        except:
            # Last resort: just use the original node order
            return list(subgraph.nodes())
    
    def _identify_parallel_groups(self, subgraph, levels):
        """
        Identify which transformations can run in parallel (same level, independent paths)
        """
        # Group nodes by level
        level_nodes = {}
        for node, level in levels.items():
            if level not in level_nodes:
                level_nodes[level] = []
            level_nodes[level].append(node)
        
        # For each level, identify independent parallel groups
        parallel_groups = {}
        group_counter = 0
        
        for level, nodes in level_nodes.items():
            if len(nodes) == 1:
                parallel_groups[group_counter] = nodes
                group_counter += 1
            else:
                # Check for dependencies within the level
                remaining_nodes = set(nodes)
                
                while remaining_nodes:
                    # Start a new group
                    current_group = []
                    nodes_to_remove = set()
                    
                    for node in remaining_nodes:
                        # Check if this node can be added to current group
                        can_add = True
                        for group_node in current_group:
                            # Check if there's any path between node and group_node
                            if (nx.has_path(subgraph, node, group_node) or 
                                nx.has_path(subgraph, group_node, node)):
                                can_add = False
                                break
                        
                        if can_add:
                            current_group.append(node)
                            nodes_to_remove.add(node)
                    
                    if current_group:
                        parallel_groups[group_counter] = current_group
                        group_counter += 1
                        remaining_nodes -= nodes_to_remove
                    else:
                        # Fallback: add remaining nodes individually
                        for node in remaining_nodes:
                            parallel_groups[group_counter] = [node]
                            group_counter += 1
                        break
        
        return parallel_groups

    def _extract_transformation_logic(self):
        """Extract transformation logic for all transformations in the mapping"""
        mapping_data=self.mapping_data
        mapping_name=self.mapping_name
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

        elif trans_type.upper() in ['SOURCE QUALIFIER']:
            # Extract source qualifier logic
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
    
    def _process_connector_with_lineage(self,connector):
        """Process a single connector with lineage information"""
        mapping_number=self.mapping_number
        mapping_name=self.mapping_name
        transformation_orders=self.transformation_orders
        parallel_groups=getattr(self, 'parallel_groups', {})

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
            
            # Create transformation node names
            to_node_name = f"{self.create_transform_type_acronym(to_instance_type)}{to_instance}"
            from_node_name = f"{self.create_transform_type_acronym(from_instance_type)}{from_instance}"
            
            # Get transformation order (use TO instance order as it represents where data flows to)
            trans_order = transformation_orders.get(to_node_name, 0)
            
            # Get parallel group information
            parallel_group = parallel_groups.get(to_node_name, "unknown")
            
            # Calculate execution level (0-based level from the transformation order)
            execution_level = max(0, trans_order - 1) if trans_order > 0 else 0
            
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
                transformation_logic=trans_logic,
                parallel_group=parallel_group,
                execution_level=execution_level
            )
            
        except Exception as e:
            print(f"Error processing connector: {e}")
            return None
    
def save_to_csv(connectors_data, output_csv_path: str):
    """Save connector data to CSV file with lineage information"""
    try:
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'mapping_number', 'mapping_name', 'FROMFIELD', 'FROMINSTANCE', 
                'FROMINSTANCETYPE', 'TOFIELD', 'TOINSTANCE', 'TOINSTANCETYPE',
                'transformation_order', 'execution_level', 'parallel_group', 'transformation_logic'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write data
            for connector in connectors_data:
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
                    'execution_level': connector.execution_level,
                    'parallel_group': connector.parallel_group,
                    'transformation_logic': connector.transformation_logic
                })
        
        print(f"CSV file saved successfully: {output_csv_path}")
        print(f"Total records written: {len(connectors_data)}")
        
    except Exception as e:
        print(f"Error saving CSV file: {e}") 
    


if __name__ == "__main__":
    import os
    import xmltodict

    xml_files_path = "/Users/sathishkumar/AllFiles/miami/XMLify/dydy"

    #  function to read the xml file and return the json
    def read_xml_file(xml_file_path):
        with open(xml_file_path, "r") as f:
            xml_data = f.read()
        # convert the xml data to json
        json_data = xmltodict.parse(xml_data)
        return json_data

    def process_xml_file(xml_file):
        xml_file_path = os.path.join(xml_files_path, xml_file)
        json_data = read_xml_file(xml_file_path)
        return(json_data)

    wf_pld_kyc2_udm_stg_load_json = process_xml_file('wf_PLD_KYC2_UDM_STG_LOAD.XML')

    p1=InfaLineageGenerator('m_UDM_STG_LOAD_CYCLE_END', wf_pld_kyc2_udm_stg_load_json)
