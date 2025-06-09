import json
import pandas as pd
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
import re

@dataclass
class TransformationField:
    """Represents a field in a transformation"""
    name: str
    datatype: str = ""
    precision: str = ""
    scale: str = ""
    expression: str = ""
    description: str = ""
    port_type: str = ""

@dataclass
class Transformation:
    """Represents a transformation in the mapping"""
    name: str
    type: str
    description: str = ""
    fields: List[TransformationField] = field(default_factory=list)
    attributes: Dict[str, str] = field(default_factory=dict)
    
@dataclass
class Connection:
    """Represents a connection between transformations"""
    from_instance: str
    from_field: str
    to_instance: str
    to_field: str
    from_type: str = ""
    to_type: str = ""

@dataclass
class LineageRecord:
    """Represents a single lineage record for output"""
    target_table: str
    target_column: str
    target_datatype: str
    source_table: str
    source_column: str
    source_datatype: str
    transformation_path: str
    transformation_logic: str
    transformation_count: int
    direct_source: bool

class FixedInformaticaLineageFramework:
    """Fixed version of the Informatica lineage framework"""
    
    def __init__(self):
        self.transformations: Dict[str, Transformation] = {}
        self.connections: List[Connection] = []
        self.instances: Dict[str, Dict] = {}
        self.sources: List[str] = []
        self.targets: List[str] = []
        self.target_load_order: Dict[str, int] = {}
        
    def parse_mapping(self, json_data: Dict) -> None:
        """Parse the Informatica mapping JSON"""
        
        # Parse transformations
        if "TRANSFORMATION" in json_data:
            for trans_data in json_data["TRANSFORMATION"]:
                trans = self._parse_transformation(trans_data)
                self.transformations[trans.name] = trans
        
        # Parse instances
        if "INSTANCE" in json_data:
            for instance in json_data["INSTANCE"]:
                self.instances[instance["@NAME"]] = instance
                if instance["@TYPE"] == "SOURCE":
                    self.sources.append(instance["@NAME"])
                elif instance["@TYPE"] == "TARGET":
                    self.targets.append(instance["@NAME"])
        
        # Parse connections
        if "CONNECTOR" in json_data:
            for conn_data in json_data["CONNECTOR"]:
                conn = Connection(
                    from_instance=conn_data["@FROMINSTANCE"],
                    from_field=conn_data["@FROMFIELD"],
                    to_instance=conn_data["@TOINSTANCE"],
                    to_field=conn_data["@TOFIELD"],
                    from_type=conn_data.get("@FROMINSTANCETYPE", ""),
                    to_type=conn_data.get("@TOINSTANCETYPE", "")
                )
                self.connections.append(conn)
        
        # Parse target load order
        if "TARGETLOADORDER" in json_data:
            try:
                for target_order in json_data["TARGETLOADORDER"]:
                    self.target_load_order[target_order["@TARGETINSTANCE"]] = int(target_order["@ORDER"])
            except:
                # self.target_load_order[json_data["@TARGETINSTANCE"]] = int(target_order["@ORDER"])
                # print(json_data["@TARGETINSTANCE"])
                pass
    
    def _parse_transformation(self, trans_data: Dict) -> Transformation:
        """Parse individual transformation"""
        trans = Transformation(
            name=trans_data["@NAME"],
            type=trans_data["@TYPE"],
            description=trans_data.get("@DESCRIPTION", "")
        )
        
        # Parse fields
        if "TRANSFORMFIELD" in trans_data:
            fields_data = trans_data["TRANSFORMFIELD"]
            if isinstance(fields_data, dict):
                fields_data = [fields_data]
            
            for field_data in fields_data:
                field = TransformationField(
                    name=field_data["@NAME"],
                    datatype=field_data.get("@DATATYPE", ""),
                    precision=field_data.get("@PRECISION", ""),
                    scale=field_data.get("@SCALE", ""),
                    expression=field_data.get("@EXPRESSION", ""),
                    description=field_data.get("@DESCRIPTION", ""),
                    port_type=field_data.get("@PORTTYPE", "")
                )
                trans.fields.append(field)
        
        # Parse table attributes (for lookups, etc.)
        if "TABLEATTRIBUTE" in trans_data:
            attrs = trans_data["TABLEATTRIBUTE"]
            if isinstance(attrs, dict):
                attrs = [attrs]
            
            for attr in attrs:
                trans.attributes[attr["@NAME"]] = attr["@VALUE"]
        
        return trans
    
    def build_reverse_graph(self) -> Dict[str, Dict[str, List[Connection]]]:
        """Build reverse graph for backward traversal"""
        reverse_graph = defaultdict(lambda: defaultdict(list))
        
        for conn in self.connections:
            # Reverse connections (to <- from)
            reverse_graph[conn.to_instance][conn.to_field].append(conn)
        
        # print("-------- reverse_graph --------")
        # print(reverse_graph.items())
        
        return reverse_graph
    
    def is_lineage_source(self, instance_name: str) -> bool:
        """Determine if an instance is a lineage source (end point)"""
        
        # Check if it's a declared source
        if instance_name in self.sources:
            print(f"      Source identified: {instance_name} (declared source)")
            return True
            
        # Check if it's a source qualifier (common pattern)
        if instance_name.startswith("SQ_"):
            print(f"      Source identified: {instance_name} (source qualifier)")
            return True
            
        # Check if it's a sequence generator
        if "SEQ_" in instance_name or "Sequence" in instance_name:
            print(f"      Source identified: {instance_name} (sequence generator)")
            return True
            
        # Check transformation type
        if instance_name in self.instances:
            trans_type = self.instances[instance_name].get("@TRANSFORMATION_TYPE", "")
            if any(src_type in trans_type for src_type in ["Source", "Sequence"]):
                print(f"      Source identified: {instance_name} (source-like transformation)")
                return True
        
        # Check if it's a transformation marked as source-like
        if instance_name in self.transformations:
            trans = self.transformations[instance_name]
            if trans.type in ["Source Qualifier", "Sequence Generator"]:
                print(f"      Source identified: {instance_name} (source-like transformation type)")
                return True
                
            # Handle Expression transformations that might be sources
            if trans.type == "Expression":
                # Check if this expression has any incoming connections
                has_incoming = False
                for conn in self.connections:
                    if conn.to_instance == instance_name:
                        has_incoming = True
                        break
                if not has_incoming:
                    print(f"      Source identified: {instance_name} (expression with no incoming connections)")
                    return True
        
        return False
    
    def get_transformation_logic(self, trans_name: str, field_name: str = "") -> str:
        """Extract transformation logic for a given transformation and field"""
        if trans_name not in self.transformations:
            return f"Type: Unknown ({trans_name})"
        
        trans = self.transformations[trans_name]
        logic_parts = [f"Type: {trans.type}"]
        
        # For expressions, get the specific field expression
        if trans.type == "Expression" and field_name:
            for field in trans.fields:
                if field.name == field_name and field.expression:
                    # Truncate long expressions
                    expr = field.expression
                    if len(expr) > 100:
                        expr = expr[:97] + "..."
                    logic_parts.append(f"Expression: {expr}")
                    break
        
        # For lookups, get lookup conditions and table info
        elif trans.type == "Lookup Procedure":
            if "Lookup table name" in trans.attributes:
                logic_parts.append(f"Table: {trans.attributes['Lookup table name']}")
            if "Lookup condition" in trans.attributes:
                condition = trans.attributes['Lookup condition']
                if len(condition) > 100:
                    condition = condition[:97] + "..."
                logic_parts.append(f"Condition: {condition}")
        
        # For filters, get filter condition
        elif trans.type == "Filter":
            if "Filter Condition" in trans.attributes:
                condition = trans.attributes['Filter Condition']
                if len(condition) > 100:
                    condition = condition[:97] + "..."
                logic_parts.append(f"Condition: {condition}")
        
        return " | ".join(logic_parts)
    
    def _get_field_datatype(self, instance_name: str, field_name: str) -> str:
        """Get datatype for a field in an instance"""
        if instance_name in self.transformations:
            trans = self.transformations[instance_name]
            for field in trans.fields:
                if field.name == field_name:
                    datatype = field.datatype
                    if field.precision:
                        datatype += f"({field.precision}"
                        if field.scale and field.scale != "0":
                            datatype += f",{field.scale}"
                        datatype += ")"
                    return datatype
        return "Unknown"
    
    def trace_lineage(self, target_instance: str, target_field: str, max_depth: int = 20) -> List[LineageRecord]:
        """Trace lineage for a specific target field with depth limit"""
        reverse_graph = self.build_reverse_graph()
        lineage_records = []
        
        # Use BFS to traverse backward with depth tracking
        queue = deque([(target_instance, target_field, [], [], 0)])  # Added depth
        visited = set()
        
        print(f"    Tracing: {target_instance}.{target_field}")
        
        while queue:
            current_instance, current_field, path, logic_path, depth = queue.popleft()
            
            # Depth limit to prevent infinite loops
            if depth > max_depth:
                print(f"      Max depth reached at {current_instance}.{current_field}")
                continue
            
            # Create unique key for visited tracking (include depth to allow revisiting at different levels)
            visit_key = f"{current_instance}.{current_field}.{depth}"
            if visit_key in visited:
                continue
            visited.add(visit_key)
            
            # Check if this is a lineage source
            if self.is_lineage_source(current_instance):
                # Found a source, create lineage record
                source_table = current_instance.replace("Shortcut_to_", "").replace("SQ_", "")
                
                record = LineageRecord(
                    target_table=target_instance.replace("Shortcut_to_", ""),
                    target_column=target_field,
                    target_datatype=self._get_field_datatype(target_instance, target_field),
                    source_table=source_table,
                    source_column=current_field,
                    source_datatype=self._get_field_datatype(current_instance, current_field),
                    transformation_path=" -> ".join(reversed(path + [current_instance])),
                    transformation_logic=" | ".join(reversed(logic_path)),
                    transformation_count=len(path),
                    direct_source=len(path) == 0
                )
                lineage_records.append(record)
                print(f"      ✓ Found: {source_table}.{current_field}")
                continue
            
            # Get incoming connections for this field
            connections_found = 0
            
            # First check direct field connections
            if current_instance in reverse_graph and current_field in reverse_graph[current_instance]:
                for conn in reverse_graph[current_instance][current_field]:
                    connections_found += 1
                    print(f"      Following direct connection: {conn.from_instance}.{conn.from_field} -> {current_instance}.{current_field}")
                    new_path = path + [current_instance]
                    logic = self.get_transformation_logic(current_instance, current_field)
                    new_logic_path = logic_path + [logic] if logic else logic_path
                    
                    queue.append((
                        conn.from_instance,
                        conn.from_field,
                        new_path,
                        new_logic_path,
                        depth + 1
                    ))
            
            # Then check for expression transformations that might create this field
            if current_instance in self.transformations:
                trans = self.transformations[current_instance]
                if trans.type == "Expression":
                    # Check all fields in this transformation for expressions that might create our field
                    for field in trans.fields:
                        if field.expression:
                            # Look for field references in the expression
                            # This is a simple check - you might want to make this more sophisticated
                            if current_field in field.expression:
                                print(f"      Found field reference in expression: {field.name} = {field.expression}")
                                # Add the field that contains the expression as a potential source
                                new_path = path + [current_instance]
                                logic = f"Expression: {field.expression}"
                                new_logic_path = logic_path + [logic]
                                
                                queue.append((
                                    current_instance,
                                    field.name,
                                    new_path,
                                    new_logic_path,
                                    depth + 1
                                ))
                                connections_found += 1
            
            # Finally check for any connections to this instance that might be transformed into our field
            for conn in self.connections:
                if conn.to_instance == current_instance:
                    # Check if this connection might be transformed into our field
                    if current_instance in self.transformations:
                        trans = self.transformations[current_instance]
                        if trans.type == "Expression":
                            for field in trans.fields:
                                if field.name == current_field and field.expression:
                                    if conn.from_field in field.expression:
                                        print(f"      Found field transformation: {conn.from_instance}.{conn.from_field} -> {current_instance}.{current_field} via expression")
                                        new_path = path + [current_instance]
                                        logic = f"Expression: {field.expression}"
                                        new_logic_path = logic_path + [logic]
                                        
                                        queue.append((
                                            conn.from_instance,
                                            conn.from_field,
                                            new_path,
                                            new_logic_path,
                                            depth + 1
                                        ))
                                        connections_found += 1
            
            if connections_found == 0:
                print(f"      Dead end: {current_instance}.{current_field} (no incoming)")
                # Print all connections for this instance to help debug
                print(f"      Available connections for {current_instance}:")
                for conn in self.connections:
                    if conn.to_instance == current_instance:
                        # print(f"        {conn.from_instance}.{conn.from_field} -> {conn.to_instance}.{conn.to_field}")
                        pass
                
                # Print transformation details if available
                if current_instance in self.transformations:
                    trans = self.transformations[current_instance]
                    print(f"      Transformation details for {current_instance}:")
                    print(f"        Type: {trans.type}")
                    if trans.type == "Expression":
                        print("        Fields and expressions:")
                        for field in trans.fields:
                            if field.expression:
                                print(f"          {field.name} = {field.expression}")
        
        return lineage_records
    
    def get_target_fields(self, target_instance: str) -> List[str]:
        """Get all fields for a target instance"""
        target_fields = set()
        for conn in self.connections:
            if conn.to_instance == target_instance:
                target_fields.add(conn.to_field)
        return sorted(list(target_fields))
    
    def generate_complete_lineage(self, max_fields_per_target: int = 10) -> List[LineageRecord]:
        """Generate complete lineage for all targets (limited for debugging)"""
        all_lineage = []
        
        print(f"Processing {len(self.targets)} targets...")
        
        # Debug info
        print(f"\nSources identified: {len(self.sources)}")
        for source in self.sources:
            print(f"  - {source}")
        
        print(f"\nSample connections (first 10):")
        for i, conn in enumerate(self.connections[:10]):
            print(f"  {conn.from_instance}.{conn.from_field} -> {conn.to_instance}.{conn.to_field}")
        if len(self.connections) > 10:
            print(f"  ... and {len(self.connections) - 10} more")
        
        # Check reverse graph
        reverse_graph = self.build_reverse_graph()
        print(f"\nReverse graph has {len(reverse_graph)} instances")
        
        for target in self.targets:
            print(f"\n{'='*60}")
            print(f"Processing target: {target}")
            target_fields = self.get_target_fields(target)
            print(f"  Found {len(target_fields)} fields: {target_fields[:10]}...")
            
            # Limit fields for debugging
            fields_to_process = target_fields[:max_fields_per_target]
            
            for field in fields_to_process:
                print(f"\n  Processing field: {field}")
                lineage_records = self.trace_lineage(target, field)
                if lineage_records:
                    print(f"    Generated {len(lineage_records)} lineage record(s)")
                else:
                    print(f"    No lineage found for {field}")
                all_lineage.extend(lineage_records)
        
        return all_lineage
    
    def export_to_csv(self, lineage_records: List[LineageRecord], filename: str) -> None:
        """Export lineage records to CSV"""
        if not lineage_records:
            print(f"No lineage records to export!")
            return
        
        data = []
        for record in lineage_records:
            data.append({
                'Target_Table': record.target_table,
                'Target_Column': record.target_column,
                'Target_DataType': record.target_datatype,
                'Source_Table': record.source_table,
                'Source_Column': record.source_column,
                'Source_DataType': record.source_datatype,
                'Transformation_Path': record.transformation_path,
                'Transformation_Logic': record.transformation_logic,
                'Transformation_Count': record.transformation_count,
                'Direct_Source': record.direct_source
            })
        
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        print(f"Lineage exported to {filename}")
        print(f"Total records: {len(data)}")
    
    def print_summary(self) -> None:
        """Print mapping summary"""
        print("=== FIXED INFORMATICA MAPPING SUMMARY ===")
        print(f"Sources: {len(self.sources)}")
        for source in self.sources:
            print(f"  - {source}")
        
        print(f"\nTargets: {len(self.targets)}")
        for target in self.targets:
            order = self.target_load_order.get(target, "Unknown")
            print(f"  - {target} (Load Order: {order})")
        
        print(f"\nTransformations: {len(self.transformations)}")
        trans_types = defaultdict(int)
        for trans in self.transformations.values():
            trans_types[trans.type] += 1
        
        for trans_type, count in sorted(trans_types.items()):
            print(f"  - {trans_type}: {count}")
        
        print(f"\nConnections: {len(self.connections)}")

# Main execution function
def process_informatica_mapping_fixed(json_file_path: str, output_csv_path: str = "fixed_lineage_output.csv"):
    """Process Informatica mapping with fixed framework"""
    
    # Load JSON data
    with open(json_file_path, 'r') as f:
        json_data = json.load(f)
    
    # Initialize framework
    framework = FixedInformaticaLineageFramework()
    
    # Parse the mapping
    framework.parse_mapping(json_data)
    
    # Print summary
    framework.print_summary()
    
    # Generate complete lineage (limited for debugging)
    print("\n=== GENERATING LINEAGE (FIXED VERSION) ===")
    lineage_records = framework.generate_complete_lineage(max_fields_per_target=5)
    
    # Export to CSV
    framework.export_to_csv(lineage_records, output_csv_path)
    
    return framework, lineage_records

# Example usage for debugging
if __name__ == "__main__":
    # If you have the JSON data as a variable
    json_data = {
        # Your JSON data here
    }
    
    framework = FixedInformaticaLineageFramework()
    framework.parse_mapping(json_data)
    framework.print_summary()
    
    # lineage_records = framework.generate_complete_lineage(max_fields_per_target=3)
    lineage_records = framework.generate_complete_lineage()
    framework.export_to_csv(lineage_records, "fixed_informatica_lineage.csv")