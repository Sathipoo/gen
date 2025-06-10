import json
import csv
import pandas as pd
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

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

class InformaticaMappingExtractor:
    def __init__(self):
        self.connectors_data = []
    
    def extract_mappings_to_csv(self, json_file_path: str, output_csv_path: str = None) -> List[ConnectorInfo]:
        """
        Extract all mappings and their connectors from Informatica workflow JSON
        and optionally save to CSV
        
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
        """Process the workflow data and extract connector information"""
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
            
            # Extract connectors from this mapping
            connectors = self._extract_connectors_from_mapping(mapping_data)
            
            print(f"  Found {len(connectors)} connector(s)")
            
            # Process each connector
            for connector in connectors:
                connector_info = self._process_connector(mapping_number, mapping_name, connector)
                if connector_info:
                    self.connectors_data.append(connector_info)
        
        # Save to CSV if path provided
        if output_csv_path:
            self._save_to_csv(output_csv_path)
        
        return self.connectors_data
    
    def _get_workflow_name(self, workflow_data: Dict[str, Any]) -> str:
        """Extract workflow name from workflow data"""
        try:
            workflow_name = workflow_data['POWERMART']['REPOSITORY']['FOLDER'][1]['WORKFLOW']['@NAME']
        except:
            workflow_name = workflow_data['POWERMART']['REPOSITORY']['FOLDER'][0]['WORKFLOW']['@NAME']
        return workflow_name
        
    
    def _extract_mappings(self, workflow_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract all mappings from the workflow data"""
        mappings = []
        
        try:
            # Based on the structure: POWERMART > REPOSITORY > FOLDER > MAPPING
            workflow_name = self._get_workflow_name(workflow_data)
            self.workflow_name = workflow_name
            print("="*100)
            print(workflow_name)
            print("="*100)
            powermart = workflow_data.get('POWERMART', {})
            repository = powermart.get('REPOSITORY', {})
            folder = repository.get('FOLDER', {})
            try:
                mapping_data = folder[0]['MAPPING']
            except:
                mapping_data = folder[1]['MAPPING']

            # mapping_data = workflow_data['POWERMART']['REPOSITORY']['FOLDER'][1]['MAPPING']
            
            # Handle both single mapping (dict) and multiple mappings (list)
            if isinstance(mapping_data, dict):
                mappings = [mapping_data]
            elif isinstance(mapping_data, list):
                mappings = mapping_data
            else:
                print("No MAPPING structure found in the expected location")
                
        except Exception as e:
            print(f"Error extracting mappings: {e}")
            
        return mappings
    
    def _get_mapping_name(self, mapping_data: Dict[str, Any]) -> str:
        """Extract mapping name from mapping data"""
        # Try different possible name fields
        name_fields = ['@NAME', 'NAME', '@name', 'name']
        for field in name_fields:
            if field in mapping_data:
                return str(mapping_data[field])
        return "Unknown Mapping"
    
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
    
    def _process_connector(self, mapping_number: int, mapping_name: str, 
                          connector: Dict[str, Any]) -> Optional[ConnectorInfo]:
        """Process a single connector and extract required information"""
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
            
            return ConnectorInfo(
                mapping_number=mapping_number,
                mapping_name=mapping_name,
                from_field=from_field,
                from_instance=from_instance,
                from_instance_type=from_instance_type,
                to_field=to_field,
                to_instance=to_instance,
                to_instance_type=to_instance_type
            )
            
        except Exception as e:
            print(f"Error processing connector: {e}")
            return None
    
    def _save_to_csv(self, output_csv_path: str):
        """Save connector data to CSV file"""
        try:
            with open(output_csv_path, 'a', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'workflow_name', 'mapping_number', 'mapping_name', 'FROMFIELD', 'FROMINSTANCE', 
                    'FROMINSTANCETYPE', 'TOFIELD', 'TOINSTANCE', 'TOINSTANCETYPE'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write data
                for connector in self.connectors_data:
                    writer.writerow({
                        'workflow_name': self.workflow_name,
                        'mapping_number': connector.mapping_number,
                        'mapping_name': connector.mapping_name,
                        'FROMFIELD': connector.from_field,
                        'FROMINSTANCE': connector.from_instance,
                        'FROMINSTANCETYPE': connector.from_instance_type,
                        'TOFIELD': connector.to_field,
                        'TOINSTANCE': connector.to_instance,
                        'TOINSTANCETYPE': connector.to_instance_type
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
            'TOINSTANCETYPE': [c.to_instance_type for c in self.connectors_data]
        }
        
        return pd.DataFrame(data_dict)
    
    def print_summary(self):
        """Print a summary of extracted data"""
        if not self.connectors_data:
            print("No connector data found")
            return
        
        print(f"\n=== EXTRACTION SUMMARY ===")
        print(f"Total connectors extracted: {len(self.connectors_data)}")
        
        # Group by mapping
        mappings = {}
        for connector in self.connectors_data:
            if connector.mapping_name not in mappings:
                mappings[connector.mapping_name] = 0
            mappings[connector.mapping_name] += 1
        
        print(f"Mappings processed: {len(mappings)}")
        for mapping_name, count in mappings.items():
            print(f"  - {mapping_name}: {count} connectors")
        
        # Show sample data
        if len(self.connectors_data) > 0:
            print(f"\n=== SAMPLE DATA (First 3 records) ===")
            for i, connector in enumerate(self.connectors_data[:3]):
                print(f"\nRecord {i+1}:")
                print(f"  Mapping: {connector.mapping_name}")
                print(f"  From: {connector.from_instance}.{connector.from_field} ({connector.from_instance_type})")
                print(f"  To: {connector.to_instance}.{connector.to_field} ({connector.to_instance_type})")
    
    def filter_by_mapping(self, mapping_name: str) -> List[ConnectorInfo]:
        """Filter connectors by mapping name"""
        return [c for c in self.connectors_data if mapping_name.lower() in c.mapping_name.lower()]
    
    def filter_by_instance_type(self, instance_type: str) -> List[ConnectorInfo]:
        """Filter connectors by instance type (from or to)"""
        return [c for c in self.connectors_data 
                if instance_type.lower() in c.from_instance_type.lower() or 
                   instance_type.lower() in c.to_instance_type.lower()]



  

if __name__ == "__main__":
    extractor = InformaticaMappingExtractor()
   
    # Extract data
    connectors = extractor.extract_mappings_from_dict(sample_workflow, 'output.csv')
    
    # Print summary
    extractor.print_summary()
    
    # Get as DataFrame
    df = extractor.get_dataframe()
    print(f"\nDataFrame shape: {df.shape}")
    print(df.head())
    
    