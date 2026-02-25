import csv
import os
from typing import List, Dict, Optional

class SnowflakeDDLGenerator:
    def __init__(self, csv_file_path: str):
        self.csv_file_path = csv_file_path
        self.table_data = self._load_csv()

    def _load_csv(self) -> Dict[str, List[Dict]]:
        """Reads the CSV and groups column definitions by table name."""
        tables = {}
        if not os.path.exists(self.csv_file_path):
            print(f"Warning: CSV file not found at {self.csv_file_path}")
            return tables

        try:
            with open(self.csv_file_path, mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    table_name = row['TABLE_NAME'].upper()
                    if table_name not in tables:
                        tables[table_name] = []
                    tables[table_name].append(row)
        except Exception as e:
            print(f"Error reading CSV: {e}")
        
        return tables

    def _map_data_type(self, oracle_type: str, length: str, precision: str, scale: str) -> str:
        """Maps Oracle data types to Snowflake equivalents."""
        oracle_type = oracle_type.upper()
        
        # Handle TIMESTAMP(6) etc.
        if "TIMESTAMP" in oracle_type:
            # Oracle TIMESTAMP(6) often has scale 6
            s = scale if scale else "6"
            return f"TIMESTAMP_NTZ({s})"
        
        if oracle_type == "DATE":
            return "TIMESTAMP_NTZ"
        
        if oracle_type in ["NUMBER", "DECIMAL", "NUMERIC"]:
            if precision and precision != "None":
                if scale and scale != "None":
                    return f"NUMBER({precision}, {scale})"
                return f"NUMBER({precision})"
            return "NUMBER"
        
        if oracle_type in ["VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "VARCHAR"]:
            if length and length != "None":
                return f"VARCHAR({length})"
            return "VARCHAR"
        
        # Default fallback
        return "VARCHAR"

    def generate_ddl(self, table_names: List[str], schema_name: Optional[str] = None) -> str:
        """Generates DDL for the specified tables."""
        ddls = []
        
        full_schema = f"{schema_name}." if schema_name else ""
        
        for table_name in table_names:
            table_name = table_name.upper()
            if table_name not in self.table_data:
                ddls.append(f"-- Warning: Table {table_name} not found in CSV data.\n")
                continue
            
            columns = self.table_data[table_name]
            col_definitions = []
            
            for col in columns:
                col_name = col['COLUMN_NAME']
                data_type = col['DATA_TYPE']
                length = col.get('DATA_LENGTH')
                precision = col.get('DATA_PRECISION')
                scale = col.get('DATA_SCALE')
                
                sf_type = self._map_data_type(data_type, length, precision, scale)
                col_definitions.append(f"    {col_name} {sf_type}")
            
            # Add Audit Column as per requirement
            col_definitions.append(f"    ETL_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()")
            
            ddl = f"CREATE OR REPLACE TABLE {full_schema}{table_name} (\n"
            ddl += ",\n".join(col_definitions)
            ddl += "\n);"
            
            ddls.append(ddl)
            
        return "\n\n".join(ddls)

if __name__ == "__main__":
    # Example Usage
    csv_path = "GBP_ORACLE_DDL_FILE_EXTRACT.csv"
    
    # Create sample CSV for testing if not exists (based on image)
    if not os.path.exists(csv_path):
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "DATA_LENGTH", "DATA_PRECISION", "DATA_SCALE"])
            writer.writerow(["GDS_NRT_CMP_TRG_LIST", "SEQ_NUM", "NUMBER", "22", "", ""])
            writer.writerow(["GDS_NRT_CMP_TRG_LIST", "TRIGGER_NAME", "NVARCHAR2", "256", "", ""])
            writer.writerow(["GDS_RT_DEL_FID_CONFIG", "CREATED_DATE", "TIMESTAMP(6)", "11", "", "6"])

    generator = SnowflakeDDLGenerator(csv_path)
    
    # Parameters
    tables_to_generate = ["GDS_NRT_CMP_TRG_LIST", "GDS_RT_DEL_FID_CONFIG"]
    target_schema = "STAGING_DB.RAW_SCHEMA"
    
    ddl_output = generator.generate_ddl(tables_to_generate, schema_name=target_schema)
    
    print("Generated Snowflake DDLs:\n")
    print(ddl_output)
    
    # Optionally save to a file
    with open("snowflake_ddls.sql", "w") as f:
        f.write(ddl_output)
        print(f"\nDDLs saved to snowflake_ddls.sql")
