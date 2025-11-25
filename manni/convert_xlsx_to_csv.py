import sys
import os
import pandas as pd

def convert_to_csv(input_file, output_file):
    try:
        # Read the Excel file
        df = pd.read_excel(input_file)
        
        # Write to CSV
        df.to_csv(output_file, index=False)
        print(f"Successfully converted '{input_file}' to '{output_file}'")
    except Exception as e:
        print(f"Error converting '{input_file}': {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_xlsx_to_csv.py <input_file> <output_file>")
        sys.exit(1)
        
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    if not os.path.exists(input_path):
        print(f"Error: Input file '{input_path}' not found.")
        sys.exit(1)
        
    convert_to_csv(input_path, output_path)
