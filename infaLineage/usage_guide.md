# Informatica Lineage Generator - Usage Guide

This utility allows you to parse Informatica XML exports and generate a lineage report in Markdown format.

## **Available Command Options**

| Option | Requirement | Description |
| :--- | :--- | :--- |
| `--xml` | **Required** | Path to an Informatica XML file or a directory containing multiple XMLs. |
| `--workflow` | Optional | Name of a specific workflow to extract. If omitted, all workflows are processed. |
| `--folder` | Optional | Name of a specific Informatica folder to extract. If omitted, all folders are processed. |
| `--output` | Optional | Filename for the generated Markdown report. If omitted, output is printed to the console. |

---

## **Example Commands**

### **1. Process a Single XML File**
Generates a lineage report for all folders and workflows found in the XML:
```bash
python3 informatica_md_generator/xml_parser.py --xml xmls/WF_COMPLETE.XML --output report.md


2. Process a Specific Workflow
Extracts only the metadata for the workflow named WF_COMPLETE:

python3 informatica_md_generator/xml_parser.py --xml xmls/WF_COMPLETE.XML --workflow WF_COMPLETE --output wf_report.md


3. Batch Process an Entire Directory
Recursively scans the xmls/ directory and generates a combined report for all found XML files:

python3 informatica_md_generator/xml_parser.py --xml xmls/ --output batch_report.md

4. Filter by Folder

Only processes objects within the Informatica folder named project_kyc702_2:

python3 informatica_md_generator/xml_parser.py --xml xmls/WF_COMPLETE.XML --folder project_kyc702_2 --output folder_report.md
