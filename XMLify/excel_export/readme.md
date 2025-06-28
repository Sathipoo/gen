# 🔄 Informatica Lineage Analysis Framework

## 📋 Overview

This framework is designed to analyze Informatica PowerCenter mappings and provide comprehensive lineage documentation. If you're familiar with Informatica PowerCenter Designer and understand concepts like mappings, transformations, and connectors, this tool will help you automatically generate detailed lineage reports from your XML workflow exports.

## 🎯 What This Framework Does

**Think of this as an automated Informatica mapping documentation tool that:**
- Reads your exported PowerCenter XML workflow files
- Analyzes the data flow and transformations within each mapping  
- Calculates execution order and identifies parallel processing opportunities
- Generates comprehensive Excel reports with multiple analysis views
- Provides field-level lineage tracing from source to target

## 🏗️ High-Level Architecture

```
📁 Informatica XML Files
          ↓
🔄 XML to JSON Conversion (xmltodict)
          ↓
🧠 Core Lineage Engine (InfaLineageGenerator)
    ├── Parse Mapping Structure
    ├── Build Lineage Graph (NetworkX)
    ├── Calculate Execution Order
    ├── Extract Transformation Logic
    └── Generate Connector Details
          ↓
📊 Output Generation
    ├── CSV Export (Field-level details)
    ├── Single Mapping Excel Report
    └── Multi-Mapping Excel Report
```

## 🔧 Core Components

### 1. **InfaLineageGenerator** (`basic_config_loader.py`)
*The heart of the framework - analyzes individual mappings*

**What it does:**
- **Parses Mapping Structure**: Extracts all transformations, instances, and connectors from XML
- **Builds Lineage Graph**: Creates a directed graph showing data flow using NetworkX
- **Calculates Execution Order**: Determines which transformations can run in parallel vs sequentially
- **Extracts Business Logic**: Pulls out filter conditions, expressions, and transformation rules
- **Generates Connector Data**: Creates detailed field-level lineage information

**Key Informatica Concepts Handled:**
- Source/Target Definitions
- Expression Transformations
- Filter Transformations  
- Aggregator Transformations
- Lookup Procedures
- Source/XML Source Qualifiers
- Update Strategy Transformations

### 2. **Excel Exporters**
*Generate comprehensive documentation*

#### Single Mapping Exporter (`lineage_excel_exporter.py`)
Creates a detailed Excel report for one mapping with 6 sheets:
- **📊 Overview**: Mapping statistics and component analysis
- **🔗 Detailed Lineage**: Field-level source-to-target connections
- **⚙️ Transformation Logic**: Business rules and expressions
- **📋 Execution Plan**: Parallel execution analysis
- **🔄 Data Flow**: Component relationships and flow patterns
- **📉 Performance Analysis**: Bottleneck identification

#### Multi-Mapping Exporter (`multi_mapping_excel_exporter.py`)
Consolidates multiple mappings into a single Excel file with the same 6-sheet structure, allowing you to analyze entire workflows at once.

### 3. **Data Structures**

#### ConnectorInfo Class
Represents a single field-to-field connection:
```python
- mapping_name: Which mapping this belongs to
- from_field/to_field: Source and target field names
- from_instance/to_instance: Source and target transformation names
- transformation_order: Execution sequence number
- transformation_logic: Business rules applied
- parallel_group: Which transformations can run simultaneously
```

## 🚀 Process Flow

### Step 1: Prepare Your Informatica Files
1. Export your PowerCenter workflows as XML files
2. Place XML files in a directory (e.g., `/dydy` folder)

### Step 2: Initialize the Framework
```python
# Convert XML to JSON for processing
xml_files_path = "/path/to/your/xml/files"
workflow_json = process_xml_file('your_workflow.XML')
```

### Step 3: Analyze Individual Mappings
```python
# Create lineage generator for a specific mapping
mapping_analyzer = InfaLineageGenerator('your_mapping_name', workflow_json)

# The framework automatically:
# - Validates mapping exists in workflow
# - Extracts all transformations and connectors
# - Builds directed graph of data flow
# - Calculates execution dependencies
# - Identifies parallel processing opportunities
```

### Step 4: Generate Reports

#### Option A: Single Mapping Excel Report
```python
from lineage_excel_exporter import export_mapping_to_excel
export_mapping_to_excel(mapping_analyzer, "mapping_report.xlsx")
```

#### Option B: Multi-Mapping Consolidated Report
```python
from multi_mapping_excel_exporter import *

# Create exporter
exporter = create_multi_mapping_exporter("all_mappings_report.xlsx")

# Add multiple mappings
mapping1 = InfaLineageGenerator('mapping_1', workflow_json)
mapping2 = InfaLineageGenerator('mapping_2', workflow_json)

add_mapping_to_excel(exporter, mapping1)
add_mapping_to_excel(exporter, mapping2)

# Finalize report
finalize_multi_mapping_excel(exporter)
```

#### Option C: Process Entire Workflow
```python
# Automatically process all mappings in a workflow
def process_all_mappings(workflow_json, workflow_name):
    exporter = create_multi_mapping_exporter(f"{workflow_name}_complete.xlsx")
    
    # Extract all mappings from workflow
    for mapping in extract_mappings(workflow_json):
        mapping_name = mapping.get('@NAME')
        try:
            analyzer = InfaLineageGenerator(mapping_name, workflow_json)
            add_mapping_to_excel(exporter, analyzer)
        except Exception as e:
            print(f"Failed to process {mapping_name}: {e}")
    
    finalize_multi_mapping_excel(exporter)
```

## 📊 Understanding the Output

### Excel Report Sheets Explained

**📊 Overview Sheet**
- Mapping statistics (transformation counts by type)
- Component analysis (how many data flow pipelines)
- Execution level summary (parallel processing opportunities)

**🔗 Detailed Lineage Sheet**  
- Field-by-field lineage from source to target
- Transformation type for each step
- Execution order and parallel grouping
- Truncated transformation logic

**⚙️ Transformation Logic Sheet**
- Complete business rules for each transformation
- Filter conditions, expressions, lookup logic
- Field count per transformation

**📋 Execution Plan Sheet**
- Visual representation of execution sequence
- Shows which transformations can run in parallel
- Identifies execution dependencies

**🔄 Data Flow Sheet**
- Component relationship analysis
- Pipeline classification (single-source, multi-target, etc.)
- Data flow patterns

**📉 Performance Analysis Sheet**
- Bottleneck identification (high fan-in/fan-out)
- Parallel efficiency calculations
- Performance optimization suggestions

## 🎯 Key Benefits for Informatica Developers

### 1. **Automated Documentation**
- No more manual mapping documentation
- Consistent format across all mappings
- Always up-to-date with current XML exports

### 2. **Performance Analysis**
- Identify bottlenecks in your mappings
- Optimize parallel processing
- Calculate execution efficiency

### 3. **Impact Analysis**
- Understand complete field lineage
- Track data transformations end-to-end
- Support change impact assessments

### 4. **Compliance & Auditing**
- Generate regulatory compliance reports
- Document data transformation rules
- Provide audit trails for data governance

## 🛠️ Practical Usage Examples

### Scenario 1: Single Mapping Analysis
```python
# Analyze one mapping in detail
p1 = InfaLineageGenerator('m_CUSTOMER_LOAD', workflow_json)
export_mapping_to_excel(p1, "customer_load_analysis.xlsx")
```

### Scenario 2: Workflow Comparison
```python
# Compare multiple workflows
workflows = {
    "PROD": prod_workflow_json,
    "DEV": dev_workflow_json,
    "TEST": test_workflow_json
}

for env, wf_json in workflows.items():
    process_all_mappings(wf_json, f"{env}_environment")
```

### Scenario 3: Migration Documentation
```python
# Document all mappings before migration
migration_exporter = create_multi_mapping_exporter("pre_migration_baseline.xlsx")

for mapping_name in get_all_mapping_names(workflow_json):
    analyzer = InfaLineageGenerator(mapping_name, workflow_json)
    add_mapping_to_excel(migration_exporter, analyzer)

finalize_multi_mapping_excel(migration_exporter)
```

## 🔍 Understanding Transformation Types

The framework recognizes all standard Informatica transformations:

| Informatica Transformation | Framework Prefix | Description |
|---------------------------|------------------|-------------|
| Source Definition | `SRC_` | Data source tables/files |
| Target Definition | `TGT_` | Data target tables/files |
| Source Qualifier | `SRCQ_` | SQL override and source filtering |
| Expression | `EXPR_` | Field calculations and derivations |
| Filter | `FILT_` | Row filtering conditions |
| Aggregator | `AGGR_` | Grouping and aggregation functions |
| Lookup Procedure | `LOOK_` | Reference data lookups |
| Update Strategy | `UPD_` | Insert/Update/Delete strategies |

## 📝 Output File Formats

### CSV Export
- Field-level lineage details
- Suitable for data lineage tools
- Easy to import into databases

### Excel Reports
- Comprehensive multi-sheet analysis
- Visual formatting and color coding
- Business-ready documentation

## ⚠️ Prerequisites & Requirements

**Informatica Knowledge Required:**
- Understanding of PowerCenter mappings
- Familiarity with transformation types
- Knowledge of XML workflow exports

**Technical Requirements:**
- Python 3.7+
- Required packages: `pandas`, `networkx`, `openpyxl`, `xmltodict`
- Access to Informatica XML workflow exports

## 🎉 Getting Started

1. **Install Dependencies**: `pip install -r requirements.txt`
2. **Export XML**: Export your PowerCenter workflows as XML files
3. **Run Analysis**: Use the Jupyter notebook examples in `nb_try.ipynb`
4. **Generate Reports**: Follow the examples above to create your lineage documentation

This framework transforms the tedious task of manually documenting Informatica mappings into an automated, comprehensive analysis that provides insights you might never have discovered manually. 
