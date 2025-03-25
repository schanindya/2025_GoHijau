# GoHijau Project

This is an internal documentation of workstreams related to the GoHijau Project.

## 🌱 Overview

This project consists of two main components:

1. **Emissions Calculation**
   - Calculate and track carbon emissions

2. **EUDR Documentation**
   - Digital transformation of EUDR documentation
   - Semantic analysis of compliance documents
   - Cost driver identification and analysis

## 📊 EUDR Cost Driver Analysis Workflow

This workflow processes EUDR documentation to identify, extract, and analyze cost drivers that impact non-EU exporters.

### Prerequisites

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your environment:
   - Place EUDR PDF documents in `data/pdfs/`
   - Ensure you have the necessary API keys in your `.env` file

### Step-by-Step Process

1. **PDF Processing** (`python/pdf_processing/pdf_to_excel_EUDR.py`)
   - Converts EUDR PDF documents into structured Excel format
   - Extracts text and organizes by paragraphs
   - Output: Excel file with structured document content

2. **Initial Cost Driver Analysis** (`python/pdf_processing/all_eudr_perplexity_api.py`)
   - Analyzes each paragraph using Perplexity API
   - Identifies potential cost drivers and their context
   - Output: Raw API responses with cost driver analysis

3. **Cost Driver Extraction** (`python/cost_drivers/extract_cost_drivers.py`)
   - Processes the raw API responses
   - Extracts structured cost driver information
   - Organizes cost drivers with their reasoning
   - Output: Structured Excel file with extracted cost drivers

4. **Cost Driver Expansion** (`python/cost_drivers/cost_driver_expander.py`)
   - Takes extracted cost drivers and expands their analysis
   - Uses Perplexity API for detailed impact assessment
   - Provides comprehensive analysis of each cost driver
   - Output: Final expanded cost driver analysis

### Running the Pipeline

Execute each script in sequence:

```bash
# 1. Process PDFs
python python/pdf_processing/pdf_to_excel_EUDR.py

# 2. Analyze with Perplexity API
python python/pdf_processing/all_eudr_perplexity_api.py

# 3. Extract Cost Drivers
python python/cost_drivers/extract_cost_drivers.py

# 4. Expand Cost Driver Analysis
python python/cost_drivers/cost_driver_expander.py
```

### Output Files

The pipeline generates several output files in `data/output/`:
- Structured PDF content (Step 1)
- Raw API responses (Step 2)
- Extracted cost drivers (Step 3)
- Expanded cost driver analysis (Step 4)

## 📁 Repository Structure

```
.
├── data/
│   ├── pdfs/          # Input EUDR PDF documents
│   └── output/        # Generated analysis files
├── python/
│   ├── cost_drivers/  # Cost driver processing scripts
│   └── pdf_processing/# PDF processing scripts
└── requirements.txt   # Python dependencies
```

## 🔒 Security Note

- Keep your API keys secure and never commit them to the repository
- Store sensitive credentials in your `.env` file
- The `.env` file is included in `.gitignore`
