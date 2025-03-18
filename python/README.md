# EUDR Document Analysis

This Python project processes EUDR PDF documents and performs semantic analysis to identify key cost drivers for CGE modeling.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

2. Place your PDF documents in the `data/pdfs` directory.

## Usage

Run the script:
```bash
python pdf_processing/pdf_to_excel.py
```

The script will:
1. Process all PDFs in the `data/pdfs` directory
2. Extract text and split into paragraphs
3. Perform semantic analysis to identify:
   - Key phrases using KeyBERT
   - Cost-related terms and indicators
4. Save results to an Excel file in `data/output`

## Output Format

The Excel file will contain the following columns:
- document_name: Name of the source PDF
- page_number: Page where the paragraph appears
- paragraph_number: Sequential number of the paragraph on the page
- text: Full paragraph text
- word_count: Number of words in the paragraph
- key_phrases: Extracted key phrases with relevance scores
- cost_drivers: Identified cost-related terms and indicators

## Customization

You can modify the following in `pdf_to_excel.py`:
- Number of key phrases extracted (default: 5)
- Cost-related terms list
- Paragraph separation logic
- Output format 