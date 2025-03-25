import pandas as pd
import json
from openai import OpenAI
import time
import re
from typing import Dict, Optional, Tuple
import os
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure Perplexity API
client = OpenAI(
    api_key="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",
    base_url="https://api.perplexity.ai"
)

def extract_output_content(raw_response):
    """Extract content between <output> and </output> tags."""
    if not raw_response:
        return None
        
    output_match = re.search(r'<output>(.*?)</output>', raw_response, re.DOTALL)
    if output_match:
        return output_match.group(1).strip()
    return None

def expand_cost_driver(cost_driver: str, reasoning: str, document_name: str = None, article: str = None):
    """
    Use Perplexity API to expand on a cost driver using its reasoning and context.
    
    Args:
        cost_driver (str): The cost driver to expand
        reasoning (str): The reasoning/explanation for the cost driver
        document_name (str): Optional document name for context
        article (str): Optional article reference for context
        
    Returns:
        tuple: (raw response, output content, citations)
    """
    if not cost_driver or pd.isna(cost_driver) or cost_driver == 'NA':
        return None, None, None
        
    # Build context string including document and article info if available
    context = ""
    if document_name and not pd.isna(document_name):
        context += f"Document: {document_name}\n"
    if article and not pd.isna(article):
        context += f"Article: {article}\n"
    context += f"Reasoning: {reasoning}"
        
    prompt = f"""I am analyzing cost drivers for EUDR (European Union Deforestation Regulation) compliance and their potential ad valorem rate implications for non-EU exporters.

Cost Driver: {cost_driver}
Context: {context}

You can think through your analysis first before providing the final output. 

Your final response MUST be wrapped in <output></output> XML tags. Only the content within these tags will be shown to the end user.

Within your <output> tags, please provide:
1. A detailed explanation of what this cost driver means in the context of EUDR compliance
2. Potential cost items that would be incurred by non-EU exporters
3. Estimated impact on operational costs for exporters (low/medium/high)
4. A "Citations" section with relevant sources from EUDR documentation

Please structure your response with clear sections."""

    try:
        response = client.chat.completions.create(
            model="sonar-deep-research",  # Using Perplexity's research model
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert in EUDR compliance, international trade, and cost analysis for non-EU exporters. Provide detailed, well-cited responses with practical insights. Always include your final response within <output></output> XML tags."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.1
        )
        
        # Get the text content
        raw_response = response.choices[0].message.content.strip()
        
        # Extract the content between <output> tags
        output_content = extract_output_content(raw_response)
        
        # Get the citations as a string
        citations = None
        if hasattr(response, 'citations'):
            citations = str(response.citations)
        
        # Return all three components
        return raw_response, output_content, citations
        
    except Exception as e:
        print(f"Error expanding cost driver: {e}")
        return None, None, None

def process_excel(input_file: str, output_file: str, batch_size: int = 10, test_rows: int = None):
    """
    Process the Excel file and expand cost drivers using Perplexity API.
    
    Args:
        input_file (str): Path to input Excel file
        output_file (str): Path to output Excel file
        batch_size (int): Number of rows to process before saving checkpoint
        test_rows (int): Number of rows to process for testing, if None process all rows
    """
    # Read the Excel file
    print(f"Reading Excel file: {input_file}")
    df = pd.read_excel(input_file)
    
    # Ensure required columns exist
    required_cols = ['cost_driver', 'reasoning']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required columns: {required_cols}")
    
    # Add new columns
    df['raw_response'] = None
    df['output_content'] = None
    df['citations'] = None
    
    # Limit rows for testing if specified
    if test_rows is not None:
        df = df.head(test_rows)
    
    total_rows = len(df)
    print(f"Processing {total_rows} rows...")
    
    # Process each row with tqdm progress bar
    for idx in tqdm(range(len(df)), desc="Expanding cost drivers"):
        row = df.iloc[idx]
        
        # Skip rows with NA cost drivers
        if row['cost_driver'] == 'NA' or pd.isna(row['cost_driver']):
            continue
            
        # Get document info for context if available
        document_name = row.get('document_name', None)
        article = row.get('article', None)
        
        # Expand cost driver and get full response
        raw_response, output_content, citations = expand_cost_driver(
            row['cost_driver'], 
            row['reasoning'],
            document_name,
            article
        )
        
        # Store all three components
        df.at[idx, 'raw_response'] = raw_response
        df.at[idx, 'output_content'] = output_content
        df.at[idx, 'citations'] = citations
        
        # Print the first response to see the format
        if idx == 0:
            print("\nExample of raw response format:\n")
            print(raw_response)
            print("\nExample of output content:\n")
            print(output_content)
            print("\nCitations:\n")
            print(citations)
            print("\n" + "-"*80 + "\n")
        
        # Save checkpoint every batch_size rows
        if (idx + 1) % batch_size == 0:
            checkpoint_file = f"{os.path.splitext(output_file)[0]}_checkpoint_{idx+1}.xlsx"
            df.to_excel(checkpoint_file, index=False)
            print(f"\nCheckpoint saved to {checkpoint_file}")
            
        # Add delay to avoid rate limits
        time.sleep(1)
    
    # Save final results
    df.to_excel(output_file, index=False)
    print(f"\nProcessing complete. Results saved to {output_file}")

if __name__ == "__main__":
    input_file = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/extracted_cost_drivers.xlsx"
    output_file = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/expanded_cost_drivers_2.xlsx"
    
    # Process all rows with a batch size of 100
    process_excel(input_file, output_file, batch_size=100)