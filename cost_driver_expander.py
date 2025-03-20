import pandas as pd
import json
from openai import OpenAI
import time
from typing import Dict, Optional
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure Perplexity API
client = OpenAI(
    api_key="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",
    base_url="https://api.perplexity.ai"
)

def expand_cost_driver(cost_driver: str, context_text: str) -> Optional[str]:
    """
    Use Perplexity API to expand on a cost driver using its context.
    
    Args:
        cost_driver (str): The cost driver to expand
        context_text (str): The text from which the cost driver was inferred
        
    Returns:
        str: Expanded explanation with citations, or None if empty input
    """
    if not cost_driver or pd.isna(cost_driver):
        return None
        
    prompt = f"""I am analyzing cost drivers for EUDR (European Union Deforestation Regulation) compliance and their potential ad valorem rate implications.

Cost Driver: {cost_driver}
Context: {context_text}

Please provide:
1. A detailed explanation of what this cost driver means in the context of EUDR compliance
2. Potential cost items that would be incurred
3. Relevant citations and sources from EUDR documentation or related materials

Please structure your response with clear sections for explanation, cost items, and citations."""

    try:
        response = client.chat.completions.create(
            model="sonar-deep-research",
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert in EUDR compliance, international trade, and cost analysis. Provide detailed, well-cited responses."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.2
        )
        
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        print(f"Error expanding cost driver: {e}")
        return None

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
    df = pd.read_excel(input_file)
    
    # Ensure required columns exist
    required_cols = ['cost_driver', 'text']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required columns: {required_cols}")
    
    # Add new column for expanded explanations
    df['cost_driver_expansion'] = None
    
    # Limit rows for testing if specified
    if test_rows is not None:
        df = df.head(test_rows)
    
    total_rows = len(df)
    print(f"Processing {total_rows} rows...")
    
    for idx, row in df.iterrows():
        print(f"Processing row {idx + 1}/{total_rows}")
        
        # Expand cost driver
        expansion = expand_cost_driver(row['cost_driver'], row['text'])
        df.at[idx, 'cost_driver_expansion'] = expansion
        
        # Save checkpoint every batch_size rows
        if (idx + 1) % batch_size == 0:
            checkpoint_file = f"{os.path.splitext(output_file)[0]}_checkpoint_{idx+1}.xlsx"
            df.to_excel(checkpoint_file, index=False)
            print(f"Checkpoint saved to {checkpoint_file}")
            
        # Add delay to avoid rate limits
        time.sleep(1)
    
    # Save final results
    df.to_excel(output_file, index=False)
    print(f"Processing complete. Results saved to {output_file}")

if __name__ == "__main__":
    input_file = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_cost_analysis_final_processed.xlsx"
    output_file = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_analysis_final_expanded.xlsx"
    process_excel(input_file, output_file, test_rows=20)