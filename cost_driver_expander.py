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

def expand_cost_driver(cost_driver: str, context_text: str) -> Optional[Dict]:
    """
    Use Perplexity API to expand on a cost driver using its context.
    
    Args:
        cost_driver (str): The cost driver to expand
        context_text (str): The text from which the cost driver was inferred
        
    Returns:
        Dict: Dictionary containing explanation and citations, or None if empty input
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
        
        return {
            'content': response.choices[0].message.content.strip(),
            'citations': response.citations if hasattr(response, 'citations') else []
        }
        
    except Exception as e:
        print(f"Error expanding cost driver: {e}")
        return None

def process_excel(input_file: str, output_file: str):
    """
    Process only row 12 of the Excel file and expand its cost driver using Perplexity API.
    
    Args:
        input_file (str): Path to input Excel file
        output_file (str): Path to output Excel file
    """
    # Read the Excel file
    df = pd.read_excel(input_file)
    
    # Ensure required columns exist
    required_cols = ['cost_driver', 'text']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required columns: {required_cols}")
    
    # Add new columns for expanded explanations and citations
    df['cost_driver_expansion'] = None
    df['citations'] = None
    
    # Process only row 12
    if len(df) > 12:
        row = df.iloc[11]  # 0-based indexing, so row 12 is at index 11
        print(f"Processing row 12")
        
        # Expand cost driver
        result = expand_cost_driver(row['cost_driver'], row['text'])
        if result:
            df.at[11, 'cost_driver_expansion'] = result['content']
            df.at[11, 'citations'] = json.dumps(result['citations'])  # Store citations as JSON string
    else:
        print("Row 12 does not exist in the input file.")

    # Save the processed file
    df.to_excel(output_file, index=False)

if __name__ == "__main__":
    input_file = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_cost_analysis_final_processed.xlsx"
    output_file = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_analysis_final_expanded.xlsx"
    process_excel(input_file, output_file)