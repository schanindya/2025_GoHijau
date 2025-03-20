import pandas as pd
import json
import openai
from typing import List, Dict, Tuple
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure OpenAI
openai.api_key = "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"

def extract_cost_driver_names(cost_analysis_str: str) -> List[str]:
    """Extract cost driver names from the JSON string."""
    try:
        data = json.loads(cost_analysis_str)
        if isinstance(data, list) and len(data) > 0:
            cost_drivers = data[0].get('cost_drivers', [])
            # Extract all cost driver names and clean them
            names = []
            for driver in cost_drivers:
                if driver.get('name'):
                    # Clean the name by removing extra whitespace and special characters
                    clean_name = driver['name'].strip()
                    names.append(clean_name)
            return names
    except json.JSONDecodeError:
        print(f"Error decoding JSON: {cost_analysis_str}")
    return []

def is_relevant_cost_driver(cost_driver: str) -> bool:
    """Use OpenAI to determine if the cost driver is relevant for EUDR compliance."""
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert in EUDR (European Union Deforestation Regulation) compliance and cost analysis. Your task is to determine if a given cost driver is relevant for companies complying with EUDR. Only consider costs that could be directly incurred by companies due to EUDR requirements."},
                {"role": "user", "content": f"Is this cost driver relevant for EUDR compliance? Answer with 'yes' or 'no' only: {cost_driver}"}
            ],
            temperature=0.1,
            max_tokens=10
        )
        answer = response.choices[0].message.content.strip().lower()
        return answer == 'yes'
    except Exception as e:
        print(f"Error checking relevance: {e}")
        return False

def process_cost_drivers(input_file: str, output_file: str):
    """Process the Excel file and clean cost drivers."""
    # Read the Excel file
    df = pd.read_excel(input_file)
    
    # Ensure cost_analysis column exists
    if 'cost_analysis' not in df.columns:
        raise ValueError("cost_analysis column not found in the Excel file")
    
    # Create a list to store new rows
    new_rows = []
    total_rows = len(df)
    
    for idx, row in df.iterrows():
        print(f"Processing row {idx + 1}/{total_rows}")
        cost_analysis = row['cost_analysis']
        if pd.isna(cost_analysis):
            # Add empty row with original data
            new_row = row.to_dict()
            new_row['cost_driver'] = ''
            new_rows.append(new_row)
            continue
            
        # Extract cost driver names
        drivers = extract_cost_driver_names(str(cost_analysis))
        
        # Filter relevant drivers
        relevant_drivers = []
        for driver in drivers:
            if is_relevant_cost_driver(driver):
                relevant_drivers.append(driver)
        
        if not relevant_drivers:
            # Add empty row with original data if no relevant drivers
            new_row = row.to_dict()
            new_row['cost_driver'] = ''
            new_rows.append(new_row)
        else:
            # Create a new row for each relevant cost driver
            for driver in relevant_drivers:
                new_row = row.to_dict()
                new_row['cost_driver'] = driver
                new_rows.append(new_row)
        
        # Save intermediate results every 100 rows
        if (idx + 1) % 100 == 0:
            temp_df = pd.DataFrame(new_rows)
            temp_checkpoint_file = f"{os.path.splitext(output_file)[0]}_checkpoint_{idx+1}.xlsx"
            temp_df.to_excel(temp_checkpoint_file, index=False)
            print(f"Checkpoint saved at row {idx+1} to {temp_checkpoint_file}")
    
    # Create final dataframe with all new rows
    final_df = pd.DataFrame(new_rows)
    
    # Reorder columns to put cost_driver in a logical position
    columns = ['document_name', 'page_number', 'article', 'text', 'cost_driver']
    other_cols = [col for col in final_df.columns if col not in columns]
    final_df = final_df[columns + other_cols]
    
    # Save to new Excel file
    final_df.to_excel(output_file, index=False)
    print(f"Processing complete. Results saved to {output_file}")

if __name__ == "__main__":
    input_file = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/2025_GoHijau/output.xlsx"
    output_file = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_analysis_final_processed.xlsx"
    process_cost_drivers(input_file, output_file) 