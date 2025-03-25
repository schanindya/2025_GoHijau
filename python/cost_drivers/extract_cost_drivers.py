import pandas as pd
import re
import ast
from tqdm import tqdm
import os

def extract_content_from_api_response(api_response_str):
    """Extract the content field from the API response string."""
    try:
        # The API response is in string format but represents a Python object
        # Convert to a dictionary using ast.literal_eval
        if isinstance(api_response_str, str):
            response_dict = ast.literal_eval(api_response_str)
            # Extract content from the response
            if 'choices' in response_dict and len(response_dict['choices']) > 0:
                if 'message' in response_dict['choices'][0]:
                    return response_dict['choices'][0]['message'].get('content', '')
    except Exception as e:
        print(f"Error parsing API response: {e}")
    
    # Return the original string if we couldn't parse it
    return api_response_str

def extract_cost_drivers_and_reasoning(content):
    """Extract cost drivers and reasoning from the content."""
    # First, extract the output section
    output_match = re.search(r'<output>(.*?)</output>', content, re.DOTALL)
    if not output_match:
        print(f"Warning: No <output> tags found in content")
        return []
    
    output_content = output_match.group(1).strip()
    
    # Check if there are no cost drivers (output is NA)
    if output_content.lower() == 'na':
        # Return a special marker for NA output
        return [{'is_na': True}]
    
    # Extract all cost drivers and reasoning
    cost_drivers_reasoning = []
    
    # Find all cost driver and reasoning pairs
    driver_pattern = r'<cost_driver(\d+)>(.*?)</cost_driver\1>'
    reasoning_pattern = r'<reasoning\1>(.*?)</reasoning\1>'
    
    drivers = re.findall(driver_pattern, output_content, re.DOTALL)
    
    for number, driver_text in drivers:
        # Find the corresponding reasoning
        reasoning_match = re.search(f'<reasoning{number}>(.*?)</reasoning{number}>', output_content, re.DOTALL)
        if reasoning_match:
            reasoning_text = reasoning_match.group(1).strip()
            cost_drivers_reasoning.append({
                'is_na': False,
                'cost_driver_number': number,
                'cost_driver': driver_text.strip(),
                'reasoning': reasoning_text
            })
    
    return cost_drivers_reasoning

def process_excel_file(input_file, output_file):
    """Process the Excel file and extract cost drivers and reasoning."""
    print(f"Reading Excel file: {input_file}")
    df = pd.read_excel(input_file)
    
    # Check if the cost_driver_analysis column exists
    if 'cost_driver_analysis' not in df.columns:
        print(f"Error: 'cost_driver_analysis' column not found. Available columns: {df.columns.tolist()}")
        return
    
    # Prepare lists to collect the extracted data
    all_data = []
    
    # Process each row
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing rows"):
        # Get all the metadata columns that exist
        row_data = {}
        for col in ['document_name', 'page_number', 'article', 'text']:
            if col in df.columns:
                row_data[col] = row.get(col, 'Unknown')
        
        # Get the API response(s) for this row
        api_responses = row['cost_driver_analysis']
        
        # Skip if empty
        if pd.isna(api_responses) or api_responses is None:
            continue
        
        # Convert to list if it's not already
        if not isinstance(api_responses, list):
            api_responses = [api_responses]
        
        # Process each API response
        for api_response in api_responses:
            # Extract content from API response
            content = extract_content_from_api_response(api_response)
            
            # Extract cost drivers and reasoning
            cost_drivers_reasoning = extract_cost_drivers_and_reasoning(content)
            
            # Add to the data list
            for item in cost_drivers_reasoning:
                # Create a new row with all the metadata
                new_row = row_data.copy()
                
                # Check if the result is NA
                if item.get('is_na', False):
                    # Add NA values for the cost driver info
                    new_row.update({
                        'cost_driver_number': 'NA',
                        'cost_driver': 'NA',
                        'reasoning': 'NA'
                    })
                else:
                    # Add the cost driver info
                    new_row.update({
                        'cost_driver_number': item['cost_driver_number'],
                        'cost_driver': item['cost_driver'],
                        'reasoning': item['reasoning']
                    })
                all_data.append(new_row)
    
    # Create a new dataframe with the extracted data
    result_df = pd.DataFrame(all_data)
    
    # Save to Excel
    print(f"Saving results to: {output_file}")
    result_df.to_excel(output_file, index=False)
    
    print(f"Successfully extracted {len(result_df)} cost drivers and saved to {output_file}")
    
    return result_df

if __name__ == "__main__":
    # Define input and output file paths
    input_directory = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output"
    input_file = os.path.join(input_directory, "all_eudr_cost_drivers_raw_responses.xlsx")
    output_file = os.path.join(input_directory, "extracted_cost_drivers.xlsx")
    
    # Process the file
    result_df = process_excel_file(input_file, output_file)
    
    # Optional: Print a summary
    if result_df is not None:
        print("\nSummary:")
        print(f"Total cost drivers extracted: {len(result_df)}")
        
        # Only try to access columns we know exist in the result
        if 'document_name' in result_df.columns:
            print(f"Documents analyzed: {result_df['document_name'].nunique()}")
        if 'article' in result_df.columns:
            print(f"Articles analyzed: {result_df['article'].nunique()}")
        
        # Count occurrences of each cost driver number
        na_count = sum(1 for x in result_df['cost_driver_number'] if x == 'NA')
        print(f"\nResponses with NA (no cost drivers): {na_count}")
        
        numeric_cost_drivers = result_df[result_df['cost_driver_number'] != 'NA']['cost_driver_number']
        if not numeric_cost_drivers.empty:
            cost_driver_counts = numeric_cost_drivers.value_counts().sort_index()
            print("\nNumber of cost drivers by number:")
            for number, count in cost_driver_counts.items():
                print(f"  Cost driver {number}: {count}")