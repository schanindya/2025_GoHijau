import pandas as pd
import os
import time
from tqdm import tqdm
import json
import re
from openai import OpenAI
from typing import List, Dict, Any, Tuple
import pickle
import glob

# Configure Perplexity API
client = OpenAI(
    api_key="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",
    base_url="https://api.perplexity.ai"
)

class EUDRCostAnalyzer:
    def __init__(self, input_file="/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/final_paragraphs_with_xml_tags.xlsx"):
        """Initialize the EUDR cost analyzer."""
        self.df = pd.read_excel(input_file)
        
    def extract_paragraph_text(self, text):
        """Extract text between <paragraph> and </paragraph> tags."""
        paragraphs = re.findall(r'<paragraph>(.*?)</paragraph>', text, re.DOTALL)
        return paragraphs
    
    def get_cost_driver_analysis(self, text, doc_name=None, article=None):
        """
        Use Perplexity API to get cost driver analysis.
        Returns the raw response content.
        
        Args:
            text (str): The text to analyze
            doc_name (str): Document name for reference
            article (str): Article number/section for reference
            
        Returns:
            str: The raw response content
        """
        prompt = f"""You are an analyst from a non-EU exporting country (like Indonesia) reviewing EUDR regulations. Your task is to:

1. Analyze the provided text segment to identify explicit cost drivers that would directly impact non-EU exporters.
2. For each identified cost driver, provide clear reasoning explaining why it represents a direct cost impact for non-EU exporters.

Important Guidelines:
- Only include cost drivers that are EXPLICITLY mentioned in the text
- Consider ONLY costs that directly affect non-EU exporters
- Even if you understand what EUDR requires in general, ONLY analyze what is explicitly stated in this specific text segment
- Exclude costs that:
  * Are borne by EU operators
  * Come from other regulations/policies
  * Are implicit or assumed
  * Are known EUDR requirements but not mentioned in this specific text
- Analyze from a non-EU country perspective
- Include ALL relevant cost drivers found, no matter how many
- Each cost driver must have its own numbered XML tags and corresponding reasoning
- You may discuss your analysis process or explain your thinking BEFORE providing the output
- ALWAYS include the <output> section, even after discussion
- Keep all analysis and discussion BEFORE the <output> tags

Return your analysis in the following XML format:

[Optional: Add any analysis, discussion, or explanation here, BEFORE the output tags]

<output>
[If cost drivers are found:]
<cost_driver1>
[First cost driver identified]
</cost_driver1>
<reasoning1>
- Text reference: [exact quote or specific reference]
- Direct cost impact: [explanation]
- Operational impact: [specific effect on export operations]
</reasoning1>

<cost_driver2>
[Second cost driver identified]
</cost_driver2>
<reasoning2>
- Text reference: [exact quote or specific reference]
- Direct cost impact: [explanation]
- Operational impact: [specific effect on export operations]
</reasoning2>

[Continue pattern for all identified cost drivers...]

[If no cost drivers are found:]
NA
</output>

Example Output with Discussion:
Let me analyze this text carefully. I notice several key requirements that would create direct costs for non-EU exporters. I'll focus particularly on explicit mentions of new systems or processes that would require investment...

<output>
<cost_driver1>
Supply chain traceability systems
</cost_driver1>
<reasoning1>
- Text reference: "operators must implement comprehensive traceability systems"
- Direct cost impact: Investment in new tracking technologies and software
- Operational impact: Implementation of new digital tracking processes
</reasoning1>

<cost_driver2>
Due diligence documentation
</cost_driver2>
<reasoning2>
- Text reference: "maintain detailed documentation of supply chain"
- Direct cost impact: Additional administrative staff and documentation systems
- Operational impact: New documentation workflow and storage requirements
</reasoning2>
</output>

Example Output with No Costs:
I've reviewed the text and found no explicit mentions of requirements that would create direct costs for non-EU exporters. While there are some regulatory requirements mentioned, they don't translate to direct costs for exporters.

<output>
NA
</output>

NOW DO THAT FOR THIS FOLLOWING DOCUMENT'S ARTICLE/SECTION

<input>
Document: {doc_name if doc_name else 'Unknown'}
Article/Section: {article if article else 'Unknown'}
TEXT: {text}
</input>"""

        try:
            response = client.chat.completions.create(
                model="r1-1776",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a precise analyst that identifies ONLY explicit cost drivers from EUDR documentation that directly impact non-EU exporters. Only extract cost drivers CLEARLY mentioned in the text. Provide reasoned explanations for each cost driver identified."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0
            )
            
            content = response
            
            # Just extract the content between <output> tags without further parsing
            
            if content:
                return content
            else:
                # If no output tags found, return the full response
                return "NA"
            
        except Exception as e:
            print(f"Error getting cost driver analysis: {e}")
            return f"Error: {str(e)}"
    
    def find_latest_pickle(self):
        """Find the latest pickle file and the number of rows processed."""
        pickle_pattern = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_analysis_rows_*_*.pkl"
        pickle_files = glob.glob(pickle_pattern)
        
        if not pickle_files:
            return None, 0
            
        # Sort by modification time (newest first)
        pickle_files.sort(key=os.path.getmtime, reverse=True)
        latest_pickle = pickle_files[0]
        
        # Extract row count from filename
        try:
            # Pattern is eudr_analysis_rows_NUMBER_timestamp.pkl
            row_count = int(os.path.basename(latest_pickle).split('_')[3])
            return latest_pickle, row_count
        except:
            # If we can't extract row count, assume 0
            return latest_pickle, 0
    
    def process_all_rows(self, sleep_interval=1, start_row=None):
        """
        Process rows and store the raw response in the dataframe.
        Can resume from a previous run.
        
        Args:
            sleep_interval (int): Seconds to sleep between API calls
            start_row (int): Row index to start processing from (for resuming)
            
        Returns:
            DataFrame: Processed dataframe with raw responses
        """
        # If no start_row specified, try to find latest pickle and resume
        if start_row is None:
            latest_pickle, row_count = self.find_latest_pickle()
            if latest_pickle:
                print(f"Found latest pickle: {latest_pickle} with {row_count} rows processed")
                try:
                    with open(latest_pickle, 'rb') as f:
                        df_to_process = pickle.load(f)
                    print(f"Successfully loaded {len(df_to_process)} rows from pickle")
                    
                    # Make sure we have all the original rows (might need to merge with self.df)
                    if len(df_to_process) < len(self.df):
                        # Get the remaining rows from self.df that aren't in df_to_process
                        remaining_df = self.df.iloc[len(df_to_process):].copy()
                        # Add the cost_driver_analysis column if it doesn't exist
                        if 'cost_driver_analysis' not in remaining_df.columns:
                            remaining_df['cost_driver_analysis'] = None
                        # Combine the processed rows with the remaining rows
                        df_to_process = pd.concat([df_to_process, remaining_df])
                        print(f"Added {len(remaining_df)} remaining rows from original dataset")
                    
                    start_row = row_count
                except Exception as e:
                    print(f"Error loading pickle: {e}")
                    df_to_process = self.df.copy()
                    start_row = 0
            else:
                df_to_process = self.df.copy()
                start_row = 0
        else:
            df_to_process = self.df.copy()
        
        # Ensure the cost_driver_analysis column exists
        if 'cost_driver_analysis' not in df_to_process.columns:
            df_to_process['cost_driver_analysis'] = None
            
        rows_to_process = len(df_to_process)
        print(f"Total rows in dataset: {rows_to_process}")
        print(f"Resuming processing from row {start_row}/{rows_to_process}")
        
        # Process rows starting from start_row
        for i, row in tqdm(df_to_process.iloc[start_row:].iterrows(), 
                          total=rows_to_process-start_row, 
                          initial=start_row, 
                          desc="Processing rows"):
            text = row['text']
            
            if not isinstance(text, str) or len(text) < 10:
                continue
            
            paragraphs = self.extract_paragraph_text(text)
            if not paragraphs:
                paragraphs = [text]
            
            # For each paragraph in the text
            all_analyses = []
            for paragraph in paragraphs:
                doc_name = row.get('document_name', None)
                article = row.get('article', None)
                
                if not isinstance(paragraph, str) or len(paragraph) < 10:
                    continue
                
                # Get raw analysis
                analysis = self.get_cost_driver_analysis(paragraph, doc_name, article)
                print(f"Row {i}: Processed analysis")
                all_analyses.append(analysis)
                
                # Sleep to avoid rate limits
                time.sleep(sleep_interval)
            
            # Store all analyses for this row
            df_to_process.at[i, 'cost_driver_analysis'] = all_analyses
            
            # Current row count (1-based)
            current_row_count = i + 1
            
            # Save intermediate results every 20 rows as pickle
            if current_row_count % 20 == 0 or current_row_count == rows_to_process:
                self.save_intermediate_pickle(df_to_process, current_row_count)
            
            # Save intermediate results every 100 rows to Excel
            if current_row_count % 100 == 0 or current_row_count == rows_to_process:
                self.save_intermediate_excel(df_to_process, current_row_count)
        
        return df_to_process
    
    def save_intermediate_pickle(self, df, row_count):
        """Save intermediate results to pickle file after processing a certain number of rows."""
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        pickle_path = f"/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_analysis_rows_{row_count}_{timestamp}.pkl"
        
        with open(pickle_path, "wb") as f:
            pickle.dump(df, f)
        
        print(f"Intermediate pickle saved to {pickle_path}")
        
        # Print information about the saved pickle
        print(f"Saved DataFrame with {len(df)} rows and {len(df.columns)} columns")
        print(f"Columns: {df.columns.tolist()}")
    
    def save_intermediate_excel(self, df, row_count):
        """Save intermediate results to Excel file after processing a certain number of rows."""
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        excel_path = f"/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_analysis_rows_{row_count}_{timestamp}.xlsx"
        
        df.to_excel(excel_path, index=False)
        
        print(f"Intermediate Excel saved to {excel_path}")
        print(f"Saved Excel with {len(df)} rows and {len(df.columns)} columns")
    
    def process_and_save(self, output_file, start_row=None):
        """Process all rows and save the final results to both pickle and Excel."""
        final_df = self.process_all_rows(start_row=start_row)
        
        # Save to final pickle
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        pickle_path = f"/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_analysis_final_{timestamp}.pkl"
        with open(pickle_path, "wb") as f:
            pickle.dump(final_df, f)
        print(f"Final DataFrame saved to {pickle_path}")
        
        # Save to Excel
        final_df.to_excel(output_file, index=False)
        print(f"Results also saved to Excel: {output_file}")
        
        # Print summary of final results
        print(f"Processed {len(final_df)} rows with {len(final_df.columns)} columns")
        print(f"Columns: {final_df.columns.tolist()}")

if __name__ == "__main__":
    analyzer = EUDRCostAnalyzer()
    output_file = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/all_eudr_cost_drivers_raw_responses.xlsx"
    
    # To resume from a specific row (it stopped at row 134), uncomment this line:
    analyzer.process_and_save(output_file, start_row=0)
    
    # Or to automatically detect and resume from the latest pickle:
    # analyzer.process_and_save(output_file)