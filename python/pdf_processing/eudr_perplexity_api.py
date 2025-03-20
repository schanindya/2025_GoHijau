import pandas as pd
import os
import time
from tqdm import tqdm
import json
import re
from openai import OpenAI
from typing import List, Dict, Any, Tuple

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
    
    def process_all_rows(self, sleep_interval=1, test_rows=None):
        """
        Process rows and store the raw response in the dataframe.
        
        Args:
            sleep_interval (int): Seconds to sleep between API calls
            test_rows (int): Number of rows to process for testing, None for all rows
            
        Returns:
            DataFrame: Processed dataframe with raw responses
        """
        if test_rows is not None:
            rows_to_process = min(test_rows, len(self.df))
            df_to_process = self.df.head(rows_to_process).copy()
        else:
            rows_to_process = len(self.df)
            df_to_process = self.df.copy()
            
        print(f"Processing {rows_to_process} text segments...")
        
        # Add a column for the raw analysis content
        df_to_process['cost_driver_analysis'] = None

        print(df_to_process[:10])
        
        for i, row in tqdm(df_to_process.iterrows(), total=rows_to_process):
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
                print(analysis)
                all_analyses.append(analysis)
                
                # Sleep to avoid rate limits
                time.sleep(sleep_interval)
            
            # Store all analyses for this row
            df_to_process.at[i, 'cost_driver_analysis'] = all_analyses
            
            # Save intermediate results periodically
            if (i + 1) % 10 == 0:
                self.save_intermediate_results(df_to_process.head(i+1), i + 1)
        
        return df_to_process
    
    def save_intermediate_results(self, df, row_count):
        """Save intermediate results after processing a certain number of rows."""
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_analysis_intermediate_{row_count}_{timestamp}.xlsx"
        df.to_excel(output_path, index=False)
        print(f"Intermediate results saved to {output_path}")
    
    def process_and_save(self, output_file, test_rows=None):
        """Process rows and save the final results."""
        final_df = self.process_all_rows(test_rows=test_rows)
        # Save the final DataFrame to a pickle file
        import pickle
        with open("test.pkl", "wb") as f:
            pickle.dump(final_df, f)
        print("DataFrame saved to test.pkl")
        # final_df.to_excel(output_file, index=False)
        print(f"Results saved to {output_file}")

if __name__ == "__main__":

    # # Import the pickle module to load the saved DataFrame
    # import pickle
    
    # # Load the test.pkl file
    # try:
    #     with open("test.pkl", "rb") as f:
    #         df = pickle.load(f)
        
    #     # Print the DataFrame
    #     print("Successfully loaded DataFrame from test.pkl")
    #     print(f"DataFrame shape: {df.shape}")
    #     print("\nDataFrame columns:")
    #     print(df.columns.tolist())
    #     print("\nFirst few rows of the DataFrame:")
    #     print(df.head())
    # except FileNotFoundError:
    #     print("Error: test.pkl file not found")
    # except Exception as e:
    #     print(f"Error loading DataFrame: {str(e)}")

    # print(df.at[0, 'cost_driver_analysis'])

    analyzer = EUDRCostAnalyzer()
    output_file = "/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_cost_drivers_raw_responses.xlsx"
    
    # For testing with 5 rows only
    analyzer.process_and_save(output_file, test_rows=20)


    
    # To process all rows, uncomment the line below and comment the line above
    # analyzer.process_and_save(output_file)