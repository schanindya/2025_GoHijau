import pandas as pd
import os
import time
from tqdm import tqdm
import json
import re
from openai import OpenAI

# Set your OpenAI API key.
# For security, consider storing your API key in an environment variable.
client = OpenAI(api_key="YOUR API KEY")

class EUDRCostAnalyzer:
    def __init__(self, input_file="/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/final_paragraphs_with_xml_tags.xlsx"):
        """
        Initialize the EUDR cost analyzer.
        
        Args:
            input_file (str): Path to the Excel file with text data.
        """
        self.df = pd.read_excel(input_file)
        self.results = []
        
    def extract_paragraph_text(self, text):
        """
        Extract text between <paragraph> and </paragraph> tags.
        
        Args:
            text (str): Input text potentially containing XML tags
        
        Returns:
            list: List of extracted paragraph texts
        """
        paragraphs = re.findall(r'<paragraph>(.*?)</paragraph>', text, re.DOTALL)
        return paragraphs
    
    def analyze_text_for_cost_drivers(self, text, doc_name=None, article=None):
        """
        Use the OpenAI API to analyze text for EUDR cost drivers.
        It is expected to return only the cost driver name and the nominal value (i.e. the estimated ad valorem equivalent),
        and nothing else.
        
        Args:
            text (str): The text to analyze.
            doc_name (str): Document name for reference.
            article (str): Article number/section for reference.
        
        Returns:
            dict: Dictionary with a single key "cost_drivers" containing an array of cost driver objects.
                  Each object has two keys: "name" and "nominal".
        """
        prompt = f"""
Analyze the following text from EUDR documentation for potential cost drivers that would impact exporters from countries like Indonesia.
For each cost driver identified, provide only:
1. The cost driver name (as "name")
2. The estimate of ad valorem equivalent (% of product value) as "nominal" (if available; otherwise return null)

You MUST return your answer as valid, properly formatted JSON in exactly the following format:
{{
    "cost_drivers": [
        {{"name": "Example cost driver", "nominal": "Example nominal value"}},
        ...
    ]
}}

Do not include any text before or after the JSON object. Return only valid JSON.

Document: {doc_name if doc_name else 'Unknown'}
Article/Section: {article if article else 'Unknown'}

TEXT:
{text}
"""
        try:
            # Using the Chat Completions API (standard and widely supported)
            response = client.chat.completions.create(
                model="gpt-4o-mini",  # Adjust the model as necessary.
                messages=[
                    {"role": "system", "content": "You are an expert in EU regulations, international trade, and cost analysis. Return only properly formatted valid JSON as specified in the prompt."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2
            )
            
            # Get response content
            response_content = response.choices[0].message.content.strip()
            
            # Parse the JSON output with error handling
            try:
                result = json.loads(response_content)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                print(f"Raw response: {response_content}")
                
                # Attempt to fix common JSON formatting issues
                if response_content.startswith("```json") and response_content.endswith("```"):
                    # Remove markdown code block formatting
                    response_content = response_content[7:-3].strip()
                    try:
                        result = json.loads(response_content)
                    except:
                        # If still failing, return empty result
                        return {"cost_drivers": []}
                else:
                    # If we can't parse, return empty result
                    return {"cost_drivers": []}
            
            # Post-process: extract only "name" and "nominal" for each cost driver.
            filtered_result = {"cost_drivers": []}
            for driver in result.get("cost_drivers", []):
                filtered_driver = {
                    "name": driver.get("name", ""),
                    "nominal": driver.get("nominal", None)
                }
                filtered_result["cost_drivers"].append(filtered_driver)
            
            return filtered_result
            
        except Exception as e:
            print(f"Error analyzing text: {e}")
            return {
                "cost_drivers": []
            }
    
    def process_all_rows(self, sleep_interval=10):
        """
        Process all rows of the dataframe.
        Extracts paragraphs from the text column for analysis.
        
        Args:
            sleep_interval (int): Seconds to sleep between API calls to avoid rate limits.
        """
        print(f"Processing all {len(self.df)} text segments...")
        self.df['cost_analysis'] = None
        
        for i, row in tqdm(self.df.iterrows(), total=len(self.df)):
            # Extract paragraphs from the text column
            text = row['text']
            
            # If text is not a string or too short, skip
            if not isinstance(text, str) or len(text) < 10:
                continue
            
            # Extract paragraphs using XML tags
            paragraphs = self.extract_paragraph_text(text)
            
            # If no paragraphs found, try analyzing the whole text
            if not paragraphs:
                paragraphs = [text]
            
            # Analyze each paragraph
            row_analyses = []
            for paragraph in paragraphs:
                doc_name = row.get('document_name', None)
                article = row.get('article', None)
                
                if not isinstance(paragraph, str) or len(paragraph) < 10:
                    continue
                
                analysis = self.analyze_text_for_cost_drivers(paragraph, doc_name, article)
                row_analyses.append(analysis)
                time.sleep(sleep_interval)
            
            # Store analyses for the row
            self.df.at[i, 'cost_analysis'] = json.dumps(row_analyses)
            self.results.extend(row_analyses)
            
            # Save results every 100 rows
            if (i + 1) % 100 == 0:
                self.save_intermediate_results(i + 1)
    
    def save_intermediate_results(self, row_count):
        """
        Save intermediate results after processing a certain number of rows.
        
        Args:
            row_count (int): Number of rows processed so far
        """
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_analysis_intermediate_{row_count}_{timestamp}.xlsx"
        self.df.to_excel(output_path, index=False)
        print(f"Intermediate results saved to {output_path} after processing {row_count} rows")
    
    def save_results(self):
        """
        Save analysis results back to Excel with a timestamp.
        """
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/data/output/eudr_analysis_{timestamp}.xlsx"
        self.df.to_excel(output_path, index=False)
        print(f"Results saved to {output_path}")
        
    def process_single_text(self, text):
        """
        Process a single text input for testing purposes.
        
        Args:
            text (str): The text to analyze.
            
        Returns:
            dict: The analysis result.
        """
        print("Processing single text input for testing...")
        
        # Extract paragraphs from XML tags
        paragraphs = self.extract_paragraph_text(text)
        
        # If no paragraphs found, use the whole text
        if not paragraphs:
            paragraphs = [text]
        
        # Analyze all extracted paragraphs
        analyses = []
        for paragraph in paragraphs:
            analysis = self.analyze_text_for_cost_drivers(paragraph)
            analyses.append(analysis)
            print("Result:")
            print(json.dumps(analysis, indent=2))
        
        return analyses

# Example usage
if __name__ == "__main__":
    analyzer = EUDRCostAnalyzer()
    
    # Uncomment to process a single text for testing
    test_text = """
    Some irrelevant text
    <paragraph>Article 9 - Due diligence statement
    1. Relevant operators shall make a due diligence statement available to the competent authorities via the information system.</paragraph>
    <paragraph>2. The due diligence statement shall contain documentation, stating that the due diligence has been carried out.</paragraph>
    """
    analyzer.process_single_text(test_text)
    
    # Process all rows from the Excel file
    analyzer.process_all_rows()
    analyzer.save_results()