from google import genai
from google.genai import types
import google.generativeai as genai
import pandas as pd
import os
import time
from tqdm import tqdm # Optional: for a progress bar
import sys # To check for libraries
import json
import ast  # <--- ADD THIS LINE
import re   # <--- ENSURE THIS LINE IS PRESENT AND NOT COMMENTED OUT

# --- Configuration ---
# Check for necessary Excel library
try:
    import openpyxl
except ImportError:
    print("Error: The 'openpyxl' library is required to read/write .xlsx files.")
    print("Please install it using: pip install openpyxl")
    sys.exit(1)

API_KEY = "AIzaSyDmCOxNyllz_Aw_ER0rwpl1dRGpSBdu0Rw"

if not API_KEY:
    raise ValueError("Please set the GOOGLE_API_KEY environment variable.")

genai.configure(api_key=API_KEY) # CORRECT WAY TO SET THE KEY

MODEL_NAME = "gemini-2.5-pro-preview-03-25" # Double-check the latest available model name if needed

# --- File and Sheet Configuration ---
INPUT_FILE = '/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/EUDR_PROCESS_output_all.xlsx' # The output file from the PREVIOUS step
INPUT_SHEET = 'Processed_Results'       # The sheet name from the PREVIOUS step's output
FINAL_OUTPUT_FILE = '/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/EUDR_PROCESS_FINAL_ANALYSIS_all.xlsx' # New output file name
FINAL_OUTPUT_SHEET = 'Cost_Inference_Results'

# --- Column Name Configuration ---
# Input columns from INPUT_FILE needed for analysis and context
OUTPUT_CONTENT_TO_ANALYZE_COLUMN = 'Output Content' # Text to analyze
CONTEXT_COST_DRIVER_COLUMN = 'Cost Driver'          # Context
CONTEXT_ROLES_COLUMN = 'Roles'                      # Context - **ENSURE THIS COLUMN EXISTS**
CONTEXT_STAGE_COLUMN = 'Stage'                      # Context - **ENSURE THIS COLUMN EXISTS**
CONTEXT_PROCESS_COLUMN = 'Process'             # Context - **ENSURE THIS COLUMN EXISTS (Original Process Desc)**
# This column contains the list-like string of URLs, e.g., "['url1', 'url2']"
# It's needed for the *final mapping step*, not the AI call itself.
SOURCE_CITATIONS_LIST_COLUMN = 'Citations'          # **ENSURE THIS COLUMN EXISTS**

# New output columns created by AI inference
NEW_NOMINAL_COST_COLUMN = 'Inferred Nominal Cost'
NEW_COST_IMPACT_COLUMN = 'Inferred Cost Impact'
NEW_COST_TYPE_COLUMN = 'Inferred Cost Type'
NEW_NOMINAL_COST_REF_COLUMN = 'reference of nominal value' # Citation marker (e.g., "[3][7]")

# New column created by post-processing (mapping markers to URLs)
MAPPED_NOMINAL_COST_URLS_COLUMN = 'Mapped Nominal Cost Citations' # <-- New final column

# --- API Settings ---
API_DELAY_SECONDS = 1.5 # Adjust as needed
API_TIMEOUT_SECONDS = 120 # How long to wait for an API response
# --------------------

# --- Input Validation and Setup ---
if not API_KEY:
    raise ValueError("Please set the GOOGLE_API_KEY environment variable.")

genai.configure(api_key=API_KEY)

# Check if input file exists
if not os.path.exists(INPUT_FILE):
     raise FileNotFoundError(f"Input file not found: {INPUT_FILE}. Make sure the output from the previous script exists.")

print(f"Loading data from: {INPUT_FILE} (Sheet: {INPUT_SHEET})")
try:
    df = pd.read_excel(INPUT_FILE, sheet_name=INPUT_SHEET)
    # Check if all required input columns exist for AI + Mapping
    required_input_cols = [
        OUTPUT_CONTENT_TO_ANALYZE_COLUMN,
        CONTEXT_COST_DRIVER_COLUMN,
        CONTEXT_ROLES_COLUMN,
        CONTEXT_STAGE_COLUMN,
        CONTEXT_PROCESS_COLUMN,
        SOURCE_CITATIONS_LIST_COLUMN # Needed for final mapping
    ]
    missing_cols = [col for col in required_input_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in {INPUT_FILE} (Sheet: {INPUT_SHEET}): {', '.join(missing_cols)}. Please ensure these columns exist.")

except Exception as e:
    print(f"Error loading {INPUT_FILE}: {e}")
    sys.exit(1)


print(f"Loaded {len(df)} rows for cost inference.")

# --- Gemini Model Interaction ---
model = genai.GenerativeModel(MODEL_NAME)
generation_config = genai.GenerationConfig(
    response_mime_type="application/json",
    temperature=0
)
safety_settings = [
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
    # ... other settings ...
]

# --- Prompt Function (No change needed here from previous version) ---
def build_cost_inference_prompt(output_content_text, cost_driver, roles, stage, process):
    """Creates the prompt for Gemini cost inference."""
    # (Keep the prompt from the previous version - asking for the marker "[x][y]")
    cost_driver = str(cost_driver) if pd.notna(cost_driver) else "N/A"
    roles = str(roles) if pd.notna(roles) else "N/A"
    stage = str(stage) if pd.notna(stage) else "N/A"
    process = str(process) if pd.notna(process) else "N/A"

    prompt = f"""
    Analyze the following 'Output Content' text to extract cost information, using the provided context.

    **Output Content to Analyze:**
    \"\"\"
    {output_content_text}
    \"\"\"

    **Context for Relevance (Use this to guide your extraction):**
    *   **Cost Driver:** {cost_driver}
    *   **Company Role(s):** {roles}
    *   **Process Stage:** {stage}
    *   **Specific Process:** {process}

    **Your Task:**
    Extract the following information from the 'Output Content', ensuring it is **directly relevant** to the provided Context (Cost Driver, Roles, Stage, Process). If multiple costs are mentioned, prioritize the one most applicable to the context.

    1.  **Nominal Cost:** Identify any specific monetary values, ranges, percentages, or quantitative cost figures mentioned (e.g., "€10,000–€150,000 annually", "0.1% of annual revenues", "$500 per audit"). If no relevant nominal cost is found, use "N/A".
    2.  **Cost Impact:** Describe the qualitative impact or level of the cost (e.g., "Medium to high", "Disproportionate burdens for smaller exporters", "Significant operational adjustment"). If no relevant impact description is found, use "N/A".
    3.  **Cost Type:** Categorize the type of cost described (e.g., "Software licensing", "Data collection", "Staff training", "Operational", "Compliance Setup", "Audit Fees"). If no relevant cost type can be clearly identified, use "N/A".
    4.  **Nominal Cost Citation:** Identify the citation marker (e.g., "[1]", "[3][7]") found *within the 'Output Content' text* that is directly associated with the extracted Nominal Cost value from step 1. Look for the marker immediately following or very close to the cost figure. If the nominal cost itself is "N/A" or has no clear citation marker nearby or associated with it in the text, use "N/A".

    **Output Format:**
    Return your answer ONLY as a valid JSON object with the following keys: "nominal_cost", "cost_impact", "cost_type", "nominal_cost_citation". Use "N/A" as the string value for any field where relevant information cannot be extracted based on the text and the provided context.

    Example Output 1:
    {{
      "nominal_cost": "€10,000–€150,000 annually",
      "cost_impact": "Medium to high",
      "cost_type": "Software licensing",
      "nominal_cost_citation": "[3][7]"
    }}
    """
    return prompt

def get_gemini_cost_inference(prompt_text):
    """Sends prompt to Gemini and attempts to parse the JSON response."""
    # (Error handling remains the same as before)
    try:
        response = model.generate_content(
            prompt_text,
            generation_config=generation_config,
            safety_settings=safety_settings,
            request_options={'timeout': API_TIMEOUT_SECONDS}
        )
        # ... (rest of the function is identical to the previous version) ...
        if response.parts:
            try:
                cost_data = json.loads(response.text)
                if isinstance(cost_data, dict):
                    return cost_data
                else:
                    return {"error": "INVALID_JSON_STRUCTURE", "raw_text": response.text}
            except json.JSONDecodeError as json_err:
                return {"error": "JSON_DECODE_ERROR", "raw_text": response.text}
            except Exception as e:
                 return {"error": "JSON_PROCESSING_ERROR", "raw_text": response.text}
        elif response.prompt_feedback and response.prompt_feedback.block_reason:
             return {"error": "BLOCKED_BY_SAFETY"}
        else:
             if response.candidates and response.candidates[0].finish_reason.name != "STOP":
                 reason = response.candidates[0].finish_reason.name
                 return {"error": f"GENERATION_STOPPED_{reason}"}
             return {"error": "NO_RESPONSE"}
    except Exception as e:
        return {"error": "API_ERROR"}


# --- Process Data (AI Inference) ---
results_nominal = []
results_impact = []
results_type = []
results_nominal_ref_marker = [] # Store the marker like "[3][7]"

print("\nStarting cost inference using Gemini...")
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Inferring Costs"):
    # Extract data for the current row
    output_content = row[OUTPUT_CONTENT_TO_ANALYZE_COLUMN]
    context_driver = row[CONTEXT_COST_DRIVER_COLUMN]
    context_roles = row[CONTEXT_ROLES_COLUMN]
    context_stage = row[CONTEXT_STAGE_COLUMN]
    context_process = row[CONTEXT_PROCESS_COLUMN]

    # Set default values for this row's results
    nominal_cost = "N/A"
    cost_impact = "N/A"
    cost_type = "N/A"
    nominal_cost_ref_marker = "N/A" # Default for marker
    status = "SKIPPED"

    # Only process if 'Output Content' is not empty/NaN
    if pd.notna(output_content) and str(output_content).strip():
        print(f"\nProcessing row {index+1}/{len(df)}...")
        prompt = build_cost_inference_prompt(
            output_content, context_driver, context_roles, context_stage, context_process
        )
        inference_result = get_gemini_cost_inference(prompt)

        # Parse results
        if isinstance(inference_result, dict) and "error" not in inference_result:
            nominal_cost = inference_result.get("nominal_cost", "N/A")
            cost_impact = inference_result.get("cost_impact", "N/A")
            cost_type = inference_result.get("cost_type", "N/A")
            nominal_cost_ref_marker = inference_result.get("nominal_cost_citation", "N/A") # Get the marker "[x][y]"
            status = "Success"
            print(f"  SUCCESS: Extracted - Cost: '{nominal_cost}', Impact: '{cost_impact}', Type: '{cost_type}', Marker: '{nominal_cost_ref_marker}'")
        elif isinstance(inference_result, dict) and "error" in inference_result:
            status = f"Failed ({inference_result['error']})"
            nominal_cost = status
            cost_impact = status
            cost_type = status
            nominal_cost_ref_marker = status # Mark marker as failed too
            print(f"  FAILED: Reason: {status}")
        else:
            status = "Failed (Unknown Error)"
            nominal_cost = status
            cost_impact = status
            cost_type = status
            nominal_cost_ref_marker = status # Mark marker as failed too
            print(f"  FAILED: Unknown error state. Result: {inference_result}")

        # Pause between API calls
        time.sleep(API_DELAY_SECONDS)
    else:
        status = "Skipped (Empty Input)"
        print(f"\nSkipping row {index+1}/{len(df)}: Empty '{OUTPUT_CONTENT_TO_ANALYZE_COLUMN}'.")
        # Defaults are already N/A

    # Append results for this row
    results_nominal.append(nominal_cost)
    results_impact.append(cost_impact)
    results_type.append(cost_type)
    results_nominal_ref_marker.append(nominal_cost_ref_marker) # Append the marker


# Add AI inference results to the DataFrame
df[NEW_NOMINAL_COST_COLUMN] = results_nominal
df[NEW_COST_IMPACT_COLUMN] = results_impact
df[NEW_COST_TYPE_COLUMN] = results_type
df[NEW_NOMINAL_COST_REF_COLUMN] = results_nominal_ref_marker # Store the marker

# --- Post-Processing: Map Markers to URLs ---
print("\nMapping citation markers to URLs...")

def map_markers_to_urls(marker_string, citation_list_string):
    """Parses marker string (e.g., '[3][7]') and maps indices to URLs
       from the parsed citation_list_string (e.g., "['url1', 'url2', ...]").
    """
    if pd.isna(marker_string) or not isinstance(marker_string, str) or marker_string.strip() == "" or marker_string == "N/A" or "Failed" in marker_string:
        return "N/A"
    if pd.isna(citation_list_string) or not isinstance(citation_list_string, str) or citation_list_string.strip() == "" or citation_list_string == "N/A":
        return "N/A (Missing Citation List)"

    # 1. Parse the citation list string into a Python list
    try:
        # Use ast.literal_eval for safe evaluation of list string
        url_list = ast.literal_eval(citation_list_string)
        if not isinstance(url_list, list):
            return "N/A (Citation column not a valid list)"
    except (ValueError, SyntaxError, TypeError):
        return "N/A (Error parsing citation list)"

    # 2. Parse the marker string to find numbers
    try:
        # Find all sequences of digits in the marker string
        indices_str = re.findall(r'\d+', marker_string)
        if not indices_str:
            return f"N/A (No numbers found in marker: {marker_string})"
        # Convert to 1-based integer indices
        citation_indices = [int(i) for i in indices_str]
    except ValueError:
        return f"N/A (Error converting marker numbers: {marker_string})"
    except Exception as e:
         return f"N/A (Error parsing marker: {e})"


    # 3. Map indices to URLs
    mapped_urls = []
    list_len = len(url_list)
    for index_1based in citation_indices:
        index_0based = index_1based - 1 # Adjust for 0-based list indexing
        if 0 <= index_0based < list_len:
            mapped_urls.append(f"[{index_1based}]: {url_list[index_0based]}")
        else:
            mapped_urls.append(f"[{index_1based}]: Error - Index out of bounds (List size: {list_len})")

    return "\n".join(mapped_urls) if mapped_urls else "N/A (No valid indices mapped)"

# Apply the mapping function to create the new column
df[MAPPED_NOMINAL_COST_URLS_COLUMN] = df.apply(
    lambda row: map_markers_to_urls(
        row[NEW_NOMINAL_COST_REF_COLUMN],    # The marker string "[x][y]"
        row[SOURCE_CITATIONS_LIST_COLUMN]    # The string "['url1', 'url2', ...]"
    ),
    axis=1 # Apply function row-wise
)

print("Citation mapping complete.")

# --- Save Results ---
print(f"\nSaving final analysis results to: {FINAL_OUTPUT_FILE} (Sheet: {FINAL_OUTPUT_SHEET})")
try:
    with pd.ExcelWriter(FINAL_OUTPUT_FILE, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=FINAL_OUTPUT_SHEET, index=False)
    print("Processing complete. Final output file saved.")
except Exception as e:
    print(f"Error saving output file: {e}")

# Display some results (optional)
print("\nSample of final results:")
print(df[[
    # OUTPUT_CONTENT_TO_ANALYZE_COLUMN, # Maybe too long
    CONTEXT_COST_DRIVER_COLUMN,
    NEW_NOMINAL_COST_COLUMN,
    # NEW_COST_IMPACT_COLUMN,
    # NEW_COST_TYPE_COLUMN,
    NEW_NOMINAL_COST_REF_COLUMN,       # Show the marker "[x][y]"
    MAPPED_NOMINAL_COST_URLS_COLUMN    # Show the mapped URLs
]].head())