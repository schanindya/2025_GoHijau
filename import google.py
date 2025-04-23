from google import genai
from google.genai import types
import google.generativeai as genai
import pandas as pd
import os
import time
from tqdm import tqdm # Optional: for a progress bar
import sys # To check for libraries

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
EUDR_PROCESS_FILE = '/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/EUDR_PROCESS_input.xlsx'
EUDR_PROCESS_SHEET = 'Sheet1'
COST_DRIVER_FILE = '/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/cost_drivers_expanded.xlsx'
COST_DRIVER_SHEET = 'Sheet1' # Assuming cost drivers are also on Sheet1
OUTPUT_FILE = '/Users/schalkeanindya/Desktop/PROSPERA/GoHijau/2025_GoHijau/EUDR_PROCESS_output_all.xlsx' # Output file name
OUTPUT_SHEET_NAME = 'Processed_Results' # Name for the sheet in the output file

# --- Column Name Configuration ---
# Column in EUDR_PROCESS_FILE containing the process descriptions to analyze
EUDR_PROCESS_INPUT_COLUMN = 'Process' # *** ADJUST THIS if your process description column has a different name ***

# Columns in COST_DRIVER_FILE
COST_DRIVER_UNIVERSE_COLUMN = 'cost_driver'
REFERENCE_COLUMN = 'document_name'
OUTPUT_CONTENT_COLUMN = 'output_content'
CITATIONS_COLUMN = 'citations'
TEXT_COLUMN = 'text'                       # <-- New input column name for reference text

# --- Output Column Names ---
# These columns will be ADDED or OVERWRITTEN in the output file
OUTPUT_COST_DRIVER_COLUMN = 'Cost Driver'
OUTPUT_REFERENCE_COLUMN = 'Reference on Cost Driver'
OUTPUT_OUTPUT_CONTENT_COLUMN = 'Output Content'
OUTPUT_CITATIONS_COLUMN = 'Citations'
OUTPUT_REFERENCE_TEXT_COLUMN = 'citations from reference' # <-- New output column name

# --- API Settings ---
API_DELAY_SECONDS = 1.5 # Adjust as needed
API_TIMEOUT_SECONDS = 120 # How long to wait for an API response
TEXT_SEPARATOR = "\n\n---\n\n" # Separator for multiple text paragraphs
DETAIL_SEPARATOR = "; " # Separator for joining unique references/content/citations
# --------------------

# --- Input Validation and Setup ---
if not API_KEY:
    raise ValueError("Please set the GOOGLE_API_KEY environment variable.")

genai.configure(api_key=API_KEY)

# Check if input files exist
if not os.path.exists(EUDR_PROCESS_FILE):
     raise FileNotFoundError(f"EUDR process file not found: {EUDR_PROCESS_FILE}")
if not os.path.exists(COST_DRIVER_FILE):
     raise FileNotFoundError(f"Cost driver file not found: {COST_DRIVER_FILE}")

print(f"Loading EUDR processes from: {EUDR_PROCESS_FILE} (Sheet: {EUDR_PROCESS_SHEET})")
try:
    df_processes = pd.read_excel(EUDR_PROCESS_FILE, sheet_name=EUDR_PROCESS_SHEET)
    if EUDR_PROCESS_INPUT_COLUMN not in df_processes.columns:
        raise ValueError(f"Column '{EUDR_PROCESS_INPUT_COLUMN}' not found in {EUDR_PROCESS_FILE} (Sheet: {EUDR_PROCESS_SHEET}). Please adjust 'EUDR_PROCESS_INPUT_COLUMN' in the script.")
except Exception as e:
    print(f"Error loading {EUDR_PROCESS_FILE}: {e}")
    sys.exit(1)

# Process all rows in the input file
# df_processes = df_processes.head(10)  # Commented out the testing limitation
print(f"Processing all {len(df_processes)} rows of the input file.")

print(f"Loading Cost Drivers from: {COST_DRIVER_FILE} (Sheet: {COST_DRIVER_SHEET})")
try:
    df_cost_drivers = pd.read_excel(COST_DRIVER_FILE, sheet_name=COST_DRIVER_SHEET)
    # Check all required columns from the cost driver file
    required_driver_cols = [
        COST_DRIVER_UNIVERSE_COLUMN, REFERENCE_COLUMN,
        OUTPUT_CONTENT_COLUMN, CITATIONS_COLUMN, TEXT_COLUMN # Added TEXT_COLUMN
    ]
    for col in required_driver_cols:
        if col not in df_cost_drivers.columns:
            raise ValueError(f"Column '{col}' not found in {COST_DRIVER_FILE} (Sheet: {COST_DRIVER_SHEET}).")
except Exception as e:
    print(f"Error loading {COST_DRIVER_FILE}: {e}")
    sys.exit(1)

# Prepare cost driver data
# Ensure no NaN values interfere (check all needed columns from cost driver file)
df_cost_drivers = df_cost_drivers.dropna(subset=required_driver_cols)
cost_driver_list = df_cost_drivers[COST_DRIVER_UNIVERSE_COLUMN].astype(str).unique().tolist() # Get unique drivers for the prompt

# Create a dictionary mapping: Cost Driver Text -> LIST of {ref:, content:, citations:, text:} dictionaries
cost_driver_lookup_dict = {}
for _, row in df_cost_drivers.iterrows():
    driver = str(row[COST_DRIVER_UNIVERSE_COLUMN])
    details = {
        'reference': str(row[REFERENCE_COLUMN]),
        'content': str(row[OUTPUT_CONTENT_COLUMN]),
        'citations': str(row[CITATIONS_COLUMN]),
        'text': str(row[TEXT_COLUMN]) # <-- Store the text paragraph
    }
    if driver not in cost_driver_lookup_dict:
        cost_driver_lookup_dict[driver] = [] # Initialize with an empty list
    cost_driver_lookup_dict[driver].append(details) # Append the details dict for this row

print(f"Loaded {len(df_processes)} EUDR processes.")
print(f"Loaded {len(cost_driver_list)} unique potential cost drivers.")
print(f"Processed {len(df_cost_drivers)} total cost driver entries with details.")
if not cost_driver_list:
    print("Warning: No cost drivers loaded. Check the cost driver file and sheet.")

# --- Gemini Model Interaction ---
model = genai.GenerativeModel(MODEL_NAME)
generation_config = genai.GenerationConfig() # Keep default for now
safety_settings = [
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
    # ... other settings ...
]

def build_prompt(eudr_process_text, available_cost_drivers):
    """Creates the prompt for the Gemini model."""
    # Use the unique list of drivers for the prompt
    driver_list_str = "\n".join([f"- {driver}" for driver in available_cost_drivers])
    prompt = f"""
    Analyze the following EUDR Process Description:
    \"\"\"
    {eudr_process_text}
    \"\"\"

    Now, review the following list of potential Cost Drivers:
    --- START COST DRIVER LIST ---
    {driver_list_str}
    --- END COST DRIVER LIST ---

    Your task is to select the *single most relevant* Cost Driver from the provided list that directly corresponds to the EUDR Process Description.

    **Output Rules:**
    1.  Choose ONLY ONE cost driver from the list.
    2.  Output the EXACT text of the selected Cost Driver as it appears in the list.
    3.  Do NOT include any explanation, commentary, or extra text before or after the selected cost driver. Just the cost driver text itself.

    Selected Cost Driver:"""
    return prompt

def get_gemini_match(prompt_text):
    """Sends prompt to Gemini and attempts to parse the best match."""
    # (Error handling remains the same as before)
    try:
        response = model.generate_content(
            prompt_text,
            generation_config=generation_config,
            safety_settings=safety_settings,
            request_options={'timeout': API_TIMEOUT_SECONDS}
            )
        if response.parts:
             match = response.text.strip().strip('"').strip("'").strip()
             if match.startswith("- "):
                 match = match[2:]
             return match
        elif response.prompt_feedback and response.prompt_feedback.block_reason:
             print(f"  WARN: Prompt blocked. Reason: {response.prompt_feedback.block_reason}")
             return "BLOCKED_BY_SAFETY"
        else:
             if response.candidates and response.candidates[0].finish_reason.name != "STOP":
                 print(f"  WARN: Generation stopped. Reason: {response.candidates[0].finish_reason.name}")
                 return f"GENERATION_STOPPED_{response.candidates[0].finish_reason.name}"
             print(f"  WARN: Received empty response or unexpected format: {response}")
             return "NO_RESPONSE"
    except Exception as e:
        print(f"  ERROR: API call failed: {e}")
        # ... (rest of error handling) ...
        return "API_ERROR"

# --- Process Data ---
results_driver = []
results_reference = []
results_output_content = []
results_citations = []
results_reference_text = [] # <-- New list for combined reference text

print("\nStarting EUDR Process analysis using Gemini...")
for index, row in tqdm(df_processes.iterrows(), total=df_processes.shape[0], desc="Processing"):
    process_text = str(row[EUDR_PROCESS_INPUT_COLUMN]) if pd.notna(row[EUDR_PROCESS_INPUT_COLUMN]) else ""

    # Set default values for all output columns for this row
    matched_driver_text = "SKIPPED_EMPTY_PROCESS"
    final_driver = "SKIPPED_EMPTY_PROCESS"
    final_reference = "N/A"
    final_output_content = "N/A"
    final_citations = "N/A"
    final_reference_text = "N/A" # <-- Default for new column

    if process_text and cost_driver_list: # Only process if there's text and drivers exist
        print(f"\nProcessing row {index+1}/{len(df_processes)}: '{process_text[:100]}...'")

        # Pass the unique list of drivers to the prompt function
        prompt = build_prompt(process_text, cost_driver_list)
        matched_driver_text = get_gemini_match(prompt)

        if matched_driver_text not in ["API_ERROR", "NO_RESPONSE", "BLOCKED_BY_SAFETY"] and not matched_driver_text.startswith("GENERATION_STOPPED"):
            # Use the lookup dictionary which maps to a LIST of details
            if matched_driver_text in cost_driver_lookup_dict:
                details_list = cost_driver_lookup_dict[matched_driver_text] # Get the list of dicts

                # Aggregate details from all entries for this driver
                aggregated_references = set() # Use set for unique values
                aggregated_content = set()
                aggregated_citations = set()
                aggregated_text = [] # Use list to keep all paragraphs

                for details in details_list:
                    aggregated_references.add(details['reference'])
                    aggregated_content.add(details['content'])
                    aggregated_citations.add(details['citations'])
                    aggregated_text.append(details['text']) # Append each text paragraph

                # Assign the matched driver text
                final_driver = matched_driver_text

                # Join the aggregated details into strings
                final_reference = DETAIL_SEPARATOR.join(sorted(list(aggregated_references)))
                final_output_content = DETAIL_SEPARATOR.join(sorted(list(aggregated_content)))
                final_citations = DETAIL_SEPARATOR.join(sorted(list(aggregated_citations)))
                final_reference_text = TEXT_SEPARATOR.join(aggregated_text) # Join all text paragraphs

                print(f"  SUCCESS: Matched '{final_driver}' -> Found {len(details_list)} source row(s).")
            else:
                # Handle cases where Gemini response not exactly in list
                print(f"  WARN: Gemini response '{matched_driver_text}' not found exactly in the cost driver list.")
                final_driver = f"MATCH_FAILED ({matched_driver_text})"
                # Keep N/A for other fields
        else: # Handle API errors / No response / Blocked / Stopped
            final_driver = matched_driver_text # Store the error/status code
            # Keep N/A for other fields
            print(f"  FAILED: Reason: {matched_driver_text}")

    elif not process_text:
         print(f"\nSkipping row {index+1}/{len(df_processes)}: Empty process description.")
         # Defaults are already set
    elif not cost_driver_list:
         print(f"\nSkipping row {index+1}/{len(df_processes)}: No cost drivers loaded.")
         final_driver = "SKIPPED_NO_DRIVERS"
         # Defaults are already set

    # Append results for this row (including defaults if skipped/failed)
    results_driver.append(final_driver)
    results_reference.append(final_reference)
    results_output_content.append(final_output_content)
    results_citations.append(final_citations)
    results_reference_text.append(final_reference_text) # <-- Append combined text

    # Pause between API calls only if an attempt was made
    if process_text and cost_driver_list:
        time.sleep(API_DELAY_SECONDS)

# Add results to the DataFrame (overwriting if columns exist)
df_processes[OUTPUT_COST_DRIVER_COLUMN] = results_driver
df_processes[OUTPUT_REFERENCE_COLUMN] = results_reference
df_processes[OUTPUT_OUTPUT_CONTENT_COLUMN] = results_output_content
df_processes[OUTPUT_CITATIONS_COLUMN] = results_citations
df_processes[OUTPUT_REFERENCE_TEXT_COLUMN] = results_reference_text # <-- Add new column

# --- Save Results ---
print(f"\nSaving results to: {OUTPUT_FILE} (Sheet: {OUTPUT_SHEET_NAME})")
try:
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        df_processes.to_excel(writer, sheet_name=OUTPUT_SHEET_NAME, index=False)
    print("Processing complete. Output file saved.")
except Exception as e:
    print(f"Error saving output file: {e}")

# Display some results (optional)
print("\nSample of results:")
print(df_processes[[
    EUDR_PROCESS_INPUT_COLUMN,
    OUTPUT_COST_DRIVER_COLUMN,
    OUTPUT_REFERENCE_COLUMN,
    OUTPUT_OUTPUT_CONTENT_COLUMN,
    OUTPUT_CITATIONS_COLUMN,
    OUTPUT_REFERENCE_TEXT_COLUMN # <-- Show new column
]].head())