import os
import pandas as pd
from datetime import datetime
from dune_client.client import DuneClient
import requests
import json
from dotenv import load_dotenv 

# Add debug prints for path checking
print("Current working directory:", os.getcwd())
print("Script directory:", os.path.dirname(os.path.abspath(__file__)))
print(".env file exists:", os.path.exists('.env'))

# Load environment variables from .env file
# Try with explicit path
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
print("Looking for .env at:", dotenv_path)
print(".env file exists at path:", os.path.exists(dotenv_path))
load_dotenv(dotenv_path)

def fetch_validator_data():
    """Fetch validator queue data from remote URL and return a cleaned DataFrame."""
    import requests
    import pandas as pd

    # Remote URL containing the JSON data
    url = "https://raw.githubusercontent.com/etheralpha/validatorqueue-com/refs/heads/main/historical_data.json"
    print("Fetching validator queue data from remote URL:", url)
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error if the request failed
    except Exception as e:
        print("Error fetching data:", e)
        raise e

    # Load the JSON data
    data = response.json()
    df = pd.DataFrame(data)
    print("Successfully loaded data into DataFrame with shape:", df.shape)

    # Convert specific columns to numeric if they exist
    cols_to_numeric = [
        "validators", "entry_queue", "entry_wait", "exit_queue", "exit_wait",
        "churn", "entry_churn", "exit_churn", "supply", "staked_amount", "staked_percent", "apr"
    ]
    for col in cols_to_numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            print(f"Warning: Column '{col}' not found in DataFrame.")

    # Drop rows with missing values in the numeric columns
    before_drop = df.shape[0]
    df.dropna(subset=cols_to_numeric, inplace=True)
    after_drop = df.shape[0]
    print(f"Dropped {before_drop - after_drop} rows based on columns: {cols_to_numeric}")
    print("DataFrame shape after cleaning:", df.shape)

    return df

def upload_to_dune(df):
    """Upload DataFrame to Dune Analytics"""
    dune = DuneClient.from_env()
    
    # Convert DataFrame to CSV string
    csv_data = df.to_csv(index=False)
    
    # Upload to Dune
    result = dune.upload_csv(
        data=csv_data,
        description="Ethereum validator queue metrics, sourced from validatorqueue.com",
        table_name="eth_validator_queue_metrics",
        is_private=False
    )
    
    # Check the type of the returned result before accessing attributes
    if isinstance(result, bool):
        if result:
            # Upload succeeded and returned a boolean True
            print("Successfully uploaded data to Dune (upload method returned True).")
        else:
            # Upload failed and returned False
            print("Failed to upload data to Dune (upload method returned False).")
    elif hasattr(result, "table_name"):
        print(f"Successfully uploaded data to Dune. Table name: {result.table_name}")
    else:
        # In case the result isn't a bool and doesn't have table_name attribute
        print("Upload completed, but the result is not in the expected format.")

    return result

def main():
    try:
        # Optional: Print to verify the DUNE_API_KEY is loaded
        print("DUNE_API_KEY =", os.getenv("DUNE_API_KEY"))
        
        # Fetch data
        print("Loading validator queue data...")
        df = fetch_validator_data()
        
        # Upload to Dune
        print("Uploading data to Dune...")
        table = upload_to_dune(df)
        
        print("Process completed successfully!")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise e

if __name__ == "__main__":
    main() 