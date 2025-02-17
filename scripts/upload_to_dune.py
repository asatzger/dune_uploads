import os
import pandas as pd
from datetime import datetime, timedelta, timezone
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
        "supply", "staked_amount", "staked_percent", "apr"
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

def verify_recent_data(df):
    """Verify that the data contains entries from the past week."""
    # Look for a date field: first "timestamp", then "date"
    date_column = None
    if "timestamp" in df.columns:
        date_column = "timestamp"
    elif "date" in df.columns:
        date_column = "date"

    if not date_column:
        print("Verification skipped: No date column found in DataFrame for recent data check.")
        return

    # Convert the column to datetime objects
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    latest_date = df[date_column].max()
    
    if pd.isna(latest_date):
        raise ValueError(f"Verification failed: Unable to determine the latest date from '{date_column}' column.")

    # Ensure latest_date is timezone-aware. If it's naive, assume UTC.
    if latest_date.tzinfo is None:
        latest_date = latest_date.replace(tzinfo=timezone.utc)
    
    one_week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    
    if latest_date < one_week_ago:
        raise ValueError(f"Verification failed: Data is outdated. Latest date in data is {latest_date}, which is older than one week (threshold: {one_week_ago}).")
    
    print(f"Data verification passed: Latest date in data is {latest_date}.")

def clean_validator_data(df):
    """
    Clean up and normalize the validator data DataFrame.
    """
    # Verification: Print initial row count
    initial_rows = len(df)
    print(f"\nInitial row count: {initial_rows}")
    print("Columns before cleaning:", df.columns.tolist())
    
    # List of columns required for a valid row - only include the essential ones
    required_cols = [
        "validators", "entry_queue", "entry_wait",
        "exit_queue", "exit_wait",
        "supply", "staked_amount", "staked_percent", "apr"
    ]
    
    # Debug: Print sample of data before dropping
    print("\nSample row before dropping:")
    print(df.iloc[-1])
    
    before = len(df)
    df = df.dropna(subset=required_cols)
    dropped = before - len(df)
    print(f"\nDropped {dropped} rows based on columns: {required_cols}")
    
    # Verification: Print final row count and percentage retained
    final_rows = len(df)
    retention_rate = (final_rows / initial_rows) * 100 if initial_rows > 0 else 0
    print(f"Final row count: {final_rows}")
    print(f"Retention rate: {retention_rate:.1f}%")
    
    # Debug: Print columns after cleaning
    print("Columns after cleaning:", df.columns.tolist())
    print("Latest date in cleaned data:", df['date'].max())
    print("Earliest date in cleaned data:", df['date'].min())
    
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
        
        # Verify that data includes entries from the past week
        print("Verifying that data includes entries from the past week...")
        try:
            verify_recent_data(df)
        except ValueError as err:
            # Allow manual override via FORCE_UPLOAD environment variable.
            force_upload = os.getenv("FORCE_UPLOAD", "false").lower() in ["true", "1", "yes"]
            if force_upload:
                print(f"Warning: {err} Proceeding with upload because FORCE_UPLOAD is enabled.")
            else:
                raise
        
        # Clean data
        print("Cleaning validator queue data...")
        df = clean_validator_data(df)
        
        # Upload to Dune
        print("Uploading data to Dune...")
        table = upload_to_dune(df)
        
        print("Process completed successfully!")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise e

if __name__ == "__main__":
    main() 