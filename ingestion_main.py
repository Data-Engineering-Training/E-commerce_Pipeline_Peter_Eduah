import pandas as pd
import json
import os

def load_csv(file_path, expected_columns=None):
    """
    Load data from a CSV file into a pandas DataFrame.
    
    Parameters:
        file_path (str): Path to the CSV file.
        expected_columns (list): List of expected column names (optional).
        
    Returns:
        DataFrame: The loaded data as a pandas DataFrame.
    """
    try:
        # Load CSV file
        df = pd.read_csv(file_path)
    except pd.errors.ParserError as e:
        print(f"Error loading CSV file: {e}")
        return None
    
    # Check for schema evolution
    if expected_columns:
        # Compare expected columns with actual columns in DataFrame
        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            print(f"Warning: Missing columns in CSV file {file_path}: {missing_columns}")
            # Handle missing columns (e.g., fill with None, default values)
    
    return df

def load_json(file_path, expected_keys=None):
    """
    Load data from a JSON file into a dictionary.
    
    Parameters:
        file_path (str): Path to the JSON file.
        expected_keys (list): List of expected keys (optional).
        
    Returns:
        dict: The loaded data as a dictionary.
    """
    try:
        # Load JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)
    except json.JSONDecodeError as e:
        print(f"Error loading JSON file: {e}")
        return None
    
    # Check for schema evolution
    if expected_keys:
        # Compare expected keys with keys in loaded JSON data
        missing_keys = set(expected_keys) - set(data.keys())
        if missing_keys:
            print(f"Warning: Missing keys in JSON file {file_path}: {missing_keys}")
            # Handle missing keys (e.g., add default values)
    
    return data

def process_market_data(data_dir):
    """
    Process market-specific CSV and JSON files in a directory.
    
    Parameters:
        data_dir (str): Directory containing the data files.
    """
    # List of CSV and JSON files in the data directory
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    json_files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    
    # Group CSV files by market
    csv_files_by_market = {}
    for csv_file in csv_files:
        market_name = csv_file.split()[0]  # Extract market name from file name
        if market_name not in csv_files_by_market:
            csv_files_by_market[market_name] = []
        csv_files_by_market[market_name].append(csv_file)
    
    # Process each market's data
    for market_name, csv_files in csv_files_by_market.items():
        # Load customer JSON file
        customer_json_file = f"{market_name} customer.json"
        customer_json_path = os.path.join(data_dir, customer_json_file)
        customer_data = load_json(customer_json_path)
        if customer_data is not None:
            print(f"Loaded {customer_json_file}")
            # Process customer data as needed
        
        # Load orders CSV file
        orders_csv_file = f"{market_name} orders.csv"
        orders_csv_path = os.path.join(data_dir, orders_csv_file)
        orders_df = load_csv(orders_csv_path)
        if orders_df is not None:
            print(f"Loaded {orders_csv_file}")
            # Process orders data as needed
        
        # Load deliveries CSV file
        deliveries_csv_file = f"{market_name} Deliveries.csv"
        deliveries_csv_path = os.path.join(data_dir, deliveries_csv_file)
        deliveries_df = load_csv(deliveries_csv_path)
        if deliveries_df is not None:
            print(f"Loaded {deliveries_csv_file}")
            # Process deliveries data as needed

# Example usage:
if __name__ == "__main__":
    data_directory = 'data'  # Directory containing CSV and JSON files
    
    # Process market-specific data files
    process_market_data(data_directory)
