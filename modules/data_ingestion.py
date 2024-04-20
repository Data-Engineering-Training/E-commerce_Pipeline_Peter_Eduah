import pandas as pd
import json
import os
from datetime import datetime

def load_csv(file_path, last_execution_timestamp=None):
    data = pd.read_csv(file_path)
    
    if last_execution_timestamp:
        # Filter records based on the timestamp
        data = data[pd.to_datetime(data['Created At']) > last_execution_timestamp]
    
    data_list = data.to_dict(orient='records')
    return data_list

def load_json(file_path, last_execution_timestamp=None):
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
        
        if last_execution_timestamp:
            # Filter records based on the timestamp
            data = [record for record in data if datetime.strptime(record['timestamp_field'], '%Y-%m-%d %H:%M:%S') > last_execution_timestamp]
        
        return data

def ingest_data(file_path, last_execution_timestamp=None):
    file_extension = os.path.splitext(file_path)[1]
    if file_extension == '.csv':
        data = load_csv(file_path, last_execution_timestamp)
    elif file_extension == '.json':
        data = load_json(file_path, last_execution_timestamp)
    else:
        raise ValueError("Unsupported file format")

    return data

def main():
    # Example usage
    last_execution_timestamp = datetime.strptime("2024-04-12 00:00:00", "%Y-%m-%d %H:%M:%S")
    
    customer_data = ingest_data('data_dir/Market 1 Customers.json', last_execution_timestamp)
    orders_data = ingest_data('data_dir/Market 1 Orders.csv', last_execution_timestamp)
    deliveries_data = ingest_data('data_dir/Market 1 Deliveries.csv', last_execution_timestamp)

    # Process the new or modified data here
    
    # Update last_execution_timestamp to the current time after successful processing
    last_execution_timestamp = datetime.now()

if __name__ == "__main__":
    main()
