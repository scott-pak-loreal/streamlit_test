import pandas as pd
from google.cloud import bigquery

# Define Bigquery main function
def get_table_from_query(query, project_id):
    """
    Executes a query in BigQuery and returns the result as a Pandas DataFrame.

    Args:
        query (str): The SQL query to execute.
        project_id (str): Your Google Cloud project ID.

    Returns:
        pd.DataFrame: The query result as a Pandas DataFrame.
    """
    try:
        # Initialize the BigQuery client
        client = bigquery.Client(project=project_id)

        # Execute the query
        query_job = client.query(query)

        # Wait for the query to complete and fetch the result
        result = query_job.result()

        # Convert the result to a Pandas DataFrame
        df = result.to_dataframe()

        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None

# Main execution
if __name__ == "__main__":
    # Project ID
    project_id = 'amer-mediadata-us-amer-pd'
    
    # SQL query
    query = """
        SELECT * FROM `amer-mediadata-us-amer-pd.Pubco.BCL_PUBCO_data_v2` LIMIT 1000
    """
    
    print("Executing BigQuery...")
    
    # Option 1: Using the function you defined
    df = get_table_from_query(query, project_id)
    
    # Option 2: Direct execution (your current approach - commented out)
    # client = bigquery.Client(project=project_id)
    # query_job = client.query(query)
    # result = query_job.result()
    # df = result.to_dataframe()
    
    # Check if query was successful
    if df is not None:
        print(f"Query executed successfully!")
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print("\n" + "="*50)
        print("First 5 rows of data:")
        print("="*50)
        print(df.head())
        
        print("\n" + "="*50)
        print("Data types:")
        print("="*50)
        print(df.dtypes)
        
        print("\n" + "="*50)
        print("Basic statistics (for numeric columns):")
        print("="*50)
        print(df.describe())
        
    else:
        print("Failed to retrieve data from BigQuery.")