import pandas as pd
from google.cloud import bigquery

# Load your dataframe
df = pd.read_excel('streamlit_testdata.xlsx', engine='openpyxl')

# BigQuery authentication
client = bigquery.Client()

#Update month and year accordingly
month = "04"
year = "2025"

# BigQuery table settings
table_id = f"project.dataset.table_{year}_{month}"

print(table_id)

# Schema generation from Dataframe
schema = [
    bigquery.SchemaField(name, dtype.name.upper(), mode="NULLABLE")
    for name, dtype in df.dtypes.items()
]

# data type mapping (pandas to BigQuery)
dtype_mapping = {
    "int64": "INTEGER",
    "int32": "INTEGER",
    "float64": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "DATE",
    "object": "STRING",
}

# Replace data type using mapping dictionary
schema = [
    bigquery.SchemaField(name, dtype_mapping[dtype.name], mode="NULLABLE")
    for name, dtype in df.dtypes.items()
]

# upload DataFrame to BigQuery
job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")

job = client.load_table_from_dataframe(df, table_id, job_config=job_config)