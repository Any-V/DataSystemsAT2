import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
from utils.datasetup import *
from utils.dimension_classes import *

def extract(self, container_name="etl-container", blob_name="Suburb_Price.xlsx"):
    print(f'Step 1: Extracting data from Azure Blob Storage')

    # Connect to Azure Blob Storage
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')  # Ensure connection string is stored in environment variables
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Get container and blob client
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    # Download the blob data
    stream = blob_client.download_blob()
    
    # Read the Excel file into a pandas DataFrame
    self.fact_table = pd.read_excel(stream.readall(), sheet_name="Sheet1")  # Modify sheet_name as needed

    print(f'Extracted {len(self.fact_table.index)} rows and {len(self.fact_table.columns)} columns from Excel file: {blob_name}')
    print(f'Step 1 finished')

    
def transform(self):
    # Convert data types
    self.fact_table[['price', 'property_size', 'suburb_median_income', 'property_inflation_index', 'km_from_cbd']] = \
        self.fact_table[['price', 'property_size', 'suburb_median_income', 'property_inflation_index', 'km_from_cbd']].astype(float)

    int_cols = ['num_bath', 'num_bed', 'num_parking']
    self.fact_table[int_cols] = self.fact_table[int_cols].astype(int)

    self.fact_table[['suburb', 'type']] = self.fact_table[['suburb', 'type']].astype(str)

    # Convert date column
    self.fact_table['date_sold'] = pd.to_datetime(self.fact_table['date_sold'], format='%d/%m/%Y')

    # Extract useful features from date
    self.fact_table['month_sold'] = self.fact_table['date_sold'].dt.month_name()
    self.fact_table['year_sold'] = self.fact_table['date_sold'].dt.year

    # Drop duplicates in categorical fields
    new_dim_suburb = self.fact_table[['suburb']].drop_duplicates().reset_index(drop=True)
    new_dim_suburb['suburb_id'] = range(1, len(new_dim_suburb) + 1)
    
    new_dim_type = self.fact_table[['type']].drop_duplicates().reset_index(drop=True)
    new_dim_type['type_id'] = range(1, len(new_dim_type) + 1)

    # Merge dimension tables back to fact table
    self.fact_table = self.fact_table.merge(new_dim_suburb, on='suburb', how='left')
    self.fact_table = self.fact_table.merge(new_dim_type, on='type', how='left')

    # Drop original categorical columns
    self.drop_columns += ['suburb', 'type']
    self.fact_table = self.fact_table.drop(columns=self.drop_columns)

    print(f'Step 2 finished')