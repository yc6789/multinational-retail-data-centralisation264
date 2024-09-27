import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import tabula
import requests
import boto3
from io import StringIO

pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

class DataExtractor:
    def __init__(self, engine=None):
        self.engine = engine

    def read_table_data(self, table_name):
        try:
            # Use pandas to read data from the table
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, self.engine)
            return df
        
        except SQLAlchemyError as e:
            print(f"Error while reading data from table {table_name}: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

    def read_rds_table(self, db_connector, table_name):
        """
        Extracts the data from the specified table into a pandas DataFrame.

        Args:
        db_connector (DatabaseConnector): An instance of the DatabaseConnector class to interact with the database.
        table_name (str): The name of the table to extract data from.

        Returns:
        pd.DataFrame: A DataFrame containing the data from the specified table.
        """
        try:
            # Get the list of tables in the database
            tables = db_connector.list_db_tables(self.engine)
            
            # Check if the specified table exists
            if table_name not in tables:
                print(f"Error: Table '{table_name}' not found in the database.")
                return None
            
            # If the table exists, read the data and return it as a DataFrame
            df = self.read_table_data(table_name)
            return df

        except Exception as e:
            print(f"An error occurred while extracting data: {e}")
            return None
    
    def read_pdf_data(self,link):
        # Extract tables from the PDF
        try:
            # Extract all tables from the PDF into a list of DataFrames
            dfs = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)

            # Concatenate all DataFrames if there are multiple tables
            if len(dfs) > 1:
                df_combined = pd.concat(dfs, ignore_index=True)
            else:
                df_combined = dfs[0]

            return df_combined
        except Exception as e:
            print(f"An error occurred while extracting the PDF: {e}")
            return None
        
    def list_number_of_stores(self, endpoint_url, headers):
        """
        Returns the number of stores from the given API endpoint.

        Args:
        endpoint_url (str): The API endpoint to fetch the number of stores.
        headers (dict): A dictionary containing any necessary headers for the API request.

        Returns:
        int: The number of stores extracted from the API.
        """
        try:
            # Make a GET request to the API endpoint
            response = requests.get(endpoint_url, headers=headers)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse the JSON response
                data = response.json()

                # Assuming the number of stores is stored under a specific key, e.g., 'store_count'
                number_of_stores = data.get('number_stores')

                # Ensure the value is valid
                if number_of_stores is not None and isinstance(number_of_stores, int):
                    return number_of_stores
                else:
                    print("Error: 'number_stores' key not found or not an integer in the response.")
                    return None
            else:
                print(f"Error: Failed to retrieve data. Status code: {response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while making the request: {e}")
            return None
        


    def retrieve_stores_data(self, store_endpoint_url, number_of_stores, headers=None):
        """
        Retrieves details for each store and saves them in a Pandas DataFrame.

        Args:
        store_endpoint_url (str): The API endpoint template to fetch store details.
                                Use '{store_number}' as a placeholder for the store number.
        number_of_stores (int): The total number of stores to retrieve data for.
        headers (dict): A dictionary containing any necessary headers for the API request.

        Returns:
        pd.DataFrame: A DataFrame containing all the stores' details.
        """
        stores_data = []

        for store_number in range(number_of_stores):
            try:
                # Format the URL for each store using the store number
                store_url = store_endpoint_url.format(store_number=store_number)

                # Make the GET request to retrieve store details
                response = requests.get(store_url, headers=headers)

                # Check if the request was successful
                if response.status_code == 200:
                    store_data = response.json()
                    stores_data.append(store_data)  # Add the store data to the list
                else:
                    print(f"Error: Failed to retrieve store {store_number}. Status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"An error occurred while retrieving store {store_number}: {e}")

        # Convert the list of store data into a pandas DataFrame
        df_stores = pd.DataFrame(stores_data)
        return df_stores
    
    def extract_from_s3(self, s3_address):
        """
        Downloads a CSV file from the given S3 address and returns a Pandas DataFrame.

        Args:
        s3_address (str): The S3 address of the file in the format 's3://bucket-name/file.csv'.

        Returns:
        pd.DataFrame: The extracted data as a Pandas DataFrame.
        """
        # Extract bucket name and file key from the s3_address
        s3_parts = s3_address.replace("s3://", "").split("/", 1)
        bucket_name = s3_parts[0]
        file_key = s3_parts[1]

        # Initialize a boto3 client for S3
        s3 = boto3.client('s3')

        try:
            # Get the object from S3
            s3_object = s3.get_object(Bucket=bucket_name, Key=file_key)

            # Read the content of the file
            file_content = s3_object['Body'].read().decode('utf-8')

            # Use StringIO to simulate a file object for Pandas
            data = StringIO(file_content)

            # Load the CSV into a pandas DataFrame
            df = pd.read_csv(data)

            return df

        except Exception as e:
            print(f"An error occurred while downloading the file from S3: {e}")
            return None




    
    
