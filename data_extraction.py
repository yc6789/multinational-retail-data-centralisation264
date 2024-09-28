import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import tabula
import requests
import boto3
from io import StringIO


class DataExtractor:
    def __init__(self, engine=None):
        self.engine = engine

    def read_table_data(self, table_name):
        """
        Reads data from the specified table using SQLAlchemy engine and returns it as a DataFrame.

        Args:
        table_name (str): The name of the database table.

        Returns:
        pd.DataFrame: A DataFrame containing the table data, or None if an error occurs.
        """
        try:
            query = f"SELECT * FROM {table_name}"
            return pd.read_sql(query, self.engine)
        except SQLAlchemyError as e:
            print(f"SQLAlchemyError while reading data from table {table_name}: {e}")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
        return None

    def read_rds_table(self, db_connector, table_name):
        """
        Reads the data from a specified table via a DatabaseConnector instance.

        Args:
        db_connector (DatabaseConnector): An instance of the DatabaseConnector class.
        table_name (str): The name of the table to be extracted.

        Returns:
        pd.DataFrame: A DataFrame containing the data, or None if the table doesn't exist or errors occur.
        """
        try:
            tables = db_connector.list_db_tables(self.engine)
            if table_name not in tables:
                print(f"Error: Table '{table_name}' not found.")
                return None
            return self.read_table_data(table_name)
        except Exception as e:
            print(f"Error while extracting data: {e}")
        return None

    def read_pdf_data(self, link):
        """
        Extracts table data from a PDF.

        Args:
        link (str): URL of the PDF.

        Returns:
        pd.DataFrame: A DataFrame containing the extracted data, or None if an error occurs.
        """
        try:
            dfs = tabula.read_pdf(link, pages='all', multiple_tables=True)
            return pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
        except Exception as e:
            print(f"Error extracting data from PDF: {e}")
        return None

    def list_number_of_stores(self, endpoint_url, headers):
        """
        Retrieves the number of stores from the API.

        Args:
        endpoint_url (str): The API endpoint URL.
        headers (dict): Headers for the API request.

        Returns:
        int: The number of stores or None if an error occurs.
        """
        try:
            response = requests.get(endpoint_url, headers=headers)
            response.raise_for_status()
            return response.json().get('number_stores')
        except requests.exceptions.RequestException as e:
            print(f"Error during API request: {e}")
        return None

    def retrieve_stores_data(self, store_endpoint_url, number_of_stores, headers=None):
        """
        Fetches store details for the given number of stores.

        Args:
        store_endpoint_url (str): API endpoint template to fetch store details (uses '{store_number}' as a placeholder).
        number_of_stores (int): Number of stores to retrieve data for.
        headers (dict): Optional headers for the API request.

        Returns:
        pd.DataFrame: DataFrame containing store details, or None if an error occurs.
        """
        stores_data = []
        for store_number in range(number_of_stores):
            try:
                store_url = store_endpoint_url.format(store_number=store_number)
                response = requests.get(store_url, headers=headers)
                response.raise_for_status()
                stores_data.append(response.json())
            except requests.exceptions.RequestException as e:
                print(f"Error retrieving store {store_number}: {e}")
        return pd.DataFrame(stores_data) if stores_data else None

    def extract_from_s3(self, s3_address):
        """
        Extracts a CSV file from the specified S3 address.

        Args:
        s3_address (str): S3 URL for the CSV file.

        Returns:
        pd.DataFrame: DataFrame containing the extracted CSV data.
        """
        try:
            s3_parts = s3_address.replace("s3://", "").split("/", 1)
            bucket_name, file_key = s3_parts
            s3 = boto3.client('s3')
            s3_object = s3.get_object(Bucket=bucket_name, Key=file_key)
            file_content = s3_object['Body'].read().decode('utf-8')
            return pd.read_csv(StringIO(file_content))
        except Exception as e:
            print(f"Error downloading file from S3: {e}")
        return None

    def extract_json_from_s3(self, url):
        """
        Downloads JSON data from a URL and returns it as a Pandas DataFrame.

        Args:
        url (str): URL of the JSON file.

        Returns:
        pd.DataFrame: A DataFrame containing the JSON data, or None if an error occurs.
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            return pd.json_normalize(response.json())
        except requests.exceptions.RequestException as e:
            print(f"Error during JSON download: {e}")
        return None
