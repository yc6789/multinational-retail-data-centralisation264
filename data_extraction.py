import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

class DataExtractor:
    def __init__(self, engine):
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
