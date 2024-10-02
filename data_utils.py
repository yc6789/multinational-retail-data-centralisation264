import os
import yaml
import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseConnector:
    def __init__(self):
        self.engine = None  # Initialize the engine as None

    def read_db_creds(self, filename):
        """
        Reads the database credentials from a YAML file.

        Args:
        filename (str): The path to the YAML file containing database credentials.

        Returns:
        dict: A dictionary containing the database credentials or None in case of an error.
        """
        filepath = os.path.join(os.getcwd(), filename)
        
        try:
            with open(filepath, 'r') as f:
                creds = yaml.safe_load(f)
                
                # Ensure the credentials are valid
                if not isinstance(creds, dict):
                    logging.error("Invalid format: Expected a dictionary from the YAML file.")
                    return None

                return creds
            
        except FileNotFoundError:
            logging.error(f"The file '{filename}' was not found.")
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML file: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        
        return None

    def init_db_engine(self, filename):
        """
        Initializes the SQLAlchemy engine based on the credentials from the provided YAML file.

        Args:
        filename (str): The path to the YAML file containing the credentials.

        Returns:
        Engine: The SQLAlchemy engine instance or None in case of an error.
        """
        creds = self.read_db_creds(filename)
        
        if creds is None:
            logging.error("Could not read database credentials.")
            return None

        required_keys = ['RDS_HOST', 'RDS_PASSWORD', 'RDS_USER', 'RDS_DATABASE', 'RDS_PORT']
        if not all(key in creds for key in required_keys):
            logging.error(f"Missing one or more required keys in the credentials. Expected keys: {required_keys}")
            return None
        
        try:
            db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            
            # Create the SQLAlchemy engine
            self.engine = create_engine(db_url)
            
            # Test the connection
            with self.engine.connect() as connection:
                logging.info("Database connection successfully initialized.")
            
            return self.engine
        
        except SQLAlchemyError as e:
            logging.error(f"Error initializing database engine: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        
        return None
    
    def list_db_tables(self, engine=None):
        """
        Lists the tables available in the database.

        Args:
        engine (Engine): SQLAlchemy engine. If not provided, defaults to the instance's engine.

        Returns:
        list: A list of table names, or None if an error occurs.
        """
        engine = engine or self.engine
        
        if engine is None:
            logging.error("No engine provided or initialized.")
            return None
        
        try:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            if not tables:
                logging.info("No tables found in the database.")
            else:
                logging.info(f"Available tables: {tables}")
                
            return tables
        
        except SQLAlchemyError as e:
            logging.error(f"Error while fetching table names: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        
        return None
    
    def upload_to_db(self, df, table_name, engine=None):
        """
        Uploads a Pandas DataFrame to the specified table in the database.

        Args:
        df (pd.DataFrame): The data to upload.
        table_name (str): The name of the table to upload the data to.
        engine (Engine): The SQLAlchemy engine. If not provided, defaults to the instance's engine.

        Returns:
        bool: True if upload is successful, False otherwise.
        """
        engine = engine or self.engine

        if engine is None:
            logging.error("No engine provided or initialized for uploading.")
            return False
        
        try:
            # Upload DataFrame to the specified table
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            logging.info(f"Data successfully uploaded to the '{table_name}' table.")
            return True
        
        except SQLAlchemyError as e:
            logging.error(f"Error while uploading data to table '{table_name}': {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        
        return False
