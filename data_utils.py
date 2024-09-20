from sqlalchemy import create_engine,inspect
from sqlalchemy.exc import SQLAlchemyError
import yaml
import os

class DatabaseConnector:
    def read_db_creds(self, filename):
        # Construct the full path for the file
        filepath = os.path.join(os.getcwd(), f'{filename}.yaml')
        
        try:
            # Open the YAML file and load its contents
            with open(filepath, 'r') as f:
                output = yaml.safe_load(f)
                
                # Ensure that the loaded output is a dictionary
                if not isinstance(output, dict):
                    raise ValueError("Invalid format: Expected a dictionary from the YAML file.")
                    
                return output
            
        except FileNotFoundError:
            print(f"Error: The file '{filename}' was not found.")
        except yaml.YAMLError as e:
            print(f"Error parsing YAML file: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        return None

    def init_db_engine(self, filename):
        # Read database credentials
        creds = self.read_db_creds(filename)
        
        if creds is None:
            print("Error: Could not read database credentials.")
            return None
        
        # Ensure all necessary keys are present in the creds
        required_keys = ['RDS_HOST', 'RDS_PASSWORD', 'RDS_USER', 'RDS_DATABASE', 'RDS_PORT']
        if not all(key in creds for key in required_keys):
            print(f"Error: Missing one or more required keys in the credentials. Expected keys: {required_keys}")
            return None
        
        try:
            # Construct the database URL for SQLAlchemy
            db_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            
            # Create the SQLAlchemy engine
            engine = create_engine(db_url)
            
            # Try to connect to check if the engine is valid
            connection = engine.connect()
            connection.close()
            
            return engine
        
        except SQLAlchemyError as e:
            print(f"Error initializing database engine: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        
        return None
    
    def list_db_tables(self, engine):
        try:
            # Use SQLAlchemy inspector to get table names
            inspector = inspect(engine)
            tables = inspector.get_table_names()

            if tables:
                print("Available tables:", tables)
            else:
                print("No tables found in the database.")
                
            return tables
        
        except SQLAlchemyError as e:
            print(f"Error while fetching table names: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None
        
