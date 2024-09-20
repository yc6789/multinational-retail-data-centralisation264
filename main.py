from data_utils import DatabaseConnector
from data_extraction import DataExtractor
#from data_cleaning import DataCleaning

# Initialize connector and extractor
db_connector = DatabaseConnector()
sd_connector = DatabaseConnector()
engine = db_connector.init_db_engine('db_cred.yaml')
extractor = DataExtractor(engine)

# List tables and read the first table
tables = db_connector.list_db_tables(engine)
result = extractor.read_table_data('legacy_users')

# Print the result
print(result.info())

