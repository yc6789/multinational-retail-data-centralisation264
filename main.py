from data_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Step 1: Initialize connectors and engines
db_connector = DatabaseConnector()
sd_connector = DatabaseConnector()

# Step 2: Initialize engines for both databases
engine = db_connector.init_db_engine('db_cred.yaml')  # Main database (with legacy_users table)
sd_engine = sd_connector.init_db_engine('db_cred2.yaml')  # Sales data database (for dim_users table)

# Step 3: Initialize the DataExtractor for both engines
extractor = DataExtractor(engine)
sd_extractor = DataExtractor(sd_engine)

# Step 4: List tables and extract data from the 'legacy_users' table
tables = db_connector.list_db_tables(engine)

# Ensure the 'legacy_users' table exists in the database before attempting to extract data
if 'legacy_users' in tables:
    legacy_users_df = extractor.read_table_data('legacy_users')
    print("Extracted legacy_users data:")
    print(legacy_users_df.info())
else:
    print("Table 'legacy_users' not found in the database.")

# Step 5: Initialize DataCleaning instance and clean the extracted data
data_cleaner = DataCleaning()
cleaned_users_df = data_cleaner.clean_user_data(legacy_users_df)
print("Cleaned user data:")
print(cleaned_users_df.info())

# Step 6: Upload the cleaned data to the 'dim_users' table in the sales_data database
upload_status = sd_connector.upload_to_db(cleaned_users_df, 'dim_users', sd_engine)

if upload_status:
    print("Data successfully uploaded to the 'dim_users' table.")
else:
    print("Data upload to 'dim_users' failed.")
