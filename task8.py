from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from data_utils import DatabaseConnector

# Initialize the extractor, cleaner, and connector
extractor = DataExtractor()
cleaner = DataCleaning()
db_connector = DatabaseConnector()

# Step 1: Extract JSON data from the S3 link
json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
date_df = extractor.extract_json_from_s3(json_url)
print(date_df.columns)

# Step 2: Clean the extracted date data
if date_df is not None:
    cleaned_date_df = cleaner.clean_date_data(date_df)

    # Step 3: Initialize the database engine (replace 'db_cred.yaml' with your actual credentials file)
    engine = db_connector.init_db_engine('db_cred2.yaml')

    # Step 4: Upload the cleaned data to the database as 'dim_date_times'
    if engine is not None:
        db_connector.upload_to_db(cleaned_date_df, 'dim_date_times', engine)
    else:
        print("Failed to initialize the database engine.")
else:
    print("Failed to extract JSON data from S3.")