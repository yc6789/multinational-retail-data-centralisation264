# Import necessary modules and classes
from data_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Step 1: Initialize connectors and engines
def initialize_connectors_and_extract_data():
    db_connector = DatabaseConnector()
    sd_connector = DatabaseConnector()

    # Step 2: Initialize engines for both databases
    engine = db_connector.init_db_engine('db_cred.yaml')  # Main database (with legacy_users table)
    sd_engine = sd_connector.init_db_engine('db_cred2.yaml')  # Sales data database (for dim_users table)

    return db_connector, sd_connector, engine, sd_engine

# Function to extract, clean and upload user data
def process_user_data():
    db_connector, sd_connector, engine, sd_engine = initialize_connectors_and_extract_data()
    extractor = DataExtractor(engine)

    # List tables and extract data from 'legacy_users'
    tables = db_connector.list_db_tables(engine)
    if 'legacy_users' in tables:
        legacy_users_df = extractor.read_table_data('legacy_users')
        data_cleaner = DataCleaning()
        cleaned_users_df = data_cleaner.clean_user_data(legacy_users_df)

        # Upload cleaned data to 'dim_users'
        upload_status = sd_connector.upload_to_db(cleaned_users_df, 'dim_users', sd_engine)
        if upload_status:
            print("Data successfully uploaded to 'dim_users'.")
        else:
            print("Data upload to 'dim_users' failed.")
    else:
        print("Table 'legacy_users' not found.")

# Function to extract, clean and upload card details
def process_card_data():
    sd_connector = DatabaseConnector()
    engine = sd_connector.init_db_engine('db_cred2.yaml')
    extractor = DataExtractor(engine)

    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    df = extractor.read_pdf_data(pdf_link)
    
    data_cleaner = DataCleaning()
    cleaned_pdf_df = data_cleaner.clean_card_data(df)

    # Upload cleaned data to 'dim_card_details'
    upload_status = sd_connector.upload_to_db(cleaned_pdf_df, 'dim_card_details', engine)
    if upload_status:
        print("Data successfully uploaded to 'dim_card_details'.")
    else:
        print("Data upload to 'dim_card_details' failed.")

# Function to extract, clean and upload store details
def process_store_data():
    dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    retrieve_store_api = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    num_stores_api = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

    extractor = DataExtractor()
    num_stores = extractor.list_number_of_stores(num_stores_api, dict)
    store_df = extractor.retrieve_stores_data(retrieve_store_api, num_stores, dict)

    data_cleaner = DataCleaning()
    cleaned_store_df = data_cleaner.clean_store_data(store_df)

    # Upload cleaned data to 'dim_store_details'
    sd_connector = DatabaseConnector()
    engine = sd_connector.init_db_engine('db_cred2.yaml')
    upload_status = sd_connector.upload_to_db(cleaned_store_df, 'dim_store_details', engine)

    if upload_status:
        print("Data successfully uploaded to 'dim_store_details'.")
    else:
        print("Data upload to 'dim_store_details' failed.")

# Function to extract, clean and upload product data
def process_product_data():
    s3_address = 's3://data-handling-public/products.csv'
    extractor = DataExtractor()
    product_df = extractor.extract_from_s3(s3_address)

    data_cleaner = DataCleaning()
    cleaned_product_df = data_cleaner.convert_product_weights(product_df)
    cleaned_product_df = data_cleaner.clean_products_data(cleaned_product_df)

    # Upload cleaned data to 'dim_products'
    sd_connector = DatabaseConnector()
    engine = sd_connector.init_db_engine('db_cred2.yaml')
    upload_status = sd_connector.upload_to_db(cleaned_product_df, 'dim_products', engine)

    if upload_status:
        print("Data successfully uploaded to 'dim_products'.")
    else:
        print("Data upload to 'dim_products' failed.")

# Function to extract, clean and upload order data
def process_order_data():
    db_connector = DatabaseConnector()
    engine = db_connector.init_db_engine('db_cred.yaml')

    extractor = DataExtractor(engine)
    orders_df = extractor.read_rds_table(db_connector, 'orders_table')

    data_cleaner = DataCleaning()
    cleaned_order_df = data_cleaner.clean_orders_data(orders_df)

    # Upload cleaned data to 'orders_table'
    sd_connector = DatabaseConnector()
    sd_engine = sd_connector.init_db_engine('db_cred2.yaml')
    upload_status = sd_connector.upload_to_db(cleaned_order_df, 'orders_table', sd_engine)

    if upload_status:
        print("Data successfully uploaded to 'orders_table'.")
    else:
        print("Data upload to 'orders_table' failed.")

# Function to extract, clean and upload date data
def process_date_data():
    extractor = DataExtractor()
    cleaner = DataCleaning()
    db_connector = DatabaseConnector()

    json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_df = extractor.extract_json_from_s3(json_url)

    if date_df is not None:
        cleaned_date_df = cleaner.clean_date_data(date_df)

        engine = db_connector.init_db_engine('db_cred2.yaml')
        if engine is not None:
            upload_status = db_connector.upload_to_db(cleaned_date_df, 'dim_date_times', engine)
            if upload_status:
                print("Data successfully uploaded to 'dim_date_times'.")
            else:
                print("Data upload to 'dim_date_times' failed.")
        else:
            print("Failed to initialize the database engine.")
    else:
        print("Failed to extract JSON data from S3.")

# Main entry point for all processes
if __name__ == '__main__':
    process_user_data()
    process_card_data()
    process_store_data()
    process_product_data()
    process_order_data()
    process_date_data()
