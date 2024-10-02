# %%
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from data_utils import DatabaseConnector

dict = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
retrieve_store_api = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
num_stores = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

extractor = DataExtractor()

# %%
num_stores = extractor.list_number_of_stores(num_stores,dict)
store_df = extractor.retrieve_stores_data(retrieve_store_api,num_stores,dict)

# %%
cleaner = DataCleaning()
cleaned_store_df = cleaner.clean_store_data(store_df)
cleaned_store_df.info()
# %%
sd_connector = DatabaseConnector()

engine = sd_connector.init_db_engine('db_cred2.yaml') 

upload_status = sd_connector.upload_to_db(cleaned_store_df, 'dim_store_details', engine)

if upload_status:
    print("Data successfully uploaded to the 'dim_store_details' table.")
else:
    print("Data upload to 'dim_store_details' failed.")