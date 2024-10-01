# %%
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from data_utils import DatabaseConnector

db_connector = DatabaseConnector()
engine = db_connector.init_db_engine('db_cred.yaml')

tables_name = db_connector.list_db_tables(engine)

extractor = DataExtractor(engine)
orders_df = extractor.read_rds_table(db_connector,'orders_table')

orders_df.describe()
orders_df.info()
orders_df.head()

cleaner = DataCleaning()
cleaned_order_df = cleaner.clean_orders_data(orders_df)

# %%
sd_connector = DatabaseConnector()

engine = sd_connector.init_db_engine('db_cred2.yaml') 

upload_status = sd_connector.upload_to_db(cleaned_order_df, 'orders_table', engine)

if upload_status:
    print("Data successfully uploaded to the 'orders_table' table.")
else:
    print("Data upload to 'orders_table' failed.")