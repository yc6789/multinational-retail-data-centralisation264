# %%
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from data_utils import DatabaseConnector
s3_address = 's3://data-handling-public/products.csv'

# %%
extractor = DataExtractor()
product_df = extractor.extract_from_s3(s3_address)

# %%
cleaner = DataCleaning()
cleaned_product_df = cleaner.convert_product_weights(product_df)

# %%
cleaned_product_df = cleaner.clean_products_data(cleaned_product_df)

# %%
cleaned_product_df.info()

# %%
import pandas as pd
target = pd.read_csv('removed_product.csv')

# %%
filtered_df = pd.merge(cleaned_product_df, target, on='product_code', how='inner')
filtered_df
# %%
# Uploading to database
sd_connector = DatabaseConnector()

engine = sd_connector.init_db_engine('db_cred2.yaml') 

upload_status = sd_connector.upload_to_db(cleaned_product_df, 'dim_products', engine)

if upload_status:
    print("Data successfully uploaded to the 'dim_products' table.")
else:
    print("Data upload to 'dim_products' failed.")
# %%
