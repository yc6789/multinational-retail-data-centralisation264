# %%
from data_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'


sd_connector = DatabaseConnector()

engine = sd_connector.init_db_engine('db_cred2.yaml') 

extractor = DataExtractor(engine)

df = extractor.read_pdf_data(pdf_link)
# %%
data_cleaner = DataCleaning()
cleaned_pdf_df = data_cleaner.clean_card_data(df)
# %%

upload_status = sd_connector.upload_to_db(cleaned_pdf_df, 'dim_card_details', engine)

if upload_status:
    print("Data successfully uploaded to the 'dim_card_details' table.")
else:
    print("Data upload to 'dim_card_details' failed.")
