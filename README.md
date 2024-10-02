
# Multinational Retail Data Centralization

This project is designed to extract, clean, and load (ETL) retail data from various sources (databases, CSV, JSON, APIs, and PDFs) into a PostgreSQL database. The goal is to structure the data into a star schema for efficient analysis and querying.

## Table of Contents
- [Project Overview](#project-overview)
- [Technologies](#technologies)
- [Setup Instructions](#setup-instructions)
- [Functionality](#functionality)
  - [Data Extraction](#data-extraction)
  - [Data Cleaning](#data-cleaning)
  - [Uploading Data to the Database](#uploading-data-to-the-database)
- [Star Schema](#star-schema)
- [Assumptions](#assumptions)

## Project Overview

This project handles data from the following sources:
1. **Product Data** (CSV format from S3)
2. **Store Data** (API endpoint returning JSON)
3. **Sales Data** (API endpoint)
4. **Date/Time Data** (JSON format from S3)
5. **Card Details** (PDF format from S3)

The data is extracted, cleaned, and uploaded into a PostgreSQL database using a star schema for reporting and analysis.

## Technologies

- **Python**: Core programming language for the ETL process.
- **Pandas**: Used for data manipulation and cleaning.
- **SQLAlchemy**: For database interactions.
- **boto3**: For extracting data from AWS S3.
- **psycopg2**: PostgreSQL database adapter for Python.
- **Requests**: For interacting with APIs.
- **Tabula**: For extracting table data from PDFs.
- **AWS S3**: Used for storing product and date details.

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd multinational-retail-data-centralisation
   ```

2. **Install Dependencies**:
   You can install the required dependencies using `pip`:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set Up AWS Credentials**:
   Ensure that you have AWS credentials configured for accessing S3. You can do this using the AWS CLI:
   ```bash
   aws configure
   ```

4. **Configure the Database**:
   Make sure to set up a PostgreSQL database and update the `db_cred.yaml` file with your credentials:
   ```yaml
   RDS_HOST: your_host
   RDS_PASSWORD: your_password
   RDS_USER: your_username
   RDS_DATABASE: your_database
   RDS_PORT: your_port
   ```

5. **Run the ETL Process**:
   Once everything is set up, you can run the main ETL process:
   ```bash
   python main.py
   ```

## Functionality

### Data Extraction

The `DataExtractor` class handles the extraction of data from various sources:
- **CSV Data from S3**: Downloads product data from the S3 bucket using the `extract_from_s3` method.
- **Store Data from API**: Extracts store information via a REST API using the `retrieve_stores_data` method.
- **Sales Data from API**: Extracts sales data from another API endpoint.
- **Card Details from PDF**: Extracts card details from a PDF stored in S3 using `tabula` in the `read_pdf_data` method.
- **Date/Time Data from JSON (S3)**: Extracts date/time details from an S3 JSON file using the `extract_json_from_s3` method.

### Data Cleaning

The `DataCleaning` class handles the cleaning of data before uploading to the database:
- **User Data Cleaning**: The `clean_user_data` method removes null values, handles invalid email addresses, and cleans the phone number and date columns.
- **Card Data Cleaning**: The `clean_card_data` method ensures that all card numbers are valid and properly formatted.
- **Store Data Cleaning**: The `clean_store_data` method cleans store data, handles missing latitude/longitude values, and standardizes the opening date format.
- **Product Data Cleaning**: The `convert_product_weights` method converts product weights into kilograms.
- **Order Data Cleaning**: The `clean_orders_data` method prepares the orders data by removing unnecessary columns and ensuring data type consistency.
- **Date/Time Data Cleaning**: The `clean_date_data` method standardizes timestamp columns and removes invalid rows.

### Uploading Data to the Database

The `DatabaseConnector` class manages database interactions:
- **Initialize Database Engine**: The `init_db_engine` method reads the credentials from `db_cred.yaml` and initializes a SQLAlchemy engine.
- **List Tables in Database**: The `list_db_tables` method uses SQLAlchemy to list the available tables in the database.
- **Upload Data to the Database**: The `upload_to_db` method uploads cleaned data to the PostgreSQL database, ensuring that tables are properly formatted for insertion.

### Key Files
- **main.py**: The main script that orchestrates the entire ETL process.
- **data_extraction.py**: Contains the `DataExtractor` class for extracting data from various sources.
- **data_cleaning.py**: Contains the `DataCleaning` class for cleaning data.
- **data_utils.py**: Contains the `DatabaseConnector` class and utility functions for database interactions.

## Star Schema

The cleaned data is uploaded to the database using a star schema structure. The schema is organized as follows:

1. **Fact Table**: The central `orders_table` contains transaction-level data.
2. **Dimension Tables**:
   - **dim_store_details**: Contains store-level details.
   - **dim_products**: Contains product-level details.
   - **dim_date_times**: Contains timestamp and date information for each transaction.

This schema allows for efficient querying and reporting on multinational retail sales data.

## Assumptions

- **Weight Conversions**: A 1:1 ratio of milliliters to grams is used as a rough approximation in the `convert_product_weights` method.
- **Timestamp Handling**: Only the first occurrence of the `timestamp` column is used, assuming the remaining timestamps are either duplicates or less relevant.
- **Database Permissions**: It is assumed that the user has proper permissions to create tables and insert data into the database.

---

### Example Usage

```python
# Initialize the DataExtractor and DatabaseConnector classes
extractor = DataExtractor()
cleaner = DataCleaning()
db_connector = DatabaseConnector()

# Extract product data from S3
product_data_df = extractor.extract_from_s3('s3://data-handling-public/products.csv')

# Clean product data
cleaned_product_df = cleaner.convert_product_weights(product_data_df)

# Upload cleaned product data to the database
engine = db_connector.init_db_engine('db_cred.yaml')
db_connector.upload_to_db(cleaned_product_df, 'dim_products', engine)
```

This project provides a fully operational ETL pipeline that can be extended and customized for future data sources and transformations. For any further clarifications, please feel free to contact the project owner or refer to the documentation provided.
