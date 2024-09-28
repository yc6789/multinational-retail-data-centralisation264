import pandas as pd
import re
class DataCleaning:
    def clean_user_data(self, df):
        # Step 1: Handle NULL values
        # Drop rows with NULL values in important columns like 'first_name', 'email_address', 'join_date', etc.
        df_cleaned = df.dropna(subset=['first_name', 'last_name', 'email_address', 'join_date', 'date_of_birth'])

        # Step 2: Correct date columns (coerce invalid dates to NaT)
        df_cleaned['join_date'] = pd.to_datetime(df_cleaned['join_date'], errors='coerce')
        df_cleaned['date_of_birth'] = pd.to_datetime(df_cleaned['date_of_birth'], errors='coerce')

        # Drop rows where 'join_date' or 'date_of_birth' is invalid (NaT)
        df_cleaned = df_cleaned.dropna(subset=['join_date', 'date_of_birth'])

        # Step 3: Remove rows where data is incorrectly typed
        # Check if 'email_address' contains valid emails (basic validation using regex)
        df_cleaned = df_cleaned[df_cleaned['email_address'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$', regex=True, na=False)]

        # Convert 'phone_number' to string and remove non-numeric characters
        df_cleaned['phone_number'] = df_cleaned['phone_number'].str.replace(r'\D', '', regex=True)

        # Step 4: Filter out invalid rows based on logical checks
        # Remove rows where age (calculated from 'date_of_birth') is negative or unreasonably high
        current_year = pd.Timestamp.now().year
        df_cleaned['age'] = current_year - df_cleaned['date_of_birth'].dt.year
        df_cleaned = df_cleaned[(df_cleaned['age'] >= 0) & (df_cleaned['age'] <= 120)]  # Assuming max reasonable age is 120

        # Step 5: Remove rows with duplicated 'email_address' or 'user_uuid'
        df_cleaned = df_cleaned.drop_duplicates(subset=['email_address', 'user_uuid'], keep='first')

        # Drop the 'age' column as it was only used for validation
        df_cleaned = df_cleaned.drop(columns=['age'])

        # Step 6: Finalize cleaning and return cleaned DataFrame
        return df_cleaned
    
    def clean_card_data(self, df):
        """
        Cleans card data by removing NULL values, correcting data types, and formatting errors.

        Args:
        df (pd.DataFrame): The card data DataFrame.

        Returns:
        pd.DataFrame: A cleaned DataFrame.
        """
        # Step 1: Drop rows with missing critical data like card number and expiry date
        df_cleaned = df.dropna(subset=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed'])
        
        # Step 2: Remove rows with invalid card numbers
        # Assuming card numbers should be numeric and of length 16
        df_cleaned['card_number'] = df_cleaned['card_number'].astype(str)  # Ensure card numbers are strings
        df_cleaned = df_cleaned[df_cleaned['card_number'].str.isnumeric()]  # Keep only numeric card numbers
        df_cleaned = df_cleaned[df_cleaned['card_number'].str.len() == 16]  # Keep only 16-digit card numbers
        
        # Step 3: Correct expiry date formatting (assuming MM/YY format)
        df_cleaned['expiry_date'] = pd.to_datetime(df_cleaned['expiry_date'], errors='coerce', format='%m/%y')
        df_cleaned = df_cleaned.dropna(subset=['expiry_date'])  # Drop rows where expiry_date is invalid
        
        # Step 4: Correct date_payment_confirmed formatting
        df_cleaned['date_payment_confirmed'] = pd.to_datetime(df_cleaned['date_payment_confirmed'], errors='coerce')
        df_cleaned = df_cleaned.dropna(subset=['date_payment_confirmed'])  # Drop rows where date_payment_confirmed is invalid
        
        # Step 5: Handle missing or incorrect card_provider values
        # Fill missing card_provider values with 'Unknown' if necessary
        df_cleaned['card_provider'] = df_cleaned['card_provider'].fillna('Unknown')

        # Step 6: Remove duplicate card numbers if necessary
        df_cleaned = df_cleaned.drop_duplicates(subset=['card_number'], keep='first')

        # Return the cleaned DataFrame
        return df_cleaned
    
    def clean_store_data(self, df):
        """
        Cleans store data retrieved from the API by handling NULL values, correcting data types, and fixing formatting issues.

        Args:
        df (pd.DataFrame): The store data DataFrame.

        Returns:
        pd.DataFrame: A cleaned DataFrame.
        """
        # Step 1: Drop the 'index' column if it is redundant
        if 'index' in df.columns:
            df = df.drop(columns=['index'])

        # Step 2: Drop rows with missing critical data such as 'store_code', 'address', 'opening_date'
        df_cleaned = df.dropna(subset=['store_code', 'address', 'opening_date'])

        # Step 3: Convert 'opening_date' to datetime format
        df_cleaned['opening_date'] = pd.to_datetime(df_cleaned['opening_date'], errors='coerce')

        # Step 4: Ensure 'latitude' and 'longitude' are numeric
        df_cleaned['latitude'] = pd.to_numeric(df_cleaned['latitude'], errors='coerce')
        df_cleaned['longitude'] = pd.to_numeric(df_cleaned['longitude'], errors='coerce')

        # Step 5: Handle missing or incorrect values in 'latitude' and 'longitude' columns
        # If lat/lon are invalid (NaN), drop those rows
        df_cleaned = df_cleaned.dropna(subset=['latitude', 'longitude'])

        # Step 6: Convert 'staff_numbers' to numeric (assuming it represents the number of staff)
        df_cleaned['staff_numbers'] = pd.to_numeric(df_cleaned['staff_numbers'], errors='coerce')

        # Step 7: Remove rows where 'staff_numbers' is invalid (NaN or negative values)
        df_cleaned = df_cleaned[df_cleaned['staff_numbers'] >= 0]

        # Step 8: Fill missing values for non-critical fields like 'store_type'
        df_cleaned['store_type'] = df_cleaned['store_type'].fillna('Unknown')

        # Step 9: Clean the 'country_code' and 'continent' fields (strip whitespace)
        df_cleaned['country_code'] = df_cleaned['country_code'].str.strip()
        df_cleaned['continent'] = df_cleaned['continent'].str.strip()

        # Step 10: Fill missing values for 'lat' column using 'latitude' if 'lat' is NaN
        if 'lat' in df_cleaned.columns:
            df_cleaned['lat'] = df_cleaned['lat'].fillna(df_cleaned['latitude'])

        # Step 11: Remove duplicate entries based on 'store_code'
        df_cleaned = df_cleaned.drop_duplicates(subset=['store_code'], keep='first')

        # Return the cleaned DataFrame
        return df_cleaned
    
    def convert_product_weights(self, df):
        """
        Converts the 'weight' column in the products DataFrame to kilograms (kg).
        
        Args:
        df (pd.DataFrame): The products DataFrame containing a 'weight' column.
        
        Returns:
        pd.DataFrame: The cleaned DataFrame with the 'weight' column represented as a float in kg.
        """
        # Define a function to clean and convert the weight
        def convert_weight(value):
            if pd.isnull(value):
                return None

            # Remove any extra spaces
            value = value.strip().lower()

            # Use regex to find numeric part and unit
            weight_match = re.match(r"(\d+\.?\d*)\s*(g|kg|ml|l)", value)

            if weight_match:
                weight = float(weight_match.group(1))  # Extract the numeric value
                unit = weight_match.group(2)  # Extract the unit

                # Convert based on the unit
                if unit == 'g':
                    return weight / 1000  # Convert grams to kilograms
                elif unit == 'kg':
                    return weight  # Already in kilograms
                elif unit == 'ml':
                    return weight / 1000  # Convert ml to grams, then to kilograms
                elif unit == 'l':
                    return weight  # Assuming 1 liter = 1 kilogram (density of water)

            return None  # If no valid match, return None

        # Apply the conversion function to the 'weight' column
        df['weight'] = df['weight'].apply(convert_weight)

        # Return the cleaned DataFrame
        return df
    
    def clean_orders_data(self, df):
        """
        Cleans the orders data by removing unnecessary columns and preparing the table for database upload.

        Args:
        df (pd.DataFrame): The orders DataFrame.

        Returns:
        pd.DataFrame: The cleaned orders DataFrame ready for uploading to the database.
        """
        # Step 1: Remove the unnecessary columns 'level_0', 'index', 'first_name', 'last_name', and '1'
        columns_to_remove = ['level_0', 'index', 'first_name', 'last_name', '1']
        df_cleaned = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')

        # Step 2: Check and handle missing values in critical columns
        # Critical columns: 'user_uuid', 'card_number', 'store_code', 'product_code', 'product_quantity'
        critical_columns = ['user_uuid', 'card_number', 'store_code', 'product_code', 'product_quantity']
        df_cleaned = df_cleaned.dropna(subset=critical_columns)

        # Step 3: Ensure data types are consistent
        df_cleaned['card_number'] = df_cleaned['card_number'].astype(str)  # Treat card number as a string (to avoid numeric issues)
        df_cleaned['product_quantity'] = pd.to_numeric(df_cleaned['product_quantity'], errors='coerce')

        # Step 4: Remove rows where 'product_quantity' is invalid (e.g., NaN or negative)
        df_cleaned = df_cleaned[df_cleaned['product_quantity'] >= 0]

        # Step 5: Drop duplicate rows if any, keeping the first occurrence
        df_cleaned = df_cleaned.drop_duplicates()

        # Step 6: Clean the column names (optional)
        df_cleaned.columns = df_cleaned.columns.str.strip().str.lower().str.replace(' ', '_')

        # Return the cleaned DataFrame
        return df_cleaned
    

    def clean_date_data(self, df):
        """
        Cleans the date data by selecting relevant timestamp columns, converting them to proper datetime format,
        and ensuring consistency.

        Args:
        df (pd.DataFrame): The raw date data DataFrame.

        Returns:
        pd.DataFrame: The cleaned date data DataFrame.
        """
        # Step 1: Select the first 'timestamp' and 'date_uuid' columns
        timestamp_cols = [col for col in df.columns if col.startswith('timestamp')]
        date_uuid_cols = [col for col in df.columns if col.startswith('date_uuid')]

        # Select the first occurrence of 'timestamp' and 'date_uuid'
        if timestamp_cols and date_uuid_cols:
            first_timestamp_col = timestamp_cols[0]  # Select the first 'timestamp' column (timestamp.0)
            first_date_uuid_col = date_uuid_cols[0]  # Select the first 'date_uuid' column (date_uuid.0)

            # Step 2: Convert the selected 'timestamp' column to datetime
            df[first_timestamp_col] = pd.to_datetime(df[first_timestamp_col], errors='coerce')

            # Step 3: Drop rows where the timestamp is missing
            df_cleaned = df.dropna(subset=[first_timestamp_col])

            # Step 4: Keep only relevant columns (timestamp and date_uuid)
            df_cleaned = df_cleaned[[first_timestamp_col, first_date_uuid_col]]

            # Step 5: Rename columns for clarity
            df_cleaned.columns = ['timestamp', 'date_uuid']

            return df_cleaned
        else:
            print("No valid 'timestamp' or 'date_uuid' columns found in the DataFrame.")
            return df



        


        
        

    
