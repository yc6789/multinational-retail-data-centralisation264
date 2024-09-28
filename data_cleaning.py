import pandas as pd
import re

class DataCleaning:
    def clean_user_data(self, df):
        """
        Cleans user data by handling NULL values, validating data types, and removing invalid entries.
        
        Args:
        df (pd.DataFrame): The user data DataFrame.

        Returns:
        pd.DataFrame: A cleaned DataFrame.
        """
        # Handle NULL values in critical columns
        df_cleaned = df.dropna(subset=['first_name', 'last_name', 'email_address', 'join_date', 'date_of_birth'])

        # Correct date columns
        df_cleaned['join_date'] = pd.to_datetime(df_cleaned['join_date'], errors='coerce')
        df_cleaned['date_of_birth'] = pd.to_datetime(df_cleaned['date_of_birth'], errors='coerce')

        # Drop rows where dates are invalid
        df_cleaned = df_cleaned.dropna(subset=['join_date', 'date_of_birth'])

        # Validate email format using regex
        df_cleaned = df_cleaned[df_cleaned['email_address'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$', regex=True, na=False)]

        # Clean phone number by removing non-numeric characters
        df_cleaned['phone_number'] = df_cleaned['phone_number'].str.replace(r'\D', '', regex=True)

        # Filter out invalid age entries
        current_year = pd.Timestamp.now().year
        df_cleaned['age'] = current_year - df_cleaned['date_of_birth'].dt.year
        df_cleaned = df_cleaned[(df_cleaned['age'] >= 0) & (df_cleaned['age'] <= 120)]  # Assuming max age is 120

        # Remove duplicates
        df_cleaned = df_cleaned.drop_duplicates(subset=['email_address', 'user_uuid'], keep='first')

        # Drop age column used for filtering
        df_cleaned = df_cleaned.drop(columns=['age'])

        return df_cleaned
    
    def clean_card_data(self, df):
        """
        Cleans card data by handling NULL values, correcting data types, and removing invalid entries.
        
        Args:
        df (pd.DataFrame): The card data DataFrame.

        Returns:
        pd.DataFrame: A cleaned DataFrame.
        """
        # Drop rows with critical missing data
        df_cleaned = df.dropna(subset=['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed'])

        # Ensure card numbers are numeric and of valid length
        df_cleaned['card_number'] = df_cleaned['card_number'].astype(str)
        df_cleaned = df_cleaned[df_cleaned['card_number'].str.isnumeric()]
        df_cleaned = df_cleaned[df_cleaned['card_number'].str.len() == 16]

        # Convert expiry date and payment date to datetime
        df_cleaned['expiry_date'] = pd.to_datetime(df_cleaned['expiry_date'], errors='coerce', format='%m/%y')
        df_cleaned = df_cleaned.dropna(subset=['expiry_date'])
        df_cleaned['date_payment_confirmed'] = pd.to_datetime(df_cleaned['date_payment_confirmed'], errors='coerce')
        df_cleaned = df_cleaned.dropna(subset=['date_payment_confirmed'])

        # Fill missing card_provider with 'Unknown'
        df_cleaned['card_provider'] = df_cleaned['card_provider'].fillna('Unknown')

        # Remove duplicate card numbers
        df_cleaned = df_cleaned.drop_duplicates(subset=['card_number'], keep='first')

        return df_cleaned
    
    def clean_store_data(self, df):
        """
        Cleans store data retrieved from the API by handling NULL values and correcting data types,
        with special handling for 'Web Portal' stores to avoid deleting important records.

        Args:
        df (pd.DataFrame): The store data DataFrame.

        Returns:
        pd.DataFrame: A cleaned DataFrame.
        """
        # Drop redundant columns if present
        df_cleaned = df.drop(columns=['index'], errors='ignore')

        # Drop rows with missing critical data
        df_cleaned = df_cleaned.dropna(subset=['store_code', 'opening_date'], how='any')

        # Convert date and numeric fields to appropriate types
        df_cleaned['opening_date'] = pd.to_datetime(df_cleaned['opening_date'], errors='coerce')
        
        # Convert latitude and longitude, but keep 'Web Portal' entries even if these values are missing
        df_cleaned['latitude'] = pd.to_numeric(df_cleaned['latitude'], errors='coerce')
        df_cleaned['longitude'] = pd.to_numeric(df_cleaned['longitude'], errors='coerce')

        # Filter out rows where latitude and longitude are missing, but keep 'Web Portal' stores
        df_cleaned = df_cleaned[
            (df_cleaned['store_type'] == 'Web Portal') | 
            (df_cleaned['latitude'].notnull() & df_cleaned['longitude'].notnull())
        ]

        # Handle missing or invalid staff numbers
        df_cleaned['staff_numbers'] = pd.to_numeric(df_cleaned['staff_numbers'], errors='coerce')
        df_cleaned = df_cleaned[df_cleaned['staff_numbers'] >= 0]

        # Fill missing store type and clean string fields
        df_cleaned['store_type'] = df_cleaned['store_type'].fillna('Unknown')
        df_cleaned['country_code'] = df_cleaned['country_code'].str.strip()
        df_cleaned['continent'] = df_cleaned['continent'].str.strip()

        # Fill lat column using latitude if missing
        if 'lat' in df_cleaned.columns:
            df_cleaned['lat'] = df_cleaned['lat'].fillna(df_cleaned['latitude'])

        # Remove duplicates based on store_code
        df_cleaned = df_cleaned.drop_duplicates(subset=['store_code'], keep='first')

        return df_cleaned

    
    def convert_product_weights(self, df):
        """
        Converts weights in the products DataFrame to kilograms.
        
        Args:
        df (pd.DataFrame): The products DataFrame containing a 'weight' column.

        Returns:
        pd.DataFrame: A cleaned DataFrame with the 'weight' column in kilograms.
        """
        def convert_weight(value):
            if pd.isnull(value):
                return None
            value = value.strip().lower()
            weight_match = re.match(r"(\d+\.?\d*)\s*(g|kg|ml|l)", value)
            if weight_match:
                weight = float(weight_match.group(1))
                unit = weight_match.group(2)
                if unit == 'g':
                    return weight / 1000
                elif unit == 'kg':
                    return weight
                elif unit == 'ml':
                    return weight / 1000
                elif unit == 'l':
                    return weight
            return None

        df['weight'] = df['weight'].apply(convert_weight)
        return df
    
    def clean_orders_data(self, df):
        """
        Cleans orders data by removing unnecessary columns and ensuring consistency in data types.

        Args:
        df (pd.DataFrame): The orders DataFrame.

        Returns:
        pd.DataFrame: The cleaned orders DataFrame.
        """
        # Remove unnecessary columns
        columns_to_remove = ['level_0', 'index', 'first_name', 'last_name', '1']
        df_cleaned = df.drop(columns=columns_to_remove, errors='ignore')

        # Ensure critical columns are filled
        critical_columns = ['user_uuid', 'card_number', 'store_code', 'product_code', 'product_quantity']
        df_cleaned = df_cleaned.dropna(subset=critical_columns)

        # Convert numeric columns and ensure product quantity is valid
        df_cleaned['card_number'] = df_cleaned['card_number'].astype(str)
        df_cleaned['product_quantity'] = pd.to_numeric(df_cleaned['product_quantity'], errors='coerce')
        df_cleaned = df_cleaned[df_cleaned['product_quantity'] >= 0]

        # Remove duplicates
        df_cleaned = df_cleaned.drop_duplicates()

        # Clean and standardize column names
        df_cleaned.columns = df_cleaned.columns.str.strip().str.lower().str.replace(' ', '_')

        return df_cleaned

    def clean_date_data(self, df):
        """
        Cleans date data by converting timestamps and ensuring data consistency.

        Args:
        df (pd.DataFrame): The raw date data DataFrame.

        Returns:
        pd.DataFrame: The cleaned date data DataFrame.
        """
        # Select and clean relevant columns
        timestamp_cols = [col for col in df.columns if col.startswith('timestamp')]
        date_uuid_cols = [col for col in df.columns if col.startswith('date_uuid')]

        if timestamp_cols and date_uuid_cols:
            first_timestamp_col = timestamp_cols[0]
            first_date_uuid_col = date_uuid_cols[0]

            df[first_timestamp_col] = pd.to_datetime(df[first_timestamp_col], errors='coerce')
            df_cleaned = df.dropna(subset=[first_timestamp_col])

            df_cleaned = df_cleaned[[first_timestamp_col, first_date_uuid_col]]
            df_cleaned.columns = ['timestamp', 'date_uuid']

            return df_cleaned
        else:
            print("No valid 'timestamp' or 'date_uuid' columns found.")
            return df
