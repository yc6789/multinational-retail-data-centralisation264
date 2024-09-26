import pandas as pd

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
        

    
