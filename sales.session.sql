SELECT
  MAX(LENGTH(card_number)) AS max_card_number_length, --19
  MAX(LENGTH(store_code)) AS max_store_code_length, --12
  MAX(LENGTH(product_code)) AS max_product_code_length -- 11
FROM orders_table;

/**
Cast the colums of the orders_table to correct datatypes
**/

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR,
ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::VARCHAR,
ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::VARCHAR,
ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::SMALLINT;



SELECT MAX(LENGTH(country_code)) AS max_country_code_length --3
FROM dim_users;

/**
Cast the colums of the dim_users to correct datatypes
**/

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
ALTER COLUMN country_code TYPE VARCHAR(3),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

/**
Cast the colums of the dim_store_details to correct datatypes
**/

SELECT MAX(LENGTH(store_code)) AS max_store_code_length, -- 12
        MAX(LENGTH(country_code)) AS max_country_code_length -- 2
FROM dim_store_details;

SELECT * FROM dim_store_details
LIMIT 3;

UPDATE dim_store_details
SET latitude = COALESCE(
    latitude::FLOAT,
    NULLIF(lat, 'N/A')::FLOAT
);


ALTER TABLE dim_store_details
DROP COLUMN lat;

-- Step 3: Update data types for each column as per the requirements
ALTER TABLE dim_store_details
-- Change 'longitude' to FLOAT
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
-- Change 'latitude' to FLOAT
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
-- Change 'locality' to VARCHAR(255)
ALTER COLUMN locality TYPE VARCHAR(255),
-- Change 'store_code' to VARCHAR(11)
ALTER COLUMN store_code TYPE VARCHAR(12),  -- Adjust length based on your data
-- Change 'staff_numbers' to SMALLINT
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
-- Change 'opening_date' to DATE
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
-- Change 'store_type' to VARCHAR(255) and make it nullable
ALTER COLUMN store_type TYPE VARCHAR(255),
-- Change 'country_code' to VARCHAR(2)
ALTER COLUMN country_code TYPE VARCHAR(2),
-- Change 'continent' to VARCHAR(255)
ALTER COLUMN continent TYPE VARCHAR(255);

-- Make changes to the dim_products table

-- Step 1: Add the 'weight_class' column
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

-- Step 2: Update the weight_class column based on the weight ranges
UPDATE dim_products
SET weight_class = 
    CASE 
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        WHEN weight >= 140 THEN 'Truck_Required'
        ELSE 'Unknown'
    END;

-- Update the dim_products table with the required data types

-- Rename 'removed' column to 'still_available'
ALTER TABLE dim_products
RENAME COLUMN removed to still_available;

SELECT MAX(LENGTH("EAN")) AS max_ean_length, --17
        MAX(LENGTH(product_code)) AS max_product_code_length, -- 11
        MAX(LENGTH(weight_class)) AS max_weight_class_length -- 14
FROM dim_products;

SELECT * FROM dim_products
LIMIT 10;

ALTER TABLE dim_products
-- Alter product_price to FLOAT, using safe casting
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
-- Alter weight to FLOAT, using safe casting
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
-- Change EAN to VARCHAR(3)
ALTER COLUMN "EAN" TYPE VARCHAR(17),
-- Change product_code to VARCHAR(11)
ALTER COLUMN product_code TYPE VARCHAR(11),
-- Alter date_added to DATE, using safe casting
ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
-- Alter uuid to UUID, using safe casting
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
-- Alter weight_class to VARCHAR(14)
ALTER COLUMN weight_class TYPE VARCHAR(14);

/**
Update the dim_date_times table
**/

SELECT * FROM dim_date_times;

SELECT  MAX(LENGTH(CAST("month" AS TEXT))) AS max_month_length, -- 2
        MAX(LENGTH(CAST("year" AS TEXT))) AS max_year_length, -- 4
        MAX(LENGTH(CAST("day" AS TEXT))) AS max_day_length, -- 2
        MAX(LENGTH(time_period)) AS max_time_period, -- 10
        MAX(LENGTH(date_uuid)) AS max_date_uuid -- 36
FROM    dim_date_times;

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

/**
Update the dim_card_details table
**/

SELECT MAX(LENGTH(card_number)) AS max_card_number_length, --16
       MAX(LENGTH(CAST("expiry_date" AS TEXT))) AS max_expiry_date_length, --19
       MAX(LENGTH(CAST("date_payment_confirmed" AS TEXT))) AS max_date_payment_confirmed_length --19
FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(16),
ALTER COLUMN expiry_date TYPE VARCHAR(19) USING expiry_date::TEXT,
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;


/**
Set Primary keys in each table
**/
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

/**
Adding the foreign keys to the orders table
**/

ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid) 
REFERENCES dim_date_times(date_uuid)
ON DELETE CASCADE;

ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid) 
REFERENCES dim_users(user_uuid)
ON DELETE CASCADE;

ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number) 
REFERENCES dim_card_details(card_number)
ON DELETE CASCADE;

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code) 
REFERENCES dim_store_details(store_code)
ON DELETE CASCADE;

ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code) 
REFERENCES dim_products(product_code)
ON DELETE CASCADE;

SELECT *
FROM orders_table
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

SELECT * FROM dim_users
WHERE user_uuid NOT IN (SELECT user_uuid FROM orders_table)

SELECT COUNT(*) FROM dim_card_details;
SELECT COUNT(*) FROM orders_table;

SELECT * FROM orders_table
WHERE user_uuid NOT IN(SELECT dim_users.user_uuid FROM dim_users);