print(
    "################################################################################"
)
print("Use standard python libraries to do the transformations")
print(
    "################################################################################"
)

import pandas as pd
import json

### Question: How do you read data from a CSV file at ./data/sample_data.csv into a list of dictionaries? ###
sample_df = pd.read_csv("data/sample_data.csv")

# list of dicts - each dict is a customer
customers = []

for _, row in sample_df.iterrows():
    # each row returns a Series object corresponding to a single customer
    customer_dict = row.to_dict()
    customers.append(customer_dict)

print(json.dumps(customers, indent=4))

### Question: How do you remove duplicate rows based on customer ID? ###
sample_df_dedup = sample_df.drop_duplicates(subset='Customer_ID', ignore_index=True) # by default, keeps first among set of duplicates
print(sample_df_dedup)

### Question: How do you handle missing values by replacing them with 0? ###
sample_df_copy = sample_df.fillna(0)
total_missing = sample_df_copy.isna().sum().sum()
assert total_missing == 0

### Question: How do you remove outliers such as age > 100 or purchase amount > 1000? ###
mask = ((sample_df['Age'] > 100) | (sample_df['Purchase_Amount'] > 1000))
sample_df_no_outliers = sample_df[~mask]
print(sample_df_no_outliers)

### Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male)? ###
sample_df_copy = sample_df.copy()

gender_map = {
    'Female': 0,
    'Male': 1
}

sample_df_copy['Gender'] = sample_df_copy['Gender'].map(gender_map)

### Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns? ###
sample_df_copy = sample_df.copy()
customer_name_split = sample_df_copy['Customer_Name'].str.split(expand=True)
sample_df_copy['First_Name'] = customer_name_split[0]
sample_df_copy['Last_Name'] = customer_name_split[1]

### Question: How do you calculate the total purchase amount by Gender? ###
purchase_amount_by_gender = sample_df.groupby('Gender')['Purchase_Amount'].sum().reset_index()

### Question: How do you calculate the average purchase amount by Age group? ###
# assume age_groups is the grouping we want
# hint: Why do we convert to float?
sample_df_copy = sample_df.copy()

# bin Age into age_groups
age_groups = {"18-30": [], "31-40": [], "41-50": [], "51-60": [], "61-70": []}
age_group_index = pd.IntervalIndex.from_tuples([
    (18, 30),
    (31, 40),
    (41, 50),
    (51, 60),
    (61, 70)
], closed='both')

sample_df_copy['Age_Grouped'] = pd.cut(sample_df['Age'], bins=age_group_index)

# compute average purchase amount by age group
avg_purchase_by_age_group = sample_df_copy.groupby('Age_Grouped')['Purchase_Amount'].mean().reset_index()

### Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group? ###
your_total_purchase_amount_by_gender = {} # your results should be assigned to this variable
average_purchase_by_age_group = {} # your results should be assigned to this variable

# store results
your_total_purchase_amount_by_gender = dict(zip(purchase_amount_by_gender['Gender'], purchase_amount_by_gender['Purchase_Amount']))
average_purchase_by_age_group = dict(zip(avg_purchase_by_age_group['Age_Grouped'].astype(str), avg_purchase_by_age_group['Purchase_Amount']))

print(f"Total purchase amount by Gender: {json.dumps(your_total_purchase_amount_by_gender, indent=4)}")
print(f"Average purchase amount by Age group: {json.dumps(average_purchase_by_age_group, indent=4)}")

print(
    "################################################################################"
)
print("Use DuckDB to do the transformations")
print(
    "################################################################################"
)

import duckdb

### Question: How do you connect to DuckDB and load data from a CSV file into a DuckDB table? ###
# Connect to DuckDB 
duckdb_conn = duckdb.connect("db/duckdb.db")

# Create SampleData table
duckdb_conn.execute("DROP TABLE IF EXISTS SampleData")
duckdb_conn.execute(
    """
    CREATE TABLE IF NOT EXISTS SampleData (
        Customer_ID Integer,
        Customer_Name TEXT,
        Age INTEGER,
        Gender TEXT,
        Purchase_Amount FLOAT,
        Purchase_Date DATE
    )
    """
)

# defining common queries
insert_SampleData = """
INSERT INTO SampleData SELECT * FROM 'data/sample_data.csv'
"""
read_all_SampleData = """
SELECT * FROM SampleData
"""
reset_SampleData = """
TRUNCATE TABLE SampleData;
INSERT INTO SampleData SELECT * FROM 'data/sample_data.csv';
"""

# Read data from CSV file into DuckDB table
duckdb_conn.execute(insert_SampleData)

### Question: How do you remove duplicate rows based on customer ID in DuckDB? ###
# insert duplicate data
print("Adding duplicate rows to SampleData...")
duckdb_conn.execute(insert_SampleData)

# query result to verify duplicate rows
duckdb_conn.sql(read_all_SampleData).show()

deduplicate_query = """
-- syntax for creating/overwriting existing table
CREATE OR REPLACE TABLE SampleData AS 
    SELECT 
        Customer_ID
        , Customer_Name
        , Age
        , Gender
        , Purchase_Amount
        , Purchase_Date
    FROM ( 
        -- identify duplicate rows by Customer_ID
        SELECT 
        *
        , ROW_NUMBER() OVER (PARTITION BY Customer_ID ORDER BY Purchase_Date DESC) as row_number
        FROM SampleData
        ORDER BY Customer_ID
    ) a
    WHERE a.row_number = 1;
"""
duckdb_conn.execute(deduplicate_query)

print("deduplicated results...")
duckdb_conn.sql(read_all_SampleData).show()

### Question: How do you handle missing values by replacing them with 0 in DuckDB? ###
# insert missing values
insert_missing_values = """
UPDATE SampleData
SET Age = NULL, Purchase_Amount = NULL 
WHERE Customer_ID <= 10
"""
duckdb_conn.execute(insert_missing_values)

print("Verify missing values in Age, Purchase_Amount...")
duckdb_conn.sql(read_all_SampleData).show()

# fill missing values
fill_missing_values = """
CREATE OR REPLACE TABLE SampleData AS
    SELECT 
    Customer_ID
    , Customer_Name
    , COALESCE(Age, 0) Age
    , Gender
    , COALESCE(Purchase_Amount, 0) Purchase_Amount
    , Purchase_Date
    FROM SampleData
"""
duckdb_conn.execute(fill_missing_values)

print("Verify missing values in Age, Purchase_Amount are filled with 0...")
duckdb_conn.sql(read_all_SampleData).show()

### Question: How do you remove data based on a condition in DuckDB? ###
# Remove observations with Age > 50 and Purchase_Amount > 700
rows_to_remove = """
SELECT 
*
FROM SampleData
WHERE Age > 50 AND Purchase_Amount > 700
"""
print("Customers with Age > 50 AND Purchase_Amount > 700...")
duckdb_conn.sql(rows_to_remove).show()

# Remove observations
delete_query = """
DELETE FROM SampleData
WHERE Age > 50 AND Purchase_Amount > 700
"""
duckdb_conn.execute(delete_query)

print("Deleted rows with Age > 50 AND Purchase_Amount > 700...")
duckdb_conn.sql(read_all_SampleData).show()

# reset SampleData table
duckdb_conn.execute(reset_SampleData)

### Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male) in DuckDB? ###
convert_gender_query = """
UPDATE SampleData
SET Gender = 1
WHERE Gender = 'Male';

UPDATE SampleData
SET Gender = 0
WHERE Gender = 'Female';
"""
duckdb_conn.execute(convert_gender_query)
print("Verify converted Gender...")
duckdb_conn.sql(read_all_SampleData).show()

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns in DuckDB?
split_customer_name_query = """
ALTER TABLE SampleData
ADD First_Name TEXT;

ALTER TABLE SampleData
ADD Last_Name TEXT;

UPDATE SampleData
SET First_Name = split_part(Customer_Name, ' ', 1),
Last_Name = split_part(Customer_Name, ' ', 2);
"""
duckdb_conn.execute(split_customer_name_query)

print("Split Customer_Name...")
duckdb_conn.sql(read_all_SampleData).show()

# reset SampleData schema
drop_first_last_name_cols = """
ALTER TABLE SampleData
DROP COLUMN First_Name;

ALTER TABLE SampleData
DROP COLUMN Last_Name;
"""
duckdb_conn.execute(drop_first_last_name_cols)

### Question: How do you calculate the total purchase amount by Gender in DuckDB? ###
# reset SampleData
duckdb_conn.execute(reset_SampleData)

# aggregate purchase amount by gender
total_purchase_amt_by_gender = """
SELECT 
  Gender
  , round(SUM(Purchase_Amount), 2) Total_Purchase_Amount
FROM SampleData
GROUP BY Gender
"""

### Question: How do you calculate the average purchase amount by Age group in DuckDB? ###
# age_groups = {"18-30", "31-40", "41-50", "51-60", "61-70"}
mean_purchase_amt_by_age_group = """
--create age groups
WITH age_groups AS (
    SELECT 
    *,
    CASE 
        WHEN Age >= 18 AND Age <= 30 THEN '18-30'
        WHEN Age >= 31 AND Age <= 40 THEN '31-40'
        WHEN Age >= 41 AND Age <= 50 THEN '41-50'
        WHEN Age >= 51 AND Age <= 60 THEN '51-60'
        WHEN Age >= 61 AND Age <= 70 THEN '61-70'
        ELSE 'UnknownAge'
    END AS Age_Group
    FROM SampleData
)

--mean purchase amount grouped by age group
SELECT 
Age_Group
, round(AVG(Purchase_Amount), 2) AS Avg_Purchase_Amount
FROM age_groups
GROUP BY Age_Group
ORDER BY Age_Group
"""


# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group in DuckDB?
print("====================== Results ======================")
print("Total purchase amount by Gender:")
duckdb_conn.sql(total_purchase_amt_by_gender).show()

print("Average purchase amount by Age group:")
duckdb_conn.sql(mean_purchase_amt_by_age_group).show()

duckdb_conn.close()
