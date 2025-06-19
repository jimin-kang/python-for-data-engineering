# Extract: Process to pull data from Source system
# Load: Process to write data to a destination system

# Common upstream & downstream systems
# OLTP Databases: Postgres, MySQL, sqlite3, etc
# OLAP Databases: Snowflake, BigQuery, Clickhouse, DuckDB, etc
# Cloud data storage: AWS S3, GCP Cloud Store, Minio, etc
# Queue systems: Kafka, Redpanda, etc
# API
# Local disk: csv, excel, json, xml files
# SFTP\FTP server

import sqlite3
import pandas as pd
import duckdb

# # ***Databases***
# # When reading or writing to a database we use a database driver. Database drivers are libraries that we can use to read or write to a database.
# # Question: How do you read data from a sqlite3 database and write to a DuckDB database?
# # Hint: Look at importing the database libraries for sqlite3 and duckdb and create connections to talk to the respective databases

# # Fetch data from the SQLite Customer table
# # connect to sqlite3 DB
# sqlite_conn = sqlite3.connect("db/tpch.db")

# # # read into dataframe
# customer_df = pd.read_sql_query("SELECT * FROM Customer", sqlite_conn)
# sqlite_conn.close()

# # connect to DuckDB
# duckdb_conn = duckdb.connect("db/duckdb.db")

# # Insert data into the DuckDB Customer table
# # By default, DuckDB operates in "auto-commit" mode
# duckdb_conn.sql("INSERT INTO Customer SELECT * FROM customer_df")

# # # query DuckDB Customer table to verify results
# # duckdb_conn.sql("SELECT * FROM Customer").show()

# duckdb_conn.close()


# ***Cloud storage***
# Question: How do you read data from the S3 location given below and write the data to a DuckDB database?
# Data source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html station data at path "csv.gz/by_station/ASN00002022.csv.gz"
# Hint: Use boto3 client with UNSIGNED config to access the S3 bucket
# Hint: The data will be zipped you have to unzip it and decode it to utf-8

# data is stored in this S3 bucket: http://noaa-ghcn-pds.s3.amazonaws.com/
# compressed data for certain year: http://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/1788.csv.gz
# uncompressed data: http://noaa-ghcn-pds.s3.amazonaws.com/csv/1788.csv

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import io
import gzip

# Create a boto3 client with anonymous access
s3_client = boto3.client(
    's3',
    config=Config(signature_version=UNSIGNED),
    region_name='us-east-1'
)

# AWS S3 bucket and file details
bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/2024.csv.gz"

try:

    # Download the CSV file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read()
    print(file_content)

    # # Decompress the gzip data
    # decompressed_content = gzip.decompress(file_content)
    
    # # Read the CSV file using csv.reader
    # weather_df = pd.read_csv(io.BytesIO(decompressed_content))

    # # Connect to the DuckDB database (assume WeatherData table exists)
    # duckdb_conn = duckdb.connect("db/duckdb.db")

    # # Insert data into the DuckDB WeatherData table
    # duckdb_conn.sql("INSERT INTO WeatherData SELECT * FROM weather_df")

    # # query DuckDB WeatherData table to verify results
    # duckdb_conn.sql("SELECT * FROM WeatherData").show()

    # # close connection
    # duckdb_conn.close()

except Exception as e:
    print(f"Error reading file from S3: {e}")




# ***API***
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?
# URL: "https://api.coincap.io/v2/exchanges"
# Hint: use requests library

# Define the API endpoint
url = "https://api.coincap.io/v2/exchanges"

# Fetch data from the CoinCap API
# Connect to the DuckDB database

# Insert data into the DuckDB Exchanges table
# Prepare data for insertion
# Hint: Ensure that the data types of the data to be inserted is compatible with DuckDBs data column types in ./setup_db.py


# ***Local disk***
# Question: How do you read a CSV file from local disk and write it to a database?
# Look up open function with csvreader for python

# ***Web scraping***
# Questions: Use beatiful soup to scrape the below website and print all the links in that website
# URL of the website to scrape
url = 'https://example.com'
