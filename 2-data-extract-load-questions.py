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

# ***Required Packages***
import sqlite3
import pandas as pd
import duckdb
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import io
import gzip
import requests
from bs4 import BeautifulSoup

# ***Databases***
# When reading or writing to a database we use a database driver. Database drivers are libraries that we can use to read or write to a database.
# Question: How do you read data from a sqlite3 database and write to a DuckDB database?
# Hint: Look at importing the database libraries for sqlite3 and duckdb and create connections to talk to the respective databases

# Fetch data from the SQLite Customer table
# connect to sqlite3 DB
sqlite_conn = sqlite3.connect("db/tpch.db")

# # read into dataframe
customer_df = pd.read_sql_query("SELECT * FROM Customer", sqlite_conn)
sqlite_conn.close()

# connect to DuckDB
duckdb_conn = duckdb.connect("db/duckdb.db")

# Insert data into the DuckDB Customer table
# By default, DuckDB operates in "auto-commit" mode
duckdb_conn.sql("INSERT INTO Customer SELECT * FROM customer_df")

# # query DuckDB Customer table to verify results
# duckdb_conn.sql("SELECT * FROM Customer").show()

duckdb_conn.close()


# ***Cloud storage***
# Question: How do you read data from the S3 location given below and write the data to a DuckDB database?
# Data source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html station data at path "csv.gz/by_station/ASN00002022.csv.gz"
# Hint: Use boto3 client with UNSIGNED config to access the S3 bucket
# Hint: The data will be zipped you have to unzip it and decode it to utf-8

# data is stored in this S3 bucket: http://noaa-ghcn-pds.s3.amazonaws.com/
# compressed data for certain year: http://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/1788.csv.gz
# uncompressed data: http://noaa-ghcn-pds.s3.amazonaws.com/csv/1788.csv

# Create a boto3 client with UNSIGNED config
s3_client = boto3.client(
    's3',
    config=Config(signature_version=UNSIGNED)
)

# AWS S3 bucket and file details
bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/by_station/ASN00002024.csv.gz" # extract 2024 data

try:
    # Download the CSV file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    file_content = response['Body'].read()
    
    # Decompress the gzip data
    decompressed_content = gzip.decompress(file_content)
    
    # Read the CSV file using pandas (or csv.reader for data too large to fit in memory?)
    weather_df = pd.read_csv(io.BytesIO(decompressed_content), header=None)

    # Connect to the DuckDB database (assume WeatherData table exists)
    duckdb_conn = duckdb.connect("db/duckdb.db")

    # Insert data into the DuckDB WeatherData table
    duckdb_conn.sql("INSERT INTO WeatherData SELECT * FROM weather_df")

    # query DuckDB WeatherData table to verify results
    duckdb_conn.sql("SELECT * FROM WeatherData").show()

    # close connection
    duckdb_conn.close()

except Exception as e:
    print(f"Error reading file from S3: {e}")


# ***API***
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?
# URL: "https://rest.coincap.io/v3/exchanges"
# Hint: use requests library

# Define the API endpoint
# v3 requires API key
apiKey = os.getenv("API_KEY")
url = f"https://rest.coincap.io/v3/exchanges?apiKey={apiKey}"

# Fetch data from the CoinCap API 
response = requests.get(url)
if response.status_code == 200:
    # successful response will return the following JSON object (refer to https://pro.coincap.io/api-docs):
    # {
    #   "data": [
    #     {
    #       "exchangeId": "itbit",
    #       "name": "Itbit",
    #       "rank": "33",
    #       "percentTotalVolume": null,
    #       "volumeUsd": null,
    #       "tradingPairs": "0",
    #       "socket": null,
    #       "exchangeUrl": "https://www.itbit.com/",
    #       "updated": 0
    #     }
    #   ],
    #   "timestamp": 1726084388658
    # }

    exchange_df = pd.DataFrame(response.json()['data'])
    
    # Connect to the DuckDB database
    duckdb_conn = duckdb.connect("db/duckdb.db")

    # Prepare data for insertion
    # Hint: Ensure that the data types of the data to be inserted is compatible with DuckDBs data column types in ./setup_db.py

    # Insert data into the DuckDB Exchanges table
    duckdb_conn.sql("INSERT INTO Exchanges SELECT * FROM exchange_df")

    # query Exchanges table to confirm successful insertion
    duckdb_conn.sql("SELECT * FROM Exchanges").show()

    # close connection
    duckdb_conn.close()

else:
    print(f"Failed to retrieve data: response code {response.status_code}")


# ***Local disk***
# Question: How do you read a CSV file from local disk and write it to a database?
# create SampleData table and insert data from sample_data.csv into table directly

# open connection
duckdb_conn = duckdb.connect("db/duckdb.db")

# create SampleData table in DuckDB
duckdb_conn.execute("DROP TABLE IF EXISTS SampleData")
duckdb_conn.execute(
    """
CREATE TABLE IF NOT EXISTS SampleData (
    Customer_ID INTEGER,
    Customer_Name TEXT,
    Age INTEGER,
    Gender TEXT,
    Purchase_Amount FLOAT,
    Purchase_Date DATE
)
"""
)

# insert data
duckdb_conn.sql("INSERT INTO SampleData SELECT * FROM 'data/sample_data.csv'")

# query SampleData to confirm successful insertion
duckdb_conn.sql("SELECT * FROM SampleData").show()

# close connection
duckdb_conn.close()

# ***Web scraping***
# Questions: Use beatiful soup to scrape the website below and print all the links in that website
url = 'https://github.com/jimin-kang/python-for-data-engineering'

# retrieve html content via requests
response = requests.get(url)
if response.status_code == 200:
    # create BS object to parse html
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # print all URLs in html content
    atags = soup.find_all('a')
    for tag in atags:
        print(tag.get('href'))
else:
    print(f"Failed to retrieve content: status code {response.status_code}")
