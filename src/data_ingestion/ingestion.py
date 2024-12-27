import os
import shutil
import zipfile
from logging.handlers import TimedRotatingFileHandler
import configparser
import psycopg2
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import json
import logging
# Database configuration
DB_HOST = '127.0.0.1'
DB_PORT = '5432'  # Default PostgreSQL port
DB_NAME = 'volumechecker'
DB_USER = 'postgres'
DB_PASSWORD = 'mysql'
json_file_path=""
logger = ""
today = ""
csvfilename = []
def setup_logging():
    # Create a logger
    logger = logging.getLogger('volumeChecker')
    logger.setLevel(logging.DEBUG)  # Set the logging level for the logger

    # Define the log file path
    log_directory = 'logs'  # Specify the log directory (you can change this)
    base_log_filename = 'app'

    # Define the current date
    current_date = today.strftime('%Y-%m-%d')

    # Construct the log filename with the current date
    log_filename = f"{base_log_filename}_{current_date}.log" # Specify the log filename (you can change this)

    # Ensure the log directory exists (create it if necessary)
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Define the full path to the log file
    log_file_path = os.path.join(log_directory, log_filename)

    # Create a TimedRotatingFileHandler
    handler = TimedRotatingFileHandler(
        log_file_path,
        when='midnight',  # Rotate the log file at midnight
        interval=1,  # Rotate every 1 day
        backupCount=30  # Keep backup logs for 30 days (adjust as needed)
    )

    # Define the log format

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    handler.setFormatter(formatter)

    # Remove all existing handlers from the logger
    logger.handlers.clear()

    # Remove any other handlers from the logger (like console handlers) to ensure only file logging
    # logger.handlers = [handler]
    logger.addHandler(handler)
    return logger


def read_config(config_file_path):
    print(config_file_path)
    print("Current working directory:", os.getcwd())
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config.json')
    config_file_path = os.path.abspath(config_path)
    print(f"Resolved config path: {config_file_path}")
    with open(config_file_path, 'r') as config_file:
        config = json.load(config_file)
    return config


# Function to download the bhavcopy for the given date
def download_bhavcopy(date):
    try:
        # Construct the URL for the bhavcopy file (adjust as needed)
        date_format = '%d-%m-%Y'
        nextday = date  # Directly use the datetime object
        next_day = nextday.strftime('%d%b%Y').upper()
        stock_year = nextday.strftime('%Y')
        stock_month = nextday.strftime('%b').upper()
        formatted_date = nextday.strftime('%Y%m%d')
        new_url_pattern = "https://nsearchives.nseindia.com/content/cm"
        new_filename_format = "BhavCopy_NSE_CM_0_0_0_{next_day}_F_0000.csv.zip"
        newbhavcopy = "BhavCopy_NSE_CM_0_0_0_{next_day}_F_0000.csv"

        download_name = new_filename_format.format(next_day=formatted_date)
        filename = newbhavcopy.format(next_day=formatted_date)

        bhavcopy_url = f"{new_url_pattern}/{download_name}"
        # Download the bhavcopy (this will need to be adjusted to match the URL format)
        # response = requests.get(bhavcopy_url)

        # if response.status_code == 200:
        #     zip_file_path = f"bhavcopy_{date.strftime('%d-%b-%Y')}.zip"
        #     with open(zip_file_path, 'wb') as f:
        #         f.write(response.content)
        #     print(f"Bhavcopy downloaded for {formatted_date}")
        #     return zip_file_path
        # else:
        #     print(f"Failed to download bhavcopy for {formatted_date}")
        #     return None

        headers = {"User-Agent": "Mozilla/5.0"}
        # Get the current file's directory (ingestion.py's location)
        current_dir = os.path.dirname(__file__)

        # Construct the path to the data folder
        data_folder_path = os.path.join(current_dir, '..', '..', 'data')
        data_folder_path = os.path.abspath(data_folder_path)
        data_folder_path = "C:\\DATA"
        download_directory = data_folder_path
        temp_directory_path = os.path.join(download_directory, "temp")
        zip_file_name = os.path.join(temp_directory_path, download_name)
        extracted_file_name = os.path.join(temp_directory_path, filename)
        final_destination = os.path.join(download_directory, filename)
        os.makedirs(temp_directory_path, exist_ok=True)
        response = requests.get(bhavcopy_url, headers=headers)
        if response.status_code == 200:
            with open(zip_file_name, 'wb') as f:
                f.write(response.content)
            source_flag = 'new'
        else:
            logger.info(f"Failed to download from new URL. Status code: {response.status_code}")
            add_filename_to_json(filename)
            return False, None

        # Extract and move the file
        if os.path.exists(zip_file_name):
            with zipfile.ZipFile(zip_file_name, 'r') as zip_ref:
                zip_ref.extractall(temp_directory_path)
            shutil.move(extracted_file_name, final_destination)
            shutil.rmtree(temp_directory_path)
            logger.info("File downloaded and extracted successfully!")

            csvfilename.append(final_destination)
            return True, final_destination

    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 404:
            logger.warning("File not found on the server.")
            add_filename_to_json(filename)
        else:
            logger.critical(f"HTTP error occurred: {http_err}")
        return False, None

    except Exception as e:
        logger.critical(f"An error occurred while downloading file: {e}")
        return False, None






# Function to extract and parse the bhavcopy CSV file (assuming the format)
# def parse_bhavcopy(file_name):
#     # import zipfile
#     #
#     # with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
#     #     # Assuming the bhavcopy file is CSV inside the zip
#     #     zip_ref.extractall("bhavcopy_data")
#     #
#     # # Now parse the CSV file
#     # bhavcopy_filename = os.listdir("bhavcopy_data")[0]
#     # file_path = os.path.join("bhavcopy_data", file_name)
#
#     # Use pandas to read the CSV (modify columns as per your bhavcopy format)[
#     try:
#         df = pd.read_csv(file_name)
#         if 'TradDt' in df.columns:
#             date_str = df['TradDt'].iloc[0]
#         elif 'BizDt' in df.columns:
#             date_str = df['BizDt'].iloc[0]
#         else:
#             raise ValueError("No date column ('TradDt' or 'BizDt') found in the CSV.")
#
#         df['symbol'] = df['TckrSymb']
#         df['volume'] = df['TtlTradgVol']
#         df['closing_price'] = df['ClsPric']
#
#         # Select only necessary columns and ensure the data types are correct
#         df = df[['symbol', 'volume', 'closing_price']]
#         date_obj = datetime.strptime(date_str, '%Y-%m-%d')
#         formatted_date1 = date_obj.strftime('%d/%m/%Y')
#        # date = datetime.strptime(formatted_date, "%d/%m/%Y")
#         print(f"Date extracted from CSV: {formatted_date1}")
#
#         df['date'] = formatted_date1  # Use the extracted date
#
#         return df
#
#     except Exception as e:
#         print(f"Error parse csv: {e}")


# Function to create the partitioned table if it doesn't exist
# def create_table_and_partitions():
#     conn = psycopg2.connect(
#         host=DB_HOST,
#         port=DB_PORT,
#         dbname=DB_NAME,
#         user=DB_USER,
#         password=DB_PASSWORD
#     )
#     cur = conn.cursor()
#
#     # Ensure the schema exists (create it if it doesn't exist)
#     create_schema_query = """
#         CREATE SCHEMA IF NOT EXISTS stock_data;
#     """
#     try:
#         cur.execute(create_schema_query)
#         print("Schema 'stock_data' created or already exists.")
#     except Exception as e:
#         print(f"Error creating schema: {e}")
#
#     # Create the parent table in the 'stock_data' schema if it doesn't exist
#     create_parent_table_query = """
#         CREATE TABLE IF NOT EXISTS stock_data.stock_data (
#             symbol VARCHAR(10) NOT NULL,
#             volume INT,
#             closing_price DECIMAL(10, 2),
#             date DATE NOT NULL,
#             PRIMARY KEY (symbol, date)  -- Primary key based on symbol and date
#         ) PARTITION BY RANGE (date);
#     """
#     try:
#         cur.execute(create_parent_table_query)
#         print("Parent table 'stock_data.stock_data' created or already exists.")
#     except Exception as e:
#         print(f"Error creating parent table: {e}")
#
#     # Create partitions for the next few months
#     current_date = datetime.today()
#
#     # Create partitions for the next 3 months (you can adjust the number of months)
#     for i in range(3):
#         partition_date = current_date.replace(day=1) + timedelta(days=i * 30)
#         partition_month = partition_date.strftime('%Y_%m')  # Format: '2024_12'
#
#         # Partition range from the 1st of the month to the 1st of the next month
#         create_partition_query = f"""
#             CREATE TABLE IF NOT EXISTS stock_data.stock_data_{partition_month}
#             PARTITION OF stock_data.stock_data
#             FOR VALUES FROM ('{partition_date.replace(day=1)}') TO ('{(partition_date.replace(day=1) + timedelta(days=32)).replace(day=1)}');
#         """
#
#         try:
#             cur.execute(create_partition_query)
#             print(f"Partition for {partition_month} created or already exists.")
#         except Exception as e:
#             print(f"Error creating partition for {partition_month}: {e}")
#
#     cur.close()
#     conn.close()
# # Function to insert data into the partitioned PostgreSQL table
# def insert_data_into_db(df):
#     # Establish connection to the database
#     conn = psycopg2.connect(
#         host=DB_HOST,
#         port=DB_PORT,
#         dbname=DB_NAME,
#         user=DB_USER,
#         password=DB_PASSWORD
#     )
#     cur = conn.cursor()
#
#     # Loop through the DataFrame and insert data into the correct partition
#     for _, row in df.iterrows():
#         # SQL insert query
#         insert_query = """
#             INSERT INTO stock_data (symbol, date, volume, closing_price)
#             VALUES (%s, %s, %s, %s)
#             ON CONFLICT (symbol, date) DO NOTHING;
#         """
#         data = (row['symbol'], row['date'], row['volume'], row['closing_price'])
#
#         try:
#             cur.execute(insert_query, data)
#         except Exception as e:
#             print(f"Error inserting data for {row['symbol']} on {row['date']}: {e}")
#
#     # Commit changes and close connection
#     conn.commit()
#     cur.close()
#     conn.close()
#     print("Data inserted into the database.")
#
#
# # Function to check if data for a specific date exists
# def check_data_exists(date):
#     conn = psycopg2.connect(
#         host=DB_HOST,
#         port=DB_PORT,
#         dbname=DB_NAME,
#         user=DB_USER,
#         password=DB_PASSWORD
#     )
#     cur = conn.cursor()
#
#     # Check if data already exists for the date in the database
#     table_name = date.strftime('%d-%b-%Y').upper()  # Format: '25-DEC-2024'
#     check_query = f"SELECT 1 FROM \"{table_name}\" WHERE date = '{date}' LIMIT 1;"
#     try:
#         cur.execute(check_query)
#         data_exists = cur.fetchone()
#     except psycopg2.errors.UndefinedTable:
#         data_exists = None
#
#     cur.close()
#     conn.close()
#
#     return data_exists is not None


# Main ingestion function
# def main():
#     global logger
#     global today
#
#     today = datetime.today() - timedelta(days=1)
#     yesterday = datetime.today() - timedelta(days=1)
#
#     # Get yesterday's date
#     logger = setup_logging()
#     print("----------volumeChecker-----------")
#     logger.info("----------volumeChecker-----------")
#     # print()
#     logger.info("Started")
#
#     config = read_config('config/config.json')
#     json_file_path = 'config/inactive_date.json'
#
#     print(f"Data for {today.date()}")
#
#     # Create the partition table for today if it doesn't exist
#     # create_table_if_not_exists(today)
#
#     create_table_and_partitions()
#     # Check if data for today already exists
#     if not check_data_exists(today.date()):
#         # If missing, download and process the bhavcopy for today
#         zip_file_path = download_bhavcopy(today)
#
#         if zip_file_path:
#             # Parse the downloaded bhavcopy and insert data into the database
#             bhavcopy_df = parse_bhavcopy(zip_file_path)
#             insert_data_into_db(bhavcopy_df)
#             os.remove(zip_file_path)  # Clean up the zip file
#     else:
#         print(f"Data for {today.date()} already exists in the database.")

def add_filename_to_json(file_name):
    # logging.info(f"Adding file name {file_name} to JSON list")
    # print(f"Adding file name {file_name} to JSON list")
    logger.info("Updating inactivity period")
    # Check if the JSON file exists
    if os.path.exists(json_file_path):
        # Read the JSON file
        with open(json_file_path, 'r') as json_file:
            file_list = json.load(json_file)

        # Check if file_list is a dictionary instead of a list
        if isinstance(file_list, dict):
            logger.critical(f"Expected a list, but found a dictionary in the JSON file at {json_file_path}.")
            raise ValueError(f"Expected a list, but found a dictionary in the JSON file at {json_file_path}.")
    else:
        # Create an empty list if the file does not exist
        file_list = []

    # Add the file name to the list
    file_list.append(file_name)
    logger.info("Updated inactivity period")
    # Save the updated list back to the JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(file_list, json_file)


# Step 2: Unzip and parse the bhavcopy file
def parse_bhavcopy(file_name):
    try:
        # Assuming that inside the zip there's a CSV file
        # with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        #     zip_ref.extractall(DOWNLOAD_DIR)
        #
        # # Find the extracted CSV file
        # extracted_file = os.listdir(DOWNLOAD_DIR)[0]  # Assuming one CSV file is extracted
        # file_path = os.path.join(DOWNLOAD_DIR, extracted_file)

        # Parse the CSV
        df = pd.read_csv(file_name)
        if 'TradDt' in df.columns:
            date_str = df['TradDt'].iloc[0]
        elif 'BizDt' in df.columns:
            date_str = df['BizDt'].iloc[0]
        else:
            raise ValueError("No date column ('TradDt' or 'BizDt') found in the CSV.")

        df['symbol'] = df['TckrSymb']
        df['volume'] = df['TtlTradgVol']
        df['closing_price'] = df['ClsPric']

        df = df[['symbol', 'volume', 'closing_price']]  # Keep only the relevant columns

        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        formatted_date1 = date_obj.strftime('%d/%m/%Y')
        print(f"Date extracted from CSV: {formatted_date1}")

        df['date'] = formatted_date1  # Use the extracted date

        return df, date_obj  # Return dataframe and formatted date

    except Exception as e:
        print(f"Error parsing CSV: {e}")
        return None, None


# Step 3: Create the parent partitioned table if it doesn't exist
def create_parent_table():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        # Create the parent partitioned table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS stock_data.csvdata (
                symbol VARCHAR(10) NOT NULL,
                volume INT,
                closing_price DECIMAL(10, 2),
                date DATE NOT NULL,
                PRIMARY KEY (symbol, date)
            ) PARTITION BY RANGE (date);
        """
        cur.execute(create_table_query)
        print("Parent partitioned table 'stock_data' created or already exists.")

        # Commit changes and close the connection
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error creating parent partitioned table: {e}")


# Step 4: Create partition for a given date
def create_partition_for_date(date):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        partition_table_name = f"stock_data_{date.strftime('%d_%b_%Y').upper()}"
        # partition_table_name = date
        # Check if the partition already exists
        check_partition_query = f"""
            SELECT to_regclass('csvdata.{partition_table_name}');
        """
        cur.execute(check_partition_query)
        result = cur.fetchone()

        if result[0] is None:  # If the partition doesn't exist, create it
            create_partition_query = f"""
                CREATE TABLE stock_data.{partition_table_name} 
                PARTITION OF stock_data.csvdata
                FOR VALUES FROM ('{date.date()}') TO ('{(date + timedelta(days=1)).date()}');
            """
            cur.execute(create_partition_query)
            print(f"Partition for {partition_table_name} created.")
        else:
            print(f"Partition for {partition_table_name} already exists.")

        # Commit changes and close the connection
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error creating partition for {date}: {e}")


# Step 5: Insert data into partitioned table for the given date
def insert_data_from_df(df, date):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        # Insert data into the partitioned table
        for _, row in df.iterrows():
            insert_query = f"""
                INSERT INTO stock_data.csvdata (symbol, volume, closing_price, date)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO NOTHING;
            """
            cur.execute(insert_query, (row['symbol'], row['volume'], row['closing_price'], date))

        print(f"Data inserted for {date.date()}.")
        conn.commit()

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error inserting data for {date}: {e}")


# Step 6: Skip non-trading days (weekends, holidays)
def is_trading_day(date):
    return date.weekday() < 5  # Weekdays (Mon-Fri) are considered trading days


# Step 7: Get the list of dates from today to the average day in config
def get_dates_from_today_to_avg_day(avg_day_str):
    # config = configparser.ConfigParser()
    # config.read('config.json')  # Ensure the config file is present

    # avg_day_str = config.get('dates', 'average_day')
    # Parse avg_day (e.g., "25" for 25th of current month)
    avg_day = int(avg_day_str)

    # Get today's date
    today = datetime.today().date()

    # Construct a date for the average day (e.g., 25th of the current month)
    try:
        avg_day_date = today.replace(day=avg_day)  # Replace with avg_day in current month
    except ValueError:
        print("Error: avg_day exceeds the number of days in this month.")
        return []

    delta = today - avg_day_date
    dates = []

    # Collect dates from today back to the avg_day_date (skip weekends)
    for i in range(delta.days + 1):
        candidate_date = today - timedelta(days=i)
        if is_trading_day(candidate_date):
            dates.append(candidate_date)

    return dates


# Main function to execute the full pipeline (download, partition creation, data insertion)
def main():
    global logger
    global today

    today = datetime.today()
    cur_date = today
    yesterday = datetime.today() - timedelta(days=1)

    # Get yesterday's date
    logger = setup_logging()
    print("----------volumeChecker-----------")
    logger.info("----------volumeChecker-----------")
    # print()
    logger.info("Started")

    config = read_config('config/config.json')
    json_file_path = 'config/inactive_date.json'
    avgdays = config['avg_day']

    print(f"Data for {today.date()}")
    # Get the list of dates to process
    # dates_to_process = get_dates_from_today_to_avg_day(avgdays)
    downloading = True

    # for date in dates_to_process:
    avgvolcount = 0
    while downloading:

        cur_date = cur_date - timedelta(days=1)
        # Step 1: Download the bhavcopy file for the date
        print(f"Data for {cur_date}")
        bhavstat, bhavcopy_file = download_bhavcopy(cur_date)

        if bhavstat:
            avgvolcount += 1
            if avgvolcount == int(avgdays):
                print(csvfilename)
                downloading = False
            # Step 2: Parse the bhavcopy file
            df, formatted_date = parse_bhavcopy(bhavcopy_file)

            if df is not None:
                # Step 3: Create the parent partitioned table if it doesn't exist
                create_parent_table()

                # Step 4: Create partition for the current date
                create_partition_for_date(formatted_date)

                # Step 5: Insert data into the partitioned table for the current date
                insert_data_from_df(df, formatted_date)


if __name__ == "__main__":
    main()
