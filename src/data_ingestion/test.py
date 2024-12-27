from datetime import datetime, timedelta

import psycopg2

# Database connection details (use your own)
DB_HOST = '127.0.0.1'
DB_PORT = '5432'  # Default PostgreSQL port
DB_NAME = 'volumechecker'
DB_USER = 'postgres'
DB_PASSWORD = 'mysql'

def create_stock_table():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    # Create the schema 'stock_data' if it doesn't exist
    create_schema_query = """
        CREATE SCHEMA IF NOT EXISTS stock_data;
    """
    try:
        cur.execute(create_schema_query)
        print("Schema 'stock_data' created or already exists.")
    except Exception as e:
        print(f"Error creating schema: {e}")

    # Create the stock table in the 'stock_data' schema if it doesn't exist
    create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_data.stock (
            symbol VARCHAR(10) NOT NULL,
            volume INT,
            closing_price DECIMAL(10, 2),
            date DATE NOT NULL,
            PRIMARY KEY (symbol, date)  -- Primary key based on symbol and date
        );
    """
    try:
        cur.execute(create_table_query)
        print("Table 'stock' created or already exists.")
    except Exception as e:
        print(f"Error creating table: {e}")

    cur.close()
    conn.close()


def check_table_exists():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to the database successfully.")

        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Query to check if the table 'test' exists in the 'stock_data' schema
        check_table_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'stock_data' AND table_name = 'test';
        """
        cur.execute(check_table_query)
        result = cur.fetchone()

        if result:
            print(f"Table '{result[0]}' exists in the 'stock_data' schema.")
        else:
            print("Table 'test' does not exist in the 'stock_data' schema.")

        # Close the cursor and connection
        cur.close()
        conn.close()
        print("Database connection closed.")

    except Exception as e:
        print(f"Error connecting to the database: {e}")


def insert_sample_data():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to the database successfully.")

        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Sample data to insert into the 'test' table
        insert_data_query = """
            INSERT INTO stock_data.test (symbol, volume, closing_price, date)
            VALUES (%s, %s, %s, %s)
        """
        data = ('AAPL', 1000000, 175.50, '2024-12-25')

        # Execute the insert statement
        cur.execute(insert_data_query, data)
        conn.commit()
        print("Sample data inserted into 'test' table.")

        # Close the cursor and connection
        cur.close()
        conn.close()
        print("Database connection closed.")

    except Exception as e:
        print(f"Error inserting data: {e}")


def create_table_with_columns():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to the database successfully.")

        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Drop the existing 'test' table if it exists
        drop_table_query = "DROP TABLE IF EXISTS stock_data.test;"
        cur.execute(drop_table_query)
        print("Dropped existing 'test' table (if any).")

        # Create the 'test' table with the required columns
        create_table_query = """
            CREATE TABLE stock_data.test (
                symbol VARCHAR(10) NOT NULL,
                volume INT,
                closing_price DECIMAL(10, 2),
                date DATE NOT NULL,
                PRIMARY KEY (symbol, date)
            );
        """
        cur.execute(create_table_query)
        print("Table 'test' created with columns.")

        # Commit changes and close the connection
        conn.commit()
        cur.close()
        conn.close()
        print("Database connection closed.")

    except Exception as e:
        print(f"Error creating table with columns: {e}")


def create_partitioned_table():
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to the database successfully.")

        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Drop the existing table (if any) to avoid conflicts
        cur.execute("DROP TABLE IF EXISTS stock_data.test CASCADE;")
        print("Dropped existing 'test' table if it existed.")

        # Create the parent partitioned table
        create_parent_table_query = """
            CREATE TABLE stock_data.test (
                symbol VARCHAR(10) NOT NULL,
                volume INT,
                closing_price DECIMAL(10, 2),
                date DATE NOT NULL,
                PRIMARY KEY (symbol, date)
            ) PARTITION BY RANGE (date);
        """
        cur.execute(create_parent_table_query)
        print("Parent partitioned table 'test' created.")

        # Create partitions for specific date ranges (e.g., for each day)
        start_date = datetime(2024, 12, 24)
        for i in range(3):  # Create partitions for 3 days (24-26 Dec)
            partition_date = start_date + timedelta(days=i)
            partition_table_name = f"test_{partition_date.strftime('%d_%b_%Y')}"

            # Create partition for the date range
            create_partition_query = f"""
                CREATE TABLE IF NOT EXISTS stock_data.{partition_table_name} 
                PARTITION OF stock_data.test 
                FOR VALUES FROM ('{partition_date.date()}') TO ('{(partition_date + timedelta(days=1)).date()}');
            """
            cur.execute(create_partition_query)
            print(f"Partition for {partition_table_name} created or already exists.")

        # Commit changes and close the connection
        conn.commit()
        cur.close()
        conn.close()
        print("Database connection closed.")

    except Exception as e:
        print(f"Error creating partitioned table: {e}")


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

        partition_table_name = f"stock_data_{date.strftime('%d_%b_%Y')}"
        check_partition_query = f"""
            SELECT to_regclass('stock_data.{partition_table_name}');
        """
        cur.execute(check_partition_query)
        result = cur.fetchone()

        if result[0] is None:  # If partition doesn't exist
            create_partition_query = f"""
                CREATE TABLE stock_data.{partition_table_name}
                PARTITION OF stock_data
                FOR VALUES FROM ('{date}') TO ('{date + timedelta(days=1)}');
            """
            cur.execute(create_partition_query)
            print(f"Partition for {partition_table_name} created.")
        else:
            print(f"Partition for {partition_table_name} already exists.")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error creating partition for {date}: {e}")

# Step 6: Insert data into partitioned table
def insert_data(df):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        for _, row in df.iterrows():
            insert_query = f"""
                INSERT INTO stock_data (symbol, volume, closing_price, date)
                VALUES (%s, %s, %s, %s);
            """
            cur.execute(insert_query, (row['symbol'], row['volume'], row['closing_price'], row['date']))

        conn.commit()
        print(f"Data inserted for date: {df['date'].iloc[0]}.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error inserting data: {e}")


date_str = "2024-12-26"
date_obj = datetime.strptime(date_str, '%Y-%m-%d')

create_partition_for_date(date_obj)
# Run the function to create the partitioned table
# create_partitioned_table()

# # Run the function to create the table with columns
# create_table_with_columns()
#
# # Run the function to insert sample data into 'test' table
# insert_sample_data()
# Run the function to check if the 'test' table exists
# check_table_exists()
# Run the function to create the stock table
# create_stock_table()
