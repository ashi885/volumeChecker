import pandas as pd
import psycopg2



DB_HOST = '127.0.0.1'
DB_PORT = '5432'  # Default PostgreSQL port
DB_NAME = 'volumechecker'
DB_USER = 'postgres'
DB_PASSWORD = 'mysql'


def create_volchecker_table():
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

        # Create volchecker table if it doesn't exist
        create_volchecker_table_query = """
            CREATE TABLE IF NOT EXISTS stock_data.volchecker (
                symbol VARCHAR(10) PRIMARY KEY,
                avg_volume INT,
                current_volume INT
            );
        """
        cur.execute(create_volchecker_table_query)
        print("Volchecker table created or already exists.")

        # Commit and close the connection
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error creating volchecker table: {e}")

def calculate_avg_volume():
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

        # Fetch unique symbols from the csvdata table
        cur.execute("SELECT DISTINCT symbol FROM stock_data.csvdata;")
        symbols = cur.fetchall()

        # Loop through each symbol to calculate the average volume
        for symbol in symbols:
            symbol = symbol[0]

            # Fetch the last 25 days of data for the symbol
            query = f"""
                SELECT volume
                FROM stock_data.csvdata
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT 25;
            """
            cur.execute(query, (symbol,))
            volumes = cur.fetchall()

            # Calculate the average volume
            avg_volume = sum([v[0] for v in volumes]) // len(volumes)

            # Insert the average volume into the volchecker table
            insert_query = """
                INSERT INTO stock_data.volchecker (symbol, avg_volume, current_volume)
                VALUES (%s, %s, 0)
                ON CONFLICT (symbol) 
                DO UPDATE SET avg_volume = %s;
            """
            cur.execute(insert_query, (symbol, avg_volume, avg_volume))

        # Commit and close the connection
        conn.commit()
        cur.close()
        conn.close()

        print("Average volume for all symbols calculated and inserted.")

    except Exception as e:
        print(f"Error calculating average volume: {e}")


create_volchecker_table()
calculate_avg_volume()