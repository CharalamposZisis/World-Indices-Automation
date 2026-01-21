from api_request import fetch_data
import psycopg2
import time

# function to create the db inside container
def connect_to_db():
    print("Connecting to the PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host = "localhost",
            port = 5000,
            dbname = "db",
            user = "Mpampis",
            password = "db_password"
        )
        print("Connected successfully")
        return conn
    except psycopg2.Error as e:
            print(f"Database connection failed: {e}")

# function to create table for the db
def create_table(conn):
    print("create table if not exitst")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                symbol TEXT NOT NULL,
                date DATE NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume BIGINT,
                PRIMARY KEY (symbol, date)
            );
        """)
        conn.commit()
        print('Table was created')
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise e 
    
# conn = connect_to_db()
# create_table(conn)

def insert_record(conn, data):
    print('Inserting stocks data into the database...')
    cursor = conn.cursor()
    # Extract symbol and time series
    symbol = data["Meta Data"]["2. Symbol"]
    time_series = data["Time Series (Daily)"]
        # Loop over each date in the time series
    for date, values in time_series.items():
        cursor.execute("""
            INSERT INTO PUBLIC.stock_prices(
                symbol,
                date,
                open,
                high,
                low,
                close,
                volume
            ) VALUES (%s, %s, %s, %s, %s, %s, %s);
        """,(
            symbol,
            date,
            float(values["1. open"]),
            float(values["2. high"]),
            float(values["3. low"]),
            float(values["4. close"]),
            int(values["5. volume"])
        )
        )
    conn.commit()
    print("Data successfull inserted")
        
    # except psycopg2.Error as e:
    #     print(f"Error inserting data into database:  {e}")
    #     raise e 
    # Connect to database
    
   
   
# Main script
def main():
    conn = connect_to_db()
    if not conn:
        return

    create_table(conn)

    stocks = ["AAPL", "MSFT", "GOOGL", "IBM"]
    for stock in stocks:
        data = fetch_data(stock)

        # Handle API rate limits
        if "Note" in data:
            print(f"Rate limit reached, waiting 60 seconds...")
            time.sleep(60)
            data = fetch_data(stock)

        insert_record(conn, data)
        time.sleep(15)  # Avoid hitting rate limit

    conn.close()
    print("DB connection closed")

if __name__ == "__main__":
    main()   
   
    
# stocks = ["AAPL", "MSFT", "GOOGL", "IBM"]    
# for stock in stocks:
#     data = fetch_data(stock)    

# conn = connect_to_db()
# create_table(conn)
# insert_record(conn, data)


# def main():
#     try:
#         conn = connect_to_db()
#         if not conn:
#             return  # stop if connection failed

#         create_table(conn)

#         stocks = ["AAPL", "MSFT", "GOOGL", "IBM"]
#         for stock in stocks:
#             data = fetch_data(stock)

#             # Check for rate limit or invalid response
#             if "Note" in data:
#                 print(f"Rate limit hit. Waiting 60 seconds...")
#                 time.sleep(60)
#                 data = fetch_data(stock)  # retry

#             if not isinstance(data, dict) or "Meta Data" not in data or "Time Series (Daily)" not in data:
#                 print(f"Skipping {stock} due to invalid API response")
#                 continue

#             insert_record(conn, data)
#             time.sleep(15)  # wait to avoid hitting API limit
            
#     except Exception as e:
#         print(f"An error occurred during execution: {e}")

#     finally:
#         if 'conn' in locals() and conn:
#             conn.close()
#             print("Database connection closed.")

# main()