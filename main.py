import yfinance as yf
import pandas as pd
import sqlite3
import json
import datetime
import time


def load_tickers_from_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

        # Iterate through all the entries (keys are like "0", "1", "2", etc.)
        tickers = [data[key]['ticker'] for key in data]
        return tickers

def get_option_data():
    # Load tickers from the JSON file
    tickers = load_tickers_from_json('company_tickers.json')

    # Loop through each ticker and fetch options data
    for ticker_symbol in tickers:
        print(f"Processing {ticker_symbol}...")

        try:
            ticker = yf.Ticker(ticker_symbol)
            expiration_dates = ticker.options  # Get available expiration dates

            if not expiration_dates:
                print(f"No options data found for {ticker_symbol}")
                continue  # Skip this ticker if no options data is available

            # Connect to SQLite database (or create it if it doesn't exist)
            conn = sqlite3.connect("options_database.db")
            cursor = conn.cursor()

            # Create a table for the ticker (calls and puts combined) if it doesn't exist
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {ticker_symbol}_options (
                    contractSymbol TEXT,
                    type TEXT,
                    expiration_date TEXT,
                    strike REAL,
                    lastPrice REAL,
                    bid REAL,
                    ask REAL,
                    volume INTEGER,
                    openInterest INTEGER,
                    impliedVolatility REAL,
                    timestamp TEXT,
                    PRIMARY KEY (contractSymbol, timestamp)
                )
            ''')

            # Loop through all expiration dates and fetch options data
            for date in expiration_dates:
                option_chain = ticker.option_chain(date)
                
                # Combine calls and puts into one DataFrame
                calls = option_chain.calls.copy()
                puts = option_chain.puts.copy()

                # Add a column to indicate whether it's a 'call' or 'put'
                calls["type"] = "call"
                puts["type"] = "put"

                # Add expiration date column
                calls["expiration_date"] = date
                puts["expiration_date"] = date

                # Add timestamp for when the data was entered
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                calls["timestamp"] = timestamp
                puts["timestamp"] = timestamp

                # Keep only the required columns to match SQL schema
                calls = calls[["contractSymbol", "type", "expiration_date", "strike", "lastPrice", "bid", "ask", "volume", "openInterest", "impliedVolatility", "timestamp"]]
                puts = puts[["contractSymbol", "type", "expiration_date", "strike", "lastPrice", "bid", "ask", "volume", "openInterest", "impliedVolatility", "timestamp"]]

                # Insert data into the SQLite database for both calls and puts
                # Use `if_exists="append"` to add new rows but not overwrite existing data
                calls.to_sql(f"{ticker_symbol}_options", conn, if_exists="append", index=False)
                puts.to_sql(f"{ticker_symbol}_options", conn, if_exists="append", index=False)

                print(f"Saved options data for {ticker_symbol} on {date}.")

                # Sleep for 1 second between each ticker's option chain data retrieval
                time.sleep(1)

            # Close the database connection
            conn.close()
            print(f"All data saved for {ticker_symbol}.")

        except Exception as e:
            print(f"Error processing {ticker_symbol}: {e}")

    # After processing all tickers, sleep for 10 minutes and repeat the process
    print("Waiting for 10 minutes before restarting the process...")
    time.sleep(600)  # Sleep for 10 minutes (600 seconds)

    # Recursively call the function to repeat the process
    get_option_data()

# Run the function to fetch and save options data
get_option_data()
