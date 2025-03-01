import yfinance as yf
import pandas as pd
import sqlite3
import json
import datetime
import time
import math
from scipy.stats import norm


# Black-Scholes Model to calculate Greeks
def black_scholes_greeks(S, K, T, r, sigma, option_type='call'):
    # S = Current price of the underlying asset
    # K = Strike price of the option
    # T = Time to expiration in years
    # r = Risk-free rate (annualized)
    # sigma = Volatility of the underlying asset
    # option_type = 'call' or 'put'
    
    # Calculate d1 and d2 for the Black-Scholes model
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    
    # Greeks calculations
    if option_type == 'call':
        delta = norm.cdf(d1)
        gamma = norm.pdf(d1) / (S * sigma * math.sqrt(T))
        theta = (-S * norm.pdf(d1) * sigma / (2 * math.sqrt(T))) - (r * K * math.exp(-r * T) * norm.cdf(d2))
        vega = S * norm.pdf(d1) * math.sqrt(T)
        rho = K * T * math.exp(-r * T) * norm.cdf(d2)
    elif option_type == 'put':
        delta = norm.cdf(d1) - 1
        gamma = norm.pdf(d1) / (S * sigma * math.sqrt(T))
        theta = (-S * norm.pdf(d1) * sigma / (2 * math.sqrt(T))) + (r * K * math.exp(-r * T) * norm.cdf(-d2))
        vega = S * norm.pdf(d1) * math.sqrt(T)
        rho = -K * T * math.exp(-r * T) * norm.cdf(-d2)
    
    return delta, gamma, theta, vega, rho


# Load tickers from the JSON file
def load_tickers_from_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
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
                    delta REAL,
                    gamma REAL,
                    theta REAL,
                    vega REAL,
                    rho REAL,
                    timestamp TEXT,
                    PRIMARY KEY (contractSymbol, timestamp)
                )
            ''')

            # Get the current price of the underlying asset (S)
            S = ticker.history(period="1d")['Close'].iloc[0]  # Latest closing price

            # Risk-free rate (you can use a fixed value or fetch from an external source)
            r = 0.05  # Example: 5% risk-free rate

            # Loop through all expiration dates and fetch options data
            for date in expiration_dates:
                option_chain = ticker.option_chain(date)
                
                # Combine calls and puts into one DataFrame
                calls = option_chain.calls.copy()
                puts = option_chain.puts.copy()

                # Add a column to indicate whether it's 'call' or 'put'
                calls["type"] = "call"
                puts["type"] = "put"

                # Add expiration date column
                calls["expiration_date"] = date
                puts["expiration_date"] = date

                # Add timestamp for when the data was entered
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                calls["timestamp"] = timestamp
                puts["timestamp"] = timestamp

                # Calculate Greeks for calls and puts
                for idx, row in calls.iterrows():
                    # Time to expiration (T) in years
                    T = (datetime.datetime.strptime(date, "%Y-%m-%d") - datetime.datetime.now()).days / 365.0
                    sigma = row['impliedVolatility']  # Implied volatility
                    delta, gamma, theta, vega, rho = black_scholes_greeks(S, row['strike'], T, r, sigma, option_type='call')
                    
                    # Insert Greeks into the DataFrame
                    calls.at[idx, 'delta'] = delta
                    calls.at[idx, 'gamma'] = gamma
                    calls.at[idx, 'theta'] = theta
                    calls.at[idx, 'vega'] = vega
                    calls.at[idx, 'rho'] = rho

                for idx, row in puts.iterrows():
                    # Time to expiration (T) in years
                    T = (datetime.datetime.strptime(date, "%Y-%m-%d") - datetime.datetime.now()).days / 365.0
                    sigma = row['impliedVolatility']  # Implied volatility
                    delta, gamma, theta, vega, rho = black_scholes_greeks(S, row['strike'], T, r, sigma, option_type='put')
                    
                    # Insert Greeks into the DataFrame
                    puts.at[idx, 'delta'] = delta
                    puts.at[idx, 'gamma'] = gamma
                    puts.at[idx, 'theta'] = theta
                    puts.at[idx, 'vega'] = vega
                    puts.at[idx, 'rho'] = rho

                # Keep only the required columns to match SQL schema
                calls = calls[["contractSymbol", "type", "expiration_date", "strike", "lastPrice", "bid", "ask", "volume", "openInterest", "impliedVolatility", "delta", "gamma", "theta", "vega", "rho", "timestamp"]]
                puts = puts[["contractSymbol", "type", "expiration_date", "strike", "lastPrice", "bid", "ask", "volume", "openInterest", "impliedVolatility", "delta", "gamma", "theta", "vega", "rho", "timestamp"]]

                # Insert data into the SQLite database for both calls and puts
                calls.to_sql(f"{ticker_symbol}_options", conn, if_exists="append", index=False)
                puts.to_sql(f"{ticker_symbol}_options", conn, if_exists="append", index=False)

                print(f"Saved options data with Greeks for {ticker_symbol} on {date}.")

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
