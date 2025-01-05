# ETH : Double Timeframe Strategy Design

import uuid
import pandas as pd
import os
from untrade.client import Client
import math
import numpy as np
from scipy.stats import genhyperbolic  


# -------INITIAL DATA PROCESSING--------#
def process_data(data_fast, data_slow):
    # Convert datetime columns to proper datetime format
    data_slow['datetime'] = pd.to_datetime(data_slow['datetime'])
    data_fast['datetime'] = pd.to_datetime(data_fast['datetime'])

    """
     Use the following code in case of an error in date-time
    df['datetime'] = df['datetime'].str.split(' ').str[0]  # Keep only the date part
    df1['datetime'] = df1['datetime'].str.split(' ').str[0]  # Keep only the date part
    
    # Convert datetime columns to proper datetime format with the correct format
    df['datetime'] = pd.to_datetime(df['datetime'], format='%d-%m-%Y')
    df1['datetime'] = pd.to_datetime(df1['datetime'], format='%d-%m-%Y')
    
    """
    # Calculate Simple Moving Averages (SMA) for a window of 26
    data_slow['SMA_26'] = data_slow['close'].rolling(window=26).mean() 
    data_fast['SMA_26'] = data_fast['close'].rolling(window=26).mean() 

    # Calculate Simple Moving Averages (SMA) for a window of 14
    data_slow['SMA_14'] = data_slow['close'].rolling(window=14).mean() 
    data_fast['SMA_14'] = data_fast['close'].rolling(window=14).mean() 

    return data_slow, data_fast



# -------STRATEGY LOGIC--------#
def strat(data_fast, data_slow):
    """
    Create a strategy based on indicators or other factors.

    Parameters:
    - data: DataFrame
        The input data containing the necessary columns for strategy creation.

    Returns:
    - DataFrame
        The modified input data with an additional 'signal' column representing the strategy signals.
    """
    initial_capital = 100  # Starting capital
    capital = initial_capital  # Available capital
    holding = False  # Not holding any asset initially
    shares = 0  # Number of shares owned
    entry_price = 0  # Price at which the asset was bought
    highest_portfolio_value = 0  # Track the highest portfolio value since the entry
    portfolio_entry_value = 0  # Track portfolio value at entry

    # Track portfolio values for each day
    data_fast['capital'] = np.nan
    data_fast['shares'] = np.nan
    data_fast['portfolio_value'] = np.nan
    data_fast['signals'] = 0
    data_fast['trade_type'] = "hold"

    data_fast['signal'] = 0
    data_slow['Low_14D_Max'] = data_slow['low'].rolling(window=14).max()
    data_fast['Low_14D_Max'] = data_fast['low'].rolling(window=14).max()

    '''
        The 'loss' column represents the 7-day rolling average of daily changes in the closing price,
        showing the recent trend (negative for downtrend, positive for uptrend).
    '''

    data_slow['loss'] = (data_slow['close']- data_slow['close'].shift(1)).rolling(window=7).mean()
    
    has_bought = 0
    has_sold = 0
    
    for row_fast in range(1, len(data_fast)):
        
        row_slow = math.ceil(row_fast/96) 
        # every 96 rows in 15_min dataset correspond to 1 day in the 1_day dataset (1 day = 24 hours = 24 * 4 (96) 15_min intervals)
    
        if row_slow in data_slow.index:
            
            sma_diff_26d = data_fast.loc[row_fast, 'SMA_26'] - data_slow.loc[row_slow, 'SMA_26']
            sma_diff_14d = data_fast.loc[row_fast, 'SMA_14'] - data_slow.loc[row_slow, 'SMA_14']
            
            if row_fast > 7:

                if sma_diff_26d > 0 and data_slow.loc[row_slow,'loss'] > 0:

                    if has_bought == 0 and data_slow.loc[row_slow-1, 'close'] > data_slow.loc[row_slow-1,'Low_14D_Max'] :
                        
                        data_fast.loc[row_fast, 'signal'] = 1
                        has_bought = 1


                
                elif(data_slow.loc[row_slow-1, 'close'] < data_slow.loc[row_slow-1,'Low_14D_Max']):
                    
                    if has_bought == 1:
                        
                        data_fast.loc[row_fast, 'signal'] = -1
                        has_sold = 1
                   
    
                elif sma_diff_14d < 0:
                    
                    if has_sold == 0 and has_bought == 1:
                        
                        data_fast.loc[row_fast, 'signal'] = -1
                        has_sold = 1
    
                if has_bought == 1 and has_sold == 1:
                    
                    has_bought = 0
                    has_sold = 0
                
                
    
    # Implementing trading logic
    for index, row in data_fast.iterrows():
        # When a buy signal occurs and not holding any shares
        if row['signal'] == 1 and not holding:
            data_fast.at[index, 'signals'] = 1
            data_fast.at[index, 'trade_type'] = "long"
            shares = capital / row['close']
            capital -= shares * row['close']
            holding = True
            entry_price = row['close']
            portfolio_entry_value = capital + (shares * entry_price)
            highest_portfolio_value = portfolio_entry_value

        # When a sell signal occurs and holding shares
        elif row['signal'] == -1 and holding:
            data_fast.at[index, 'signals'] = -1
            data_fast.at[index, 'trade_type'] = "square_off"
            capital += shares * row['close']
            shares = 0
            holding = False

        # Track capital, shares, and portfolio value at each step
        data_fast.at[index, 'capital'] = capital
        data_fast.at[index, 'shares'] = shares
        data_fast.at[index, 'portfolio_value'] = capital + (shares * row['close'])

    return data_fast




# -------BACK TESTING--------#
def perform_backtest(csv_file_path):
    client = Client()
    result = client.backtest(
        jupyter_id="team97_zelta_hpps",  # the one you use to login to jupyter.untrade.io
        file_path=csv_file_path,
        leverage=1,  # Adjust leverage as needed
    )
    return result

    
# -------BACK TESTING FOR LARGE CSV--------#
# Following function can be used for every size of file, specially for large files(time consuming, depends on upload speed and file size)
def perform_backtest_large_csv(csv_file_path):
    client = Client()
    file_id = str(uuid.uuid4())
    chunk_size = 90 * 1024 * 1024
    total_size = os.path.getsize(csv_file_path)
    total_chunks = (total_size + chunk_size - 1) // chunk_size
    chunk_number = 0
    if total_size <= chunk_size:
        total_chunks = 1
        # Normal Backtest
        result = client.backtest(
            file_path=csv_file_path,
            leverage=1,
            jupyter_id="team97_zelta_hpps",
            # result_type="Q",
        )
        for value in result:
            print(value)

        return result

    with open(csv_file_path, "rb") as f:
        while True:
            chunk_data = f.read(chunk_size)
            if not chunk_data:
                break
            chunk_file_path = f"/tmp/{file_id}chunk{chunk_number}.csv"
            with open(chunk_file_path, "wb") as chunk_file:
                chunk_file.write(chunk_data)

            # Large CSV Backtest
            result = client.backtest(
                file_path=chunk_file_path,
                leverage=1,
                jupyter_id="team97_zelta_hpps",
                file_id=file_id,
                chunk_number=chunk_number,
                total_chunks=total_chunks,
                # result_type="Q",
            )

            for value in result:
                print(value)

            os.remove(chunk_file_path)
            chunk_number += 1

    return result


# -------MAIN FUNCTION--------#
def main():
    
    # Loading data
    data_slow = pd.read_csv(r"./data/ETH/ETHUSDT_1d.csv")
    data_fast = pd.read_csv(r"./data/ETH/ETHUSDT_15m.csv")

    # Processing data
    processed_data_slow, processed_data_fast = process_data(data_fast, data_slow)

    # Generating signals
    result_data = strat(processed_data_fast, processed_data_slow)

    # Saving results to csv
    csv_file_path = "eth_1_result.csv"
    result_data.to_csv(csv_file_path, index=False)

    # Performing backtesting
    backtest_result = perform_backtest_large_csv(csv_file_path)


if __name__ == "__main__":
    main()
