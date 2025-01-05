# ETH : SuperTrend Indicator Based Strategy (Optimized)

import uuid
import pandas as pd
import os
from untrade.client import Client
import numpy as np
import glob


# -------INITIAL DATA PROCESSING--------#
def process_data(df):
    # Calculate ATR
    atr_period=15
    multiplier=3
    df['HL'] = df['high'] - df['low']
    df['HC'] = abs(df['high'] - df['close'].shift(1))
    df['LC'] = abs(df['low'] - df['close'].shift(1))
    df['TR'] = df[['HL', 'HC', 'LC']].max(axis=1)
    df['ATR'] = df['TR'].rolling(atr_period).mean()

    # Calculate Supertrend bands
    df['Upper Band'] = (df['high'] + df['low']) / 2 + multiplier * df['ATR']
    df['Lower Band'] = (df['high'] + df['low']) / 2 - multiplier * df['ATR']
    df['Supertrend'] = np.nan
    df['In Uptrend'] = True

    # Calculate Supertrend and trend direction
    for i in range(1, len(df)):
        if df['close'][i] > df['Upper Band'][i - 1]:
            df.at[i, 'In Uptrend'] = True
        elif df['close'][i] < df['Lower Band'][i - 1]:
            df.at[i, 'In Uptrend'] = False
        else:
            df.at[i, 'In Uptrend'] = df['In Uptrend'][i - 1]

            if df['In Uptrend'][i] and df['Lower Band'][i] < df['Lower Band'][i - 1]:
                df.at[i, 'Lower Band'] = df['Lower Band'][i - 1]
            if not df['In Uptrend'][i] and df['Upper Band'][i] > df['Upper Band'][i - 1]:
                df.at[i, 'Upper Band'] = df['Upper Band'][i - 1]

        df.at[i, 'Supertrend'] = df['Lower Band'][i] if df['In Uptrend'][i] else df['Upper Band'][i]

    # Ensure 'In Uptrend' is a boolean column
    df['In Uptrend'] = df['In Uptrend'].astype(bool)

    # Generate Buy/Sell signals
    df['Buy Signal'] = (df['close'] > df['Supertrend']) & (~df['In Uptrend'].shift(1).fillna(False))
    df['Sell Signal'] = (df['close'] < df['Supertrend']) & (df['In Uptrend'].shift(1).fillna(False))

    # Combine Buy and Sell signals into one column called 'signal'
    df['signal'] = 0  # Default to 0
    df.loc[df['Buy Signal'], 'signal'] = 1  # Buy signal is 1
    df.loc[df['Sell Signal'], 'signal'] = -1  # Sell signal is -1

    # Drop any NaN rows introduced by calculations
    df.dropna(inplace=True)

    return df



# -------STRATEGY LOGIC--------#
def strat(df):
    # Initialize trading variables
    df['signals'] = 0  # Initialize signal column
    first_signal = True  # Flag to check if it's the first signal
    df['trade_type'] = "hold"

    for index, row in df.iterrows():
        if row['signal'] == 1:
            # Buy signal: 1 for first, 2 for subsequent buys
            df.at[index, 'signals'] = 1 if first_signal else 2
            df.at[index, 'trade_type'] = "short_reversal"
            first_signal = False
        elif row['signal'] == -1:
            # Sell signal: -1 for first, -2 for subsequent sells
            df.at[index, 'signals'] = -1 if first_signal else -2
            df.at[index, 'trade_type'] = "long_reversal"
            first_signal = False
        else:
            # Hold signal
            df.at[index, 'trade_type'] = "hold"

    return df



    
# -------BACK TESTING FOR LARGE CSV--------#
 # Following function can be used for every size of file, specially for large files(time consuming,depends on upload speed and file size)
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
             chunk_file_path = f"/tmp/{file_id}_chunk_{chunk_number}.csv"
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



# -------BACK TESTING--------#
def perform_backtest(csv_file_path):
    """
    Perform backtesting using the untrade SDK.

    Parameters:
    - csv_file_path (str): Path to the CSV file containing historical price data and signals.

    Returns:
    - result (generator): Result is a generator object that can be iterated over to get the backtest results.
    """
    # Create an instance of the untrade client
    client = Client()

    # Perform backtest using the provided CSV file path
    result = client.backtest(
        file_path=csv_file_path,
        leverage=1,  # Adjust leverage as needed
        jupyter_id="team97_zelta_hpps", 
        # result_type= "Q",
    )
    return result



# -------MAIN FUNCTION--------#
def main():
    
    # Loading data
    data = pd.read_csv(r"./data/ETH/ETHUSDT_1d.csv")

    # Processing data
    processed_data = process_data(data)
    result_data = strat(processed_data)

    # Generating signals
    csv_file_path = "eth_2_result.csv"

    # Saving results to csv
    result_data.to_csv(csv_file_path, index=False)
     
    # Performing backtesting
    backtest_result = perform_backtest(csv_file_path)
     
    print(backtest_result)
    for value in backtest_result:
        print(value)


if __name__ == "__main__":
    main()