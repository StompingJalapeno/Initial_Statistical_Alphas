# BTC : Ensemble Strategy with Dynamic Stop Loss (Novel)

import uuid
import pandas as pd
import os
from untrade.client import Client
import numpy as np

# -------INITIAL DATA PROCESSING--------#
def process_data(df):
    # Calculate shadow
    df['shadow'] = df.apply(
        lambda row: row['close'] - row['high'] if row['close'] > row['open'] else row['close'] - row['low'], axis=1)
    df['shadow'] = df['shadow'] / abs(df['close'] - df['open'])

    # Calculate the absolute difference |open - close| for each row
    df['abs_diff'] = abs(df['open'] - df['close'])

    # Calculate the rolling 15-day average of the absolute difference with standard deviation adjustment
    df['avg_abs_diff_15d'] = df['abs_diff'].rolling(window=15, min_periods=1).mean() + \
                             1.8 * df['abs_diff'].rolling(window=15, min_periods=1).std()

    # Apply the condition and store the result in a new column
    df['new_column'] = df.apply(
        lambda row: (-1) * 1 / row['abs_diff'] * row['shadow']
        if row['shadow'] != 0 and row['abs_diff'] > row['avg_abs_diff_15d'] else 0, axis=1)

    # Generate buy/sell signals based on `new_column`
    df['signal_1'] = df.apply(
        lambda row: 1 if -0.005 < row['new_column'] < 0
        else (-1 if 0 < row['new_column'] < 0.005 else 0), axis=1)

    # Calculate ATR for Supertrend
    atr_period = 5
    multiplier = 3
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

    # Calculate Ichimoku Cloud components
    df['Tenkan-sen'] = (df['high'].rolling(9).max() + df['low'].rolling(9).min()) / 2  # Conversion Line
    df['Kijun-sen'] = (df['high'].rolling(26).max() + df['low'].rolling(26).min()) / 2  # Base Line
    df['Senkou Span A'] = (df['Tenkan-sen'] + df['Kijun-sen']) / 2  # Leading Span A
    df['Senkou Span B'] = (df['high'].rolling(52).max() + df['low'].rolling(52).min()) / 2  # Leading Span B
    df['Chikou Span'] = df['close'].shift(+26)  # Lagging Span

    # Drop NaN values generated during the rolling operations
    df.dropna(inplace=True)

    # Generate Buy/Sell signals for Ichimoku and Supertrend
    df['Buy Signal'] = (df['close'] > df['Supertrend']) & (df['close'] > df['Senkou Span A'])
    df['Sell Signal'] = (df['close'] < df['Supertrend']) & (df['close'] < df['Senkou Span B'])

    # Combine Buy and Sell signals into one column called 'signal_2'
    df['signal_2'] = 0  # Default to 0
    df.loc[df['Buy Signal'], 'signal_2'] = 1  # Buy signal is 1
    df.loc[df['Sell Signal'], 'signal_2'] = -1  # Sell signal is -1

    # Combine both signals with weighted averaging
    signal_1_weight = 0.6
    signal_2_weight = 0.4
    df['combined_signal'] = (signal_1_weight * df['signal_1'] + signal_2_weight * df['signal_2']).apply(np.sign)
    df['signal'] = df['combined_signal'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))

    return df



# -------STRATEGY LOGIC--------#
def strat(df):
    # Initialize trading variables
    df['signals'] = 0
    df['trade_type'] = "hold"
    holding = False
    capital = 1000  # Starting capital
    shares_bought = 0
    initial_capital = 0
    trades = []

    # Implement risk management parameters
    stop_loss_pct = 0.05  # 5% stop loss (used for rolling stop)
    take_profit_pct = 0.1  # 10% take profit
    highest_price = 0  # Highest price for long positions
    lowest_price = float('inf')  # Lowest price for short positions

    # Sideways market filter based on Ichimoku Cloud (using Kumo flatness and price within the cloud)
    sideways_filter_threshold = 0.005  # This threshold can be adjusted

    for index, row in df.iterrows():
        signal = row['signal']
        price = row['close']
        atr = row['ATR']
        price_in_cloud = row['close'] > row['Senkou Span A'] and row['close'] < row['Senkou Span B']
        is_cloud_flat = abs(row['Senkou Span A'] - row['Senkou Span B']) < sideways_filter_threshold
        date = row['datetime']

        # Sideways Market: Entering long positions near support (Senkou Span B)
        if price_in_cloud and is_cloud_flat:
            # Buy near support (Senkou Span B) when the market is sideways
            if price <= row['Senkou Span B']:
                df.at[index, 'signals'] = 1
                df.at[index, 'trade_type'] = "long"
                shares_bought = capital / price
                initial_capital = capital
                holding = True
                buying_price = price
                buying_date = date
            # Short near resistance (Senkou Span A)
            elif price >= row['Senkou Span A']:
                df.at[index, 'signals'] = -1
                df.at[index, 'trade_type'] = "short"
                shares_bought = capital / price
                initial_capital = capital
                holding = True
                buying_price = price
                buying_date = date

        # Trend Following (regular Supertrend-based strategy)
        elif not holding and signal == 1:
            df.at[index, 'signals'] = 1
            df.at[index, 'trade_type'] = "long"
            shares_bought = capital / price
            initial_capital = capital
            holding = True
            buying_price = price
            highest_price = price  # Set the highest price as the buying price
            buying_date = date

        # Sell condition or rolling stop-loss conditions
        elif holding:
            # Check stop-loss and take-profit for long positions
            if signal == -1 or (price <= highest_price * (1 - stop_loss_pct)) or (price >= buying_price * (1 + take_profit_pct)):
                df.at[index, 'signals'] = -1
                df.at[index, 'trade_type'] = "square_off"
                capital = shares_bought * price
                final_capital = capital
                holding = False
                selling_price = price
                selling_date = date

                # Calculate profit/loss
                profit_loss = final_capital - initial_capital
                trades.append({
                    'buying_date': buying_date,
                    'buying_price': buying_price,
                    'selling_date': selling_date,
                    'selling_price': selling_price,
                    'initial_capital': initial_capital,
                    'final_capital': final_capital,
                    'profit_loss': profit_loss
                })

            # Update the rolling stop-loss for long positions (price increases)
            if price > highest_price:
                highest_price = price  # Update the highest price seen

            # Apply the rolling stop-loss
            if price <= highest_price * (1 - stop_loss_pct):
                df.at[index, 'signals'] = -1
                df.at[index, 'trade_type'] = "square_off"
                capital = shares_bought * price
                final_capital = capital
                holding = False
                selling_price = price
                selling_date = date
                profit_loss = final_capital - initial_capital
                trades.append({
                    'buying_date': buying_date,
                    'buying_price': buying_price,
                    'selling_date': selling_date,
                    'selling_price': selling_price,
                    'initial_capital': initial_capital,
                    'final_capital': final_capital,
                    'profit_loss': profit_loss
                })

    return df


    
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


# -------BACK TESTING--------#
def perform_backtest(csv_file_path):
    client = Client()
    result = client.backtest(
        file_path=csv_file_path,
        leverage=1,  # Adjust leverage as needed
        jupyter_id="team97_zelta_hpps",
        # result_type = "Q",
    )
    return result


# -------MAIN FUNCTION--------#
def main():
    
    # Loading data
    data = pd.read_csv(r"./data/BTC/BTC_2019_2023_1d.csv")

    
    # Processing data
    processed_data = process_data(data)

    
    # Generating signals
    result_data = strat(processed_data)
    
    # Saving results to csv
    csv_file_path = "btc_2_result.csv"
    result_data.to_csv(csv_file_path, index=False)

    
    # Performing backtesting
    backtest_result = perform_backtest(csv_file_path)
    
    print(backtest_result)
    for value in backtest_result:
        print(value)

if __name__ == "__main__":
    main()
