import os
import boto3
import json
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime

s3 = boto3.client('s3')
bucket = 'gascity'
current_date = datetime.now().strftime('%Y/%m/%d/%H')
predictions_key = f'cmc/predictions/{current_date}/cmcdata.csv'
positions_key = f'cmc/trades/trades.csv'
trades_df = pd.DataFrame.from_dict(json.loads(os.environ['TRADES']))

def get_s3_object(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()

def process_trades():
    #get file from s3
    try:
        predictions_df = pd.read_csv(StringIO(get_s3_object(bucket, predictions_key).decode('utf-8')))
    except Exception as e:
        predictions_df = pd.DataFrame()
        print(f"Error fetching s3 predictions: {e}")
    try:
        positions_df = pd.read_csv(StringIO(get_s3_object(bucket, positions_key).decode('utf-8')))
    except Exception as e:
        positions_df = pd.DataFrame()
        print(f"Error fetching positions: {e}")
    
    # Check current positions and sell if needed
    if not positions_df.empty and not predictions_df.empty:
        #merge predictions and positions tables
        positions_df = pd.merge(positions_df, predictions_df[['symbol', 'y']], on='symbol', how='left', suffixes=('', '_pred'))
        positions_df['y'] = positions_df['y_pred'].combine_first(positions_df['y'])
        positions_df.drop('y_pred', axis=1, inplace=True)
        #sell positions
        positions_df['price'] = positions_df['price'].astype(float)
        positions_df['out'] = positions_df['out'].astype(float)
        positions_df['return'] = np.where(
            positions_df['y'] < 2.0,
            positions_df['out'] / (positions_df['amount'] / positions_df['price']),
            0
        )
        positions_df['action'] = np.where(
            positions_df['y'] < 2.0,
            "SELL",
            positions_df['action']
        )
    #process trades
    if not trades_df.empty:
        trades_df['price'] = trades_df['price'].astype(float)
        trades_df['amount'] = trades_df['amount'].astype(float)
        trades_df['out'] =  trades_df['amount'] / trades_df['price']
        #add trades to positions
        positions_df = positions_df._append(trades_df)
        #save positions to s3
        s3.put_object(Bucket=bucket, Key=positions_key, Body=positions_df.to_csv(index=False))
    
if __name__ == "__main__":
    process_trades()