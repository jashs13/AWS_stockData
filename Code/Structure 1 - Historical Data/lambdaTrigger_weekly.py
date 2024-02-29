import json
import os
import pandas as pd
import numpy as np
import yfinance as yf

stock_ticker = ["AAPL","NFLX","GOOGL","MSFT","AMZN","NVDA","META","TSLA","URTH"]

aapl_stock_df = yf.download(["AAPL"], start='2001-01-01', end='2023-12-01', actions=True, group_by='Ticker', auto_adjust = True)

goog_stock_df = yf.download(['GOOGL'], start='2004-01-01', end='2023-12-01', actions=True, group_by='Ticker', auto_adjust = True)

msft_stock_df = yf.download(['MSFT'], start='2001-01-01', end='2023-12-01', actions=True, group_by='Ticker', auto_adjust = True)

amzn_stock_df = yf.download(['AMZN'], start='2001-01-01', end='2023-12-01', actions=True, group_by='Ticker', auto_adjust = True)

nflx_stock_df = yf.download(['NFLX'], start='2002-01-01', end='2023-12-01', actions=True, group_by='Ticker', auto_adjust = True)

nvda_stock_df = yf.download(['NVDA'], start='2001-01-01', end='2023-12-01', actions=True, group_by='Ticker', auto_adjust = True)

meta_stock_df = yf.download(['META'], start='2012-01-01', end='2023-12-01', actions=True, group_by='Ticker', auto_adjust = True)

tsla_stock_df = yf.download(['TSLA'], start='2010-01-01', end='2023-12-01', actions=True, group_by='Ticker', auto_adjust = True)

urth_fund = yf.download(['URTH'], start='2012-01-01', end='2023-12-01', actions=True, group_by='Ticker', auto_adjust = True)

fi_data = [goog_stock_df, msft_stock_df, amzn_stock_df, nflx_stock_df, nvda_stock_df, meta_stock_df, tsla_stock_df, urth_fund]

for df,name in zip(fi_data,stock_ticker):
    df['ticker'] = name
    

#change index to ticker and date
for df in fi_data:
    df.set_index(['ticker',df.index], inplace=True)
    

def lambda_handler(event, context):
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }