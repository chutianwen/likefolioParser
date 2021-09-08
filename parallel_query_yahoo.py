from multiprocessing import  Pool, freeze_support
import datetime as dt
import pandas as pd
import numpy as np
import yfinance as yf

PREMARKET = 'PREMARKET'
AFH = 'AFH'


def get_date(data):
    fmts = ('%B %d %Y','%m/%d/%Y','%m/%d/%y','%b %Y','%B%Y','%b %d,%Y',
            '%m-%d-%Y', '%Y-%m-%d')
    for fmt in fmts:
        try:
            return dt.datetime.strptime(data, fmt)
        except:
            pass
    return None


def get_quotes(row):
    ticker, earning_dt, market_time = row['Ticker'], get_date(row['Earning Date']), row['Market Time']
    if earning_dt and market_time:
        # actual is end_dt - 1, need +1 to offset API requirment
        if market_time == PREMARKET:
            start_dt = earning_dt + dt.timedelta(-1)
            end_dt = earning_dt + dt.timedelta(1)
        elif market_time == AFH:
            start_dt = earning_dt
            end_dt = earning_dt + dt.timedelta(2)
        else:
            raise
        start_dt = str(start_dt).split(' ')[0]
        end_dt = str(end_dt).split(' ')[0]

        try:
            stock = yf.Ticker(ticker)
            quotes = stock.history(start=start_dt, end=end_dt).round(2)
            start_quotes = quotes.iloc[0]
            end_quotes = quotes.iloc[1]
            row['Left-day Open'] = start_quotes.Open
            row['Right-day Open'] = end_quotes.Open
            row['Left-day Close'] = start_quotes.Close
            row['Right-day Close'] = end_quotes.Close
            row['Left-day High'] = start_quotes.High
            row['Right-day High'] = end_quotes.High
            row['Left-day Low'] = start_quotes.Low
            row['Right-day Low'] = end_quotes.Low
        except:
            pass

    return row


def parallelize_dataframe(df, func, n_cores=4):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def run(df):
    return df.apply(lambda x: get_quotes(x), axis=1)


if __name__ == '__main__':
    freeze_support()
    data = pd.read_csv('earning_prediction.csv')
    data_with_quotes = parallelize_dataframe(
        data,
        run,
        n_cores=64
    )
    data_with_quotes_sort = data_with_quotes.sort_values(by='Earning Date')
    data_with_quotes_sort.to_csv('earning_prediction_quotes_parallel.csv', index=False)
    print('Done')