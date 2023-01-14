                                    #CryptoDataDownload
# First import the libraries that we need to use
import pandas as pd
import requests
import json

import asyncio
import ssl
import pandas as pd
import os

final_date_format = '%Y-%m-%d %H:%M'
ssl._create_default_https_context = ssl._create_unverified_context

hourly_url = "https://www.cryptodatadownload.com/cdd/Coinbase_BTCUSD_1h.csv"
daily_url = "https://www.cryptodatadownload.com/cdd/Coinbase_BTCUSD_d.csv"

symbel_ = "BTC/USD" 

async def save_symbol_to_csv(symbol: str, timeframe:str, date_format: str, file_name: str):
    df = fetch_data(symbol=symbol, timeframe=timeframe)
    df = df.dropna(thresh=2)
    # df.columns = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'VolumeFrom', 'VolumeTo']
    df['Date'] = pd.to_datetime(df['Date'], format=date_format)
    df['Date'] = df['Date'].dt.strftime(final_date_format)
    final_path = os.path.join('data', 'input', file_name)
    df.to_csv(final_path, index=False)
    return df

    # csv = pd.read_csv(url, header=1)
    # csv = csv.dropna(thresh=2)
    # csv.columns = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'VolumeFrom', 'VolumeTo']
    # csv['Date'] = pd.to_datetime(csv['Date'], format=date_format)
    # csv['Date'] = csv['Date'].dt.strftime(final_date_format)

    # final_path = os.path.join('data', 'input', file_name)
    # csv.to_csv(final_path, index=False)

    # return csv


async def save_as_csv(symbol: str):
    pair_split = symbol.split('/')  # symbol must be in format XXX/XXX ie. BTC/EUR
    symbol2 = pair_split[0] + '-' + pair_split[1]

    tasks = [save_symbol_to_csv(symbol, "1h", '%Y-%m-%d %I-%p', f'coinbase-1h-{symbol2}.csv'),
             save_symbol_to_csv(symbol, "d", '%Y-%m-%d', f'coinbase-1d-{symbol2}.csv')]
    # also FIRST_EXCEPTION and ALL_COMPLETED (default)
    done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
    print('>> done: ', done)
    print('>> pending: ', pending)  # will be empty if using default return_when setting


def download_data_async():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(save_as_csv(symbel_))
    loop.close()

def fetch_data(symbol: str,timeframe:str = "d"):
    pair_split = symbol.split('/')  # symbol must be in format XXX/XXX ie. BTC/EUR
    symbol_ = pair_split[0] + '-' + pair_split[1]

    granularity_map = {
        "1h": 2073600,
        "d": 86400,
    }
    if granularity_map.keys().__contains__(timeframe):
        granularity = granularity_map[timeframe] 
    else:
        timeframe = "D"
        granularity = 86400
    url = f'https://api.pro.coinbase.com/products/{symbol_}/candles?granularity={granularity}'
    response = requests.get(url)
    if response.status_code == 200:  # check to make sure the response from server is good
        json_data = json.loads(response.text)
        # print(json_data)
        data = pd.DataFrame(json_data, columns=['unix', 'low', 'high', 'open', 'close', 'volume'])
        data['unix'] = pd.to_datetime(data['unix'], unit='ms')  # convert to a readable date
        # data['vol_fiat'] = data['volume'] * data['close']      # multiply the BTC volume by closing price to approximate fiat volume

        # if we failed to get any data, print an error...otherwise write the file
        if data is None:
            print("Did not return any data from Coinbase for this symbol")
        else:
            # data.columns = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'VolumeFrom', 'VolumeTo']
            data.insert(loc=1, column="Symbol", value=symbol)
            print(data.columns)
            # data["Symbol"] = symbol
            data.columns = ['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
            print(data)
            # data.to_csv(f'Coinbase_{pair_split[0] + pair_split[1]}_{timeframe}.csv', index=False)
        return data
    else:
        print("Did not receieve OK response from Coinbase API")
    return pd.DataFrame(columns=['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume'])


if __name__ == "__main__":
    # we set which pair we want to retrieve data for
    # pair = "BTC/USD"
    # fetch_data(symbol=pair)
    download_data_async()