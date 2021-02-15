import ccxt

import pandas as pd
# import pyarrow.parquet as pq


# def get_last_id(symbol: str) -> int:
#     meta_data = pq.ParquetFile(f"{symbol}.parquet.gzip")
#     return meta_data.metadata.num_rows


def fetch_ticks(symbol: str, from_id: int, limit: int, exchange) -> pd.DataFrame:
    """
    Download historical tick data for a specific pair on Binance
    @param symbol: string, e.g. 'BTC/USDT'
    @param from_id: integer, starting point to download
    @param limit: integer, number of ticks per request
    @param exchange: ccxt exchange object
    @return pandas dataframe with ['id', 'timestamp', 'price', 'amount']
    """
    trades_list = []

    while True:
        try:
            trades = exchange.fetch_trades(symbol=symbol, params={'fromId': from_id, 'limit': limit})
            if len(trades) == 0:
                break
            trades_list += [ {k: t[k] for k in ['id', 'timestamp', 'price', 'amount']} for t in trades ]
            from_id += limit
        except Exception as e:
            print(f"Downloading ticks for {symbol} stopped at id {from_id}, because of {e}")
            break

    return pd.DataFrame( trades_list ).astype(dtype={'id': 'int64', 'timestamp': 'int64', 'price': 'float32', 'amount': 'float32'})

def main():

    exchange = ccxt.binance()
    symbol = 'SUSHI/USDT'

    try:
        trades_df = pd.read_parquet(f"{symbol.replace('/','')}.parquet.gzip")
        from_id = len(trades_df)
    except Exception as e:
        trades_df = pd.DataFrame()
        from_id = 0

    new_trades_df = fetch_ticks(
        symbol=symbol,
        from_id=from_id,
        limit=1000,
        exchange=exchange
    )

    trades_df.append(new_trades_df).to_parquet(f"{symbol.replace('/','')}.parquet.gzip", compression='gzip')


if __name__ == "__main__":
    main()
