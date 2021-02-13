import ccxt

import pandas as pd

def fetch_ticks(symbol: str, from_id: int, limit: int, exchange) -> pd.DataFrame:

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

    trades_df = fetch_ticks(
        symbol=symbol,
        from_id=0,
        limit=1000,
        exchange=exchange
    )

    trades_df.to_parquet(f"{symbol.replace('/','')}.parquet.gzip", compression='gzip')


if __name__ == "__main__":
    main()