import aiofiles
from pathlib import Path
from aiocsv import AsyncDictWriter
from cryptofeed import FeedHandler

from cryptofeed.defines import L2_BOOK, TRADES
from cryptofeed.exchanges import Binance, Bybit, Huobi

from cryptofeed.types import OrderBook


common_cols = ['exchange', 'symbol', 'ts']
asks_cols = [f'ASK{i}' for i in range(25)]
vasks_cols = [f'VASK{i}' for i in range(25)]

bids_cols = [f'BID{i}' for i in range(25)]
vbids_cols = [f'VBID{i}' for i in range(25)]
csv_cols = common_cols + asks_cols + vasks_cols + bids_cols + vbids_cols


async def save_book(book: OrderBook, ts):
    asks_d = book.book.asks.to_dict()
    bids_d = book.book.bids.to_dict()
    asks, vasks = asks_d.keys(), asks_d.values()
    bids, vbids = bids_d.keys(), bids_d.values()

    data = {}
    data['exchange'] = book.exchange
    data['symbol'] = book.symbol
    data['ts'] = ts

    data.update(dict(zip(bids_cols, bids)))
    data.update(dict(zip(asks_cols, asks)))
    data.update(dict(zip(vasks_cols, vasks)))
    data.update(dict(zip(vbids_cols, vbids)))
    await to_csv('data/snapshots.csv', data)


async def to_csv(fname, data):
    async with aiofiles.open(fname, mode="a+", encoding="utf-8") as afp:
        writer = AsyncDictWriter(afp, data.keys(), restval="NULL")
        await writer.writerow(data)


async def save_trade(trade, ts):
    data = {
        'exchange': trade.exchange,
        'symbol': trade.symbol,
        'side': trade.side,
        'amount': trade.amount,
        'price': trade.price,
        'id': trade.id,
        'type': trade.type,
        'timestamp': trade.timestamp,
        'ts': ts
    }
    await to_csv('data/trades.csv', data)


def get_data():
    config = {'log': {'filename': 'demo.log',
                      'level': 'DEBUG', 'disabled': False}}
    Path('data').mkdir(exist_ok=True)
    f = FeedHandler(config=config)
    f.add_feed(
        Binance(symbols=['BTC-USDT', 'ETH-USDT', 'EOS-USDT', 'LINK-USDT',
                         'BNB-USDT'],
                max_depth=25,
                channels=[L2_BOOK, TRADES],
                callbacks={L2_BOOK: save_book, TRADES: save_trade})
    )

    f.add_feed(
        Bybit(symbols=['BTC-USDT-PERP', 'ETH-USDT-PERP', 'EOS-USDT-PERP',
                       'LINK-USDT-PERP', 'BNB-USDT-PERP'],
              max_depth=25,
              channels=[L2_BOOK, TRADES],
              callbacks={L2_BOOK: save_book, TRADES: save_trade})
    )

    f.add_feed(
        Huobi(symbols=['BTC-USDT', 'ETH-USDT', 'EOS-USDT', 'LINK-USDT',
                       'BNB-USDT'],
              max_depth=25,
              channels=[L2_BOOK, TRADES],
              callbacks={L2_BOOK: save_book, TRADES: save_trade})
    )

    f.run()


get_data()
