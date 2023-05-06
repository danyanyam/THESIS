import datetime as dt
from enum import Enum
from decimal import Decimal
from dataclasses import dataclass, field

from src.parsers.binance import BinanceUpdateMessage
from src.parsers.binance import Trade as BinanceTrade
from src.parsers.binance import Depth as BinanceDepth

from src.parsers.bybit import BybitUpdateMessage, DeltaData as BybitDeltaData
from src.parsers.bybit import SnapshotData as BybitSnapshotData, OBLevel as BybitObLevel
from src.parsers.bybit import Trade as BybitTrade

Update = BinanceUpdateMessage | BybitUpdateMessage


class Side(Enum):
    BUY = 'Buy'
    SELL = 'Sell'
    UNDEFINED = None


@dataclass
class Other:
    ...


@dataclass
class Trade:
    price: Decimal
    quantity: Decimal
    ts: dt.datetime
    symbol: str
    side: Side

    @staticmethod
    def from_update_message(update: Update) -> "Trade":
        return Trade(
            price=update.payload.data.price,
            quantity=update.payload.data.quantity,
            ts=update.payload.data.ts,
            symbol=update.payload.data.symbol,
            side=Side(update.payload.data.side)
        )


@dataclass
class PriceLevel:
    symbol: str
    price: Decimal
    side: Side
    quantity: Decimal = None  # in ByBit cancels no quantity is provided


@dataclass
class Depth:
    ts: dt.datetime
    delete: list[PriceLevel] = field(default_factory=list)
    update: list[PriceLevel] = field(default_factory=list)
    insert: list[PriceLevel] = field(default_factory=list)
    construct: list[PriceLevel] = field(default_factory=list)

    @staticmethod
    def from_update_message(update: Update) -> "Depth":
        if isinstance(update, BinanceUpdateMessage):

            s = update.payload.data.symbol
            asks = [
                PriceLevel(price=price, quantity=size, symbol=s, side=Side.SELL)
                for price, size in update.payload.data.asks
            ]

            bids = [
                PriceLevel(price=price, quantity=size, symbol=s, side=Side.BUY)
                for price, size in update.payload.data.bids
            ]

            return Depth(
                ts=update.payload.data.event_time,
                update=asks + bids
            )

        elif isinstance(update, BybitUpdateMessage):

            if isinstance(update.payload.data, BybitDeltaData):
                inserts = Depth.convert_to_price_level(
                    update.payload.data.insert
                )
                updates = Depth.convert_to_price_level(
                    update.payload.data.update
                )

                deletes = Depth.convert_to_price_level(
                    update.payload.data.delete
                )

                return Depth(
                    ts=update.payload.ts,
                    insert=inserts,
                    update=updates,
                    delete=deletes
                )

            elif isinstance(update.payload.data, BybitSnapshotData):
                return Depth(
                    ts=update.payload.ts,
                    construct=Depth.convert_to_price_level(
                        update.payload.data.order_book
                    )
                )

    @staticmethod
    def convert_to_price_level(source: list[BybitObLevel]) -> list[PriceLevel]:
        return [
            PriceLevel(symbol=i.symbol, price=i.price,
                       quantity=i.size, side=Side(i.side))
            for i in source
        ]


class Decoder:
    def decode(self, update: Update) -> Trade | Depth | Other:
        if isinstance(update.payload.data, (BinanceTrade, BybitTrade)):
            return Trade.from_update_message(update)

        if isinstance(update.payload.data, (BinanceDepth, BybitDeltaData)):
            return Depth.from_update_message(update)

        return Other()
