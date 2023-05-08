import datetime as dt
from enum import Enum
from decimal import Decimal
from dataclasses import dataclass, field

from src.parsers.binance import BinanceUpdateMessage
from src.parsers.binance import BinanceTradeData
from src.parsers.binance import BinanceDepthData

from src.parsers.bybit import BybitUpdateMessage, BybitDeltaData
from src.parsers.bybit import BybitSnapshotData, BybitOBLevel
from src.parsers.bybit import BybitTradeData

Update = BinanceUpdateMessage | BybitUpdateMessage


class Side(Enum):
    BUY = 'Buy'
    SELL = 'Sell'
    UNDEFINED = None


@dataclass
class Other:
    ...


@dataclass
class GenericTrade:
    price: Decimal
    quantity: Decimal
    ts: dt.datetime
    symbol: str
    side: Side

    @staticmethod
    def from_update_message(update: Update) -> "GenericTrade":
        return GenericTrade(
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
class GenericDepth:
    ts: dt.datetime
    delete: list[PriceLevel] = field(default_factory=list)
    update: list[PriceLevel] = field(default_factory=list)
    insert: list[PriceLevel] = field(default_factory=list)
    construct: list[PriceLevel] = field(default_factory=list)

    @staticmethod
    def from_update_message(update: Update) -> "GenericDepth":
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

            return GenericDepth(
                ts=update.payload.data.event_time,
                update=asks + bids
            )

        elif isinstance(update, BybitUpdateMessage):

            if isinstance(update.payload.data, BybitDeltaData):
                inserts = GenericDepth.convert_to_price_level(
                    update.payload.data.insert
                )
                updates = GenericDepth.convert_to_price_level(
                    update.payload.data.update
                )

                deletes = GenericDepth.convert_to_price_level(
                    update.payload.data.delete
                )

                return GenericDepth(
                    ts=update.payload.ts,
                    insert=inserts,
                    update=updates,
                    delete=deletes
                )

            elif isinstance(update.payload.data, BybitSnapshotData):
                return GenericDepth(
                    ts=update.payload.ts,
                    construct=GenericDepth.convert_to_price_level(
                        update.payload.data.order_book
                    )
                )

    @staticmethod
    def convert_to_price_level(source: list[BybitOBLevel]) -> list[PriceLevel]:
        return [
            PriceLevel(symbol=i.symbol, price=i.price,
                       quantity=i.size, side=Side(i.side))
            for i in source
        ]


class Decoder:

    def is_trade(self, decoded: GenericTrade | GenericDepth):
        return isinstance(decoded, GenericTrade)

    def is_depth(self, decoded: GenericTrade | GenericDepth):
        return isinstance(decoded, GenericDepth)

    def decode(self, update: Update) -> GenericTrade | GenericDepth | Other:

        if isinstance(update.payload.data, (BinanceTradeData,
                                             BybitTradeData)):
            return GenericTrade.from_update_message(update)

        if isinstance(update.payload.data,
                       (BinanceDepthData, BybitDeltaData, BybitSnapshotData)):
            return GenericDepth.from_update_message(update)

        return Other()
