from decimal import Decimal
from order_book import OrderBook
from dataclasses import dataclass, field
from tqdm import tqdm

from src.parsers.unified import Decoder, Trade, Depth, Side, PriceLevel
from src.parsers.base import BaseUpdateMessage as Update


@dataclass
class Metrics:
    buy: list[tuple[Decimal, Decimal]] = field(default_factory=list)
    sell: list[tuple[Decimal, Decimal]] = field(default_factory=list)
    undefined: list[Decimal] = field(default_factory=list)

    def get_side(self, side: Side):
        if side == Side.BUY:
            return self.buy
        elif side == Side.SELL:
            return self.sell
        else:
            return self.undefined


class BaseEstimator:
    def __init__(self, tick_size: Decimal):
        self.ob: OrderBook = OrderBook()
        self.arrivals: Metrics = Metrics()
        self.cancels: Metrics = Metrics()

        self.market_arrivals: Metrics = Metrics()

        self.decoder: Decoder = Decoder()
        self.ask_price_positions: dict[Decimal, int] = {}
        self.bid_price_positions: dict[Decimal, int] = {}
        self.tick_size = tick_size

    def estimate(self, data: list[Update]):
        for update in tqdm(data):
            event = self.decoder.decode(update)

            if isinstance(event, Trade):
                self.process_trade(event)

            elif isinstance(event, Depth):
                self.process_depth(event)

    def process_depth(self, depth: Depth):

        # reconstructing order book
        if len(depth.construct) > 0:
            self.reset_ob(depth)
            return

        self.process_depth_insert(depth)
        self.process_depth_delete(depth)
        self.process_depth_update(depth)

    def process_trade(self, trade: Trade):

        if trade.side == Side.UNDEFINED:
            self.market_arrivals.undefined.append(trade.quantity)
            return

        opposite_distance = self.get_distance(
            price=trade.price,
            best=self.opposite_side_price(trade.side)
        )

        self.market_arrivals.get_side(trade.side) \
            .append((opposite_distance, trade.quantity))

    def process_depth_insert(self, depth: Depth):
        """ any insertion is an arrival of limit orders"""
        for price_level in depth.insert:
            ob = self.ob.asks if price_level.side == Side else self.ob.bids

            opposite_distance = self.get_distance(
                price=price_level.price,
                best=self.opposite_side_price(price_level.side)
            )
            self.arrivals.get_side(price_level.side) \
                .append((opposite_distance, price_level.quantity))

            ob[price_level.price] = price_level.quantity

    def process_depth_delete(self, depth: Depth):
        for price_level in depth.delete:
            own_distance = self.get_distance(
                price=price_level.price,
                best=self.same_side_price(price_level.side)
            )

            # assuming it is market order if
            # it was emptied close to same
            # side market price
            if own_distance <= 5:
                continue

            opposite_distance = self.get_distance(
                price=price_level.price,
                best=self.opposite_side_price(price_level.side)
            )

            ob = self.ob.asks if price_level.side == Side else self.ob.bids

            try:
                # ! may not be presented in the order book
                quantity = ob[price_level.price]
                del ob[price_level.price]

            except KeyError:
                quantity = 0

            self.cancels.get_side(price_level.side) \
                .append((opposite_distance, quantity))

    def process_depth_update(self, depth: Depth):

        for price_level in depth.update:
            ob = self.ob.asks if price_level.side == Side else self.ob.bids

            opposite_distance = self.get_distance(
                price=price_level.price,
                best=self.opposite_side_price(price_level.side)
            )

            try:
                _, existing_quantity = ob[price_level.price]
                delta = price_level.quantity - existing_quantity

                if delta > 0:
                    self.arrivals.get_side(price_level.side) \
                        .append((opposite_distance, delta))

                elif delta < 0:
                    self.cancels.get_side(price_level.side) \
                        .append((opposite_distance, abs(delta)))

            # if update due to the
            except KeyError:

                self.arrivals.get_side(price_level.side) \
                    .append((opposite_distance, price_level.quantity))

            ob[price_level.price] = price_level.quantity

    def opposite_side_price(self, side: Side) -> Decimal:
        if side == Side.BUY:
            return self._best_sell()

        elif side == Side.SELL:
            return self._best_buy()
        else:
            raise Exception("Unexpected side of the price level")

    def same_side_price(self, side: Side) -> Decimal:
        if side == Side.BUY:
            return self._best_buy()

        elif side == Side.SELL:
            return self._best_sell()
        else:
            raise Exception("Unexpected side of the price level")

    def _best_sell(self) -> Decimal:
        return self.ob.asks.index(0)[0]

    def _best_buy(self) -> Decimal:
        return self.ob.bids.index(0)[0]

    def get_distance(self, price: Decimal, best: Decimal) -> Decimal:
        return abs(best - price) / self.tick_size

    def reset_ob(self, depth: Depth):
        asks, bids = [], []

        for price_level in depth.construct:

            if price_level.side == Side.SELL:
                asks.append((price_level.price, price_level.quantity))

            elif price_level.side == Side.BUY:
                bids.append((price_level.price, price_level.quantity))

        self.ob = OrderBook()
        self.ob.asks = dict(asks)
        self.ob.bids = dict(bids)
