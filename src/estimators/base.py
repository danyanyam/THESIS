import datetime as dt
from decimal import Decimal
from order_book import OrderBook
from dataclasses import dataclass, field
from tqdm import tqdm
import pandas as pd
import numpy as np

from src.parsers.unified import Decoder, GenericDepth, GenericTrade, Side
from src.parsers.base import BaseUpdateMessage as Update


@dataclass
class Metrics:
    buy: list[tuple[int, Decimal]] = field(default_factory=list)
    sell: list[tuple[int, Decimal]] = field(default_factory=list)
    undefined: list[Decimal] = field(default_factory=list)

    def get_side(self, side: Side):
        if side == Side.BUY:
            return self.buy
        elif side == Side.SELL:
            return self.sell
        else:
            return self.undefined

    def to_dataframe(self):
        buy = pd.DataFrame(self.buy, columns=['i', 'quantity'])
        sell = pd.DataFrame(self.sell, columns=['i', 'quantity'])
        undefined = pd.DataFrame(self.undefined, columns=['quantity'])
        return buy, sell, undefined


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

        self.tss: list[dt.datetime] = []

    def estimate(self, data: list[Update]):
        for update in tqdm(data):
            event = self.decoder.decode(update)

            if isinstance(event, GenericTrade):
                self.process_trade(event)

            elif isinstance(event, GenericDepth):
                self.process_depth(event)

            else:
                continue

            self.tss.append(event.ts)

    def get_parameters(self):
        Sm = float(np.mean(self.market_arrivals.undefined))

        arrivals_buy, arrivals_sell, _ = self.arrivals.to_dataframe()

        first = arrivals_buy.groupby('i').mean()
        second = arrivals_sell.groupby('i').mean()

        holder = pd.DataFrame(0, columns=['quantity'],
                              index=first.index.union(second.index))

        Sl = ((holder + first).fillna(0) + second).mean().iloc[0]

        cancels_buy, cancels_sell, _ = self.cancels.to_dataframe()

        first = cancels_buy.groupby('i').mean()
        second = cancels_sell.groupby('i').mean()

        holder = pd.DataFrame(0, columns=['quantity'],
                              index=first.index.union(second.index))

        Sc = abs(((holder + first).fillna(0) + second).mean().iloc[0]) - Sm

        Nlia = arrivals_sell.groupby('i').count()
        Nlib = arrivals_buy.groupby('i').count()

        T = (max(self.tss) - min(self.tss)).total_seconds() / 60

        lamdaia = Nlia / T
        lamdaib = Nlib / T

        mu = len(self.market_arrivals.undefined) / T * Sm / Sl

        Qi = np.mean([
            (arrivals_buy.groupby('i').mean() / Sl)['quantity'].values,
            (arrivals_sell.groupby('i').mean() / Sl)['quantity'].values,
        ], axis=0)

        thetai = (cancels_sell.groupby('i').count() +
                  cancels_sell.groupby('i').count()).sort_index().values / (T * Qi) * Sc.iloc[0] / Sl

        return {
            'mu': mu,
            'theta': thetai,
            'lamdaia': lamdaia,
            'lamdaib': lamdaib,
            'Qi': Qi
        }

    def process_depth(self, depth: GenericDepth):

        # reconstructing order book
        if len(depth.construct) > 0:
            self.reset_ob(depth)
            return

        self.process_depth_insert(depth)
        self.process_depth_delete(depth)
        self.process_depth_update(depth)

    def process_trade(self, trade: GenericTrade):

        if trade.side == Side.UNDEFINED:
            self.market_arrivals.undefined.append(trade.quantity)
            return

        opposite_distance = self.get_distance(
            price=trade.price,
            best=self.opposite_side_price(trade.side)
        )

        self.market_arrivals.get_side(trade.side) \
            .append((opposite_distance, trade.quantity))

    def process_depth_insert(self, depth: GenericDepth):
        """ any insertion is an arrival of limit orders"""
        for price_level in depth.insert:
            ob = self.ob.asks if price_level.side == Side.SELL else self.ob.bids

            opposite_distance = self.get_distance(
                price=price_level.price,
                best=self.opposite_side_price(price_level.side)
            )
            self.arrivals.get_side(price_level.side) \
                .append((opposite_distance, price_level.quantity))

            ob[price_level.price] = price_level.quantity

    def process_depth_delete(self, depth: GenericDepth):
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

            ob = self.ob.asks if price_level.side == Side.SELL else self.ob.bids

            try:
                # ! may not be presented in the order book
                quantity = ob[price_level.price]
                del ob[price_level.price]

            except KeyError:
                quantity = 0

            self.cancels.get_side(price_level.side) \
                .append((opposite_distance, quantity))

    def process_depth_update(self, depth: GenericDepth):

        for price_level in depth.update:
            ob = self.ob.asks if price_level.side == Side.SELL else self.ob.bids

            try:
                opposite_distance = self.get_distance(
                    price=price_level.price,
                    best=self.opposite_side_price(price_level.side)
                )

                existing_quantity = ob[price_level.price]
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

            except IndexError:
                pass

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

    def get_distance(self, price: Decimal, best: Decimal) -> int:
        return int(abs(best - price) / self.tick_size)

    def reset_ob(self, depth: GenericDepth):
        asks, bids = [], []

        for price_level in depth.construct:

            if price_level.side == Side.SELL:
                asks.append((price_level.price, price_level.quantity))

            elif price_level.side == Side.BUY:
                bids.append((price_level.price, price_level.quantity))

        self.ob = OrderBook()
        self.ob.asks = dict(asks)
        self.ob.bids = dict(bids)
