import json
import numpy as np
from typing import Optional
from scipy.stats import binom
from tqdm.notebook import tqdm
from order_book import OrderBook
from abc import abstractmethod, ABC


class MyOrderBook(OrderBook):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def spread(self) -> float:
        assert len(self.bids) > 0 and len(self.asks) > 0
        return self.asks.index(0)[0] - self.bids.index(0)[0]

    def __repr__(self) -> str:
        ob = {k: self.to_dict()[k] for k in sorted(self.to_dict())}
        return json.dumps(ob, indent=4)

    def vbbid(self) -> float:
        return self.bids.index(0)[-1]

    def vbask(self) -> float:
        return self.asks.index(0)[-1]

    def increment_vbask(self) -> None:
        price, vol = self.asks.index(0)
        self.asks[price] = vol + 1

    def decrement_vbask(self) -> None:
        price, vol = self.asks.index(0)

        if vol == 1:
            del self.asks[price]
        else:
            self.asks[price] = vol - 1

    def decrement_vbbid(self) -> None:
        price, vol = self.bids.index(0)

        if vol == 1:
            del self.bids[price]
        else:
            self.bids[price] = vol - 1

    def increment_vbbid(self) -> None:
        price, vol = self.bids.index(0)
        self.bids[price] = vol + 1


class MonteCarloExperiment(ABC):
    def __init__(self, trials=10_000, verbose=False, **params) -> None:
        self.verbose = verbose
        self.trials = trials
        self.params = params

    @abstractmethod
    def reset_state(self) -> None:
        ...

    @abstractmethod
    def update_state(self) -> None:
        ...

    @abstractmethod
    def success(self) -> Optional[bool]:
        """
        Is used to detemine desirable outcome, once the conditional events are
        reached. For example: in midprice increase probability, success() must
        be called, once midprice is changing and its change is positive. In all
        other cases, it must return nothing

        Returns:
            bool (optional): whether conditional is reached and outcome is
        a desirable function returns True. Otherwise - None.
        """
        ...

    @abstractmethod
    def failure(self) -> Optional[bool]:
        """
        is used to determine events, which are recorgnised as failures
        of the monte-carlo experiment. They are important, because those events
        determine conditionals. For example: when we want to calculate the
        probability of the mid price increase, we do it conditional on the
        midprice change. In other words, we calculate probability of the next
        midprice change being positive. In that scenario, failure() function
        must return true, if midprice changed in the opposite direction.
        Otherwise, experiment must continue, until next mid price change occurs

        Returns:
            bool (optional): whether conditional is reached and the outcome is
        a undesirable. In all other cases function returns None.
        """
        ...

    def evaluate(self) -> float:
        count = 0
        trials = range(self.trials)
        trials = tqdm(trials) if self.verbose else trials

        for trial in trials:
            self.reset_state()

            while True:
                self.update_state()

                if self.success():
                    count += 1
                    break

                elif self.failure():
                    break

        return count / self.trials


class BidOrderExecution(MonteCarloExperiment):
    def __init__(self, bbid_orders=5, bask_orders=5,
                 bid_pos=5, trials=10000, verbose=False, **params) -> None:
        super().__init__(trials, verbose, **params)

        self.bbid_orders = bbid_orders
        self.bask_orders = bask_orders
        self.bid_pos = bid_pos
        self.reset_state()

    def reset_state(self) -> None:
        self.xb = self.bbid_orders
        self.xa = self.bask_orders
        self.bid_idx = self.bid_pos

    def success(self) -> Optional[bool]:
        if self.bid_idx == 0:
            return True

    def failure(self) -> Optional[bool]:
        if self.xa == 0:
            return True

    def update_state(self) -> None:
        # since our orders are never canceled
        xb_rate = self.xb - 1
        xs_rate = self.xa

        if self.bid_idx == 0:
            xb_rate = self.xb

        lamda, mu = self.params['lamda'], self.params['mu']
        theta = self.params['theta']

        cum_rate = 2*mu + 2*lamda + theta*xb_rate + theta*xs_rate

        pevent = np.array([
            lamda,            # limit bid +1
            lamda,            # limit ask +1
            mu,               # bought market bid
            mu,               # bought market ask
            theta * xb_rate,  # cancel bid -1
            theta * xs_rate   # cancel ask -1
        ]) / cum_rate

        event = np.random.choice(np.arange(6), size=1, p=pevent)[0]

        if event == 0:
            self.xb += 1

        elif event == 1:
            self.xa += 1

        elif event == 2:
            self.xb -= 1
            # position moves in queue
            self.bid_idx -= 1 if self.bid_idx > 0 else 0

        elif event == 3:
            self.xa -= 1

        # cancellations
        elif event == 4:
            if np.random.uniform() > (self.xb - self.bid_idx) / xb_rate:
                # cancellation before our price
                self.bid_idx -= 1
            self.xb -= 1

        elif event == 5:
            self.xa -= 1


class MakingSpread(MonteCarloExperiment):
    def __init__(self, bbid_orders=5, bask_orders=5,
                 bid_pos=5, ask_pos=5, trials=10_000, verbose=False, **params):
        super().__init__(trials, verbose, **params)

        self.bbid_orders = bbid_orders
        self.bask_orders = bask_orders
        self.bid_pos = bid_pos
        self.ask_pos = ask_pos
        self.reset_state()

    def reset_state(self) -> None:
        self.xb = self.bbid_orders
        self.xa = self.bask_orders
        self.bid_idx = self.bid_pos
        self.ask_idx = self.ask_pos

    def update_state(self) -> None:
        # since our orders are never canceled

        lamda, mu = self.params['lamda'], self.params['mu']
        theta = self.params['theta']

        xb_rate = self.xb - 1
        xa_rate = self.xa - 1

        if self.bid_idx == 0:
            xb_rate = self.xb

        if self.ask_idx == 0:
            xa_rate = self.xa

        cum_rate = 2*mu + 2*lamda + theta*xb_rate + theta*xa_rate

        pevent = np.array([
            lamda,            # limit bid +1
            lamda,            # limit ask +1
            mu,               # bought market bid
            mu,               # bought market ask
            theta * xb_rate,  # cancel bid -1
            theta * xa_rate   # cancel ask -1
        ]) / cum_rate

        event = np.random.choice(np.arange(6), size=1, p=pevent)[0]

        if event == 0:
            self.xb += 1

        elif event == 1:
            self.xa += 1

        elif event == 2:
            self.xb -= 1
            # position moves in queue
            self.bid_idx -= 1 if self.bid_idx > 0 else 0

        elif event == 3:
            self.xa -= 1
            # position moves in queue
            self.ask_idx -= 1 if self.ask_idx > 0 else 0

        # cancellations
        elif event == 4:
            if np.random.uniform() > (self.xb - self.bid_idx) / xb_rate:
                # cancellation before our price
                self.bid_idx -= 1
            self.xb -= 1

        elif event == 5:
            if np.random.uniform() > (self.xa - self.ask_idx) / xa_rate:
                # cancellation before our price
                self.ask_idx -= 1
            self.xa -= 1

    def failure(self) -> Optional[bool]:

        # if midprice goes down
        if self.xb == 0 and self.ask_idx > 0:
            return True

        # if midprice goes up
        elif self.xa == 0 and self.bid_idx > 0:
            return True

    def success(self) -> Optional[bool]:
        # if both our orders have executed
        if self.bid_idx == 0 and self.ask_idx == 0:
            return True


class MidpriceUp(MonteCarloExperiment):
    def __init__(self, bbid_orders: int = 5, bask_orders: int = 5,
                 spread: int = 10, trials: int = 10000, verbose=False,
                 **params):
        super().__init__(trials, verbose, **params)

        self.bbid_orders = bbid_orders
        self.bask_orders = bask_orders
        self.spread = spread
        self.reset_state()

    def reset_state(self) -> None:
        self.xb = self.bbid_orders
        self.xa = self.bask_orders
        self.S = self.spread

        self.ob = MyOrderBook()
        self.ob.asks[self.spread] = self.bask_orders
        self.ob.bids[0] = self.bbid_orders

    def success(self) -> Optional[bool]:
        """
        Nescessary condition: midprice changes
        Desirable outcome:    midprice increases
        """
        if len(self.ob.bids) > 1 or len(self.ob.asks) == 0:
            return True

    def failure(self) -> Optional[bool]:
        """
        Nescessary condition: midprice changes
        Undesirable outcome:  midprice drops
        """

        # midprice decreases, if new asks get within spread
        # or when best bid level is exhausted
        if len(self.ob.asks) > 1 or len(self.ob.bids) == 0:
            return True

    def update_state(self) -> None:

        lamda, mu = self.params['lamda'], self.params['mu']
        theta, lambda_ = self.params['theta'], self.params['lambda_']

        birth_inspread_ask = np.array([lambda_(i) for i in range(1, self.S)])
        birth_inspread_bid = np.array([lambda_(i) for i in range(1, self.S)])

        birth_bid = lamda
        birth_ask = lamda
        death_bid = mu + theta*self.ob.vbbid()
        death_ask = mu + theta*self.ob.vbask()

        pevent = np.array([birth_bid, birth_ask, death_bid, death_ask])
        pevent = np.append(pevent, birth_inspread_ask)
        pevent = np.append(pevent, birth_inspread_bid)
        pevent /= pevent.sum()

        event = np.random.choice(np.arange(len(pevent)), size=1, p=pevent)[0]

        if event == 0:
            self.ob.increment_vbbid()

        elif event == 1:
            self.ob.increment_vbask()

        elif event == 2:
            self.ob.decrement_vbbid()

        elif event == 3:
            self.ob.decrement_vbask()

        elif event >= 4 and event < 4 + self.S:
            inspread_dist = event - 3
            # price of best ask
            new_bask = self.ob.asks.index(0)[0] - inspread_dist
            self.ob.asks[new_bask] = 1

        elif event >= 4 + self.S:

            inspread_dist = event - 3 - self.S
            # price of best bid
            new_bbid = self.ob.bids.index(0)[0] + inspread_dist
            self.ob.bids[new_bbid] = 1


class MakingSpreadS(MonteCarloExperiment):
    def __init__(self, bbid_orders: int = 5, bask_orders: int = 5,
                 bid_pos: int = 5, ask_pos: int = 5, spread: int = 1,
                 trials: int = 10_000, verbose=False, **params):
        super().__init__(trials, verbose, **params)

        self.bbid_orders = bbid_orders
        self.bask_orders = bask_orders
        self.bid_pos = bid_pos
        self.ask_pos = ask_pos
        self.spread = spread
        self.reset_state()

    def reset_state(self) -> None:
        self.xb = self.bbid_orders
        self.xa = self.bask_orders
        self.S = self.spread
        self.bid_idx = self.bid_pos
        self.ask_idx = self.ask_pos

        self.ob = MyOrderBook()
        self.ob.asks[self.spread] = self.bask_orders
        self.ob.bids[0] = self.bbid_orders

    def update_state(self) -> None:

        lamda, mu = self.params['lamda'], self.params['mu']
        theta, lambda_ = self.params['theta'], self.params['lambda_']

        birth_inspread_ask = np.array([lambda_(i) for i in range(1, self.S)])
        birth_inspread_bid = np.array([lambda_(i) for i in range(1, self.S)])

        # since our orders are never canceled
        xb_rate = self.ob.vbbid() - 1
        xa_rate = self.ob.vbask() - 1

        if self.bid_idx == 0:
            xb_rate = self.ob.vbbid()

        if self.ask_idx == 0:
            xa_rate = self.ob.vbask()

        birth_bid = lamda
        birth_ask = lamda
        market_buy = mu
        market_sell = mu
        death_bid = theta*xb_rate
        death_ask = theta*xa_rate

        pevent = np.array([
            birth_bid,
            birth_ask,
            market_buy,
            market_sell,
            death_bid,
            death_ask
        ])
        pevent = np.append(pevent, birth_inspread_ask)
        pevent = np.append(pevent, birth_inspread_bid)
        pevent /= pevent.sum()

        event = np.random.choice(np.arange(len(pevent)), size=1, p=pevent)[0]

        # new bid order at best bid price
        if event == 0:
            self.ob.increment_vbbid()

        # new ask order at best ask price
        elif event == 1:
            self.ob.increment_vbask()

        # market sell at the best bid price
        elif event == 2:
            self.ob.decrement_vbbid()
            # our position moves in the queue
            self.bid_idx -= 1 if self.bid_idx > 0 else 0

        # market buy at the best ask price
        elif event == 3:
            self.ob.decrement_vbask()
            # our position moves in the queue
            self.ask_idx -= 1 if self.ask_idx > 0 else 0

        # cancellation of bid
        elif event == 4:
            r = np.random.uniform()
            if r > (self.ob.vbbid() - self.bid_idx) / xb_rate:
                # cancellation before our price
                self.bid_idx -= 1

            self.ob.decrement_vbbid()

        # cancellation of ask
        elif event == 5:
            r = np.random.uniform()
            if r > (self.ob.vbask() - self.ask_idx) / xa_rate:
                # cancellation before our price
                self.ask_idx -= 1

            self.ob.decrement_vbask()

        # inspread ask arrivals
        elif event >= 6 and event < 6 + self.S:
            inspread_dist = event - 5
            # price of best ask
            new_bask = self.ob.asks.index(0)[0] - inspread_dist
            self.ob.asks[new_bask] = 1

        # inspread bid arrivals
        elif event >= 6 + self.S:
            inspread_dist = event - 5 - self.S
            # price of best bid
            new_bbid = self.ob.bids.index(0)[0] + inspread_dist
            self.ob.bids[new_bbid] = 1

    def failure(self) -> Optional[bool]:
        """
        Nescessary condition: midprice changes
        Undesirable outcome:  any of the orders are not fulfilled
        """

        # if new bid/ask appears within spread
        if len(self.ob.bids) > 1 or len(self.ob.asks) > 1:
            if self.ask_idx != 0 or self.bid_idx != 0:
                return True

        # if midprice goes up, but we didnt close positions
        if len(self.ob.asks) == 0 and self.bid_idx > 0:
            return True

        # if midprice goes down, but we didnt close positions
        elif len(self.ob.bids) == 0 and self.ask_idx > 0:
            return True

    def success(self) -> Optional[bool]:
        """
        Nescessary condition: midprice changes
        Desirable outcome:    both orders are fulfilled
        """
        # we earn if both ask and bid orders are fulfilled
        # but at the same time, midprice doesnt change

        if self.bid_idx == 0 and self.ask_idx == 0:
            return True


class StopLossReaching(MidpriceUp):
    """
    Probability of reaching stop loss corresponds to the probability of
    midprice moving certain amount of ticks away.
    """

    def __init__(self, n_times: int, k_ticks: int,
                 bbid_orders: int = 5, bask_orders: int = 5,
                 spread: int = 10, trials: int = 10000,
                 verbose=False, **params):
        super().__init__(bbid_orders, bask_orders, spread,
                         trials, verbose, **params)

        self.n_times = n_times
        self.k_ticks = k_ticks

    def evaluate(self) -> float:
        p = 1 - super().evaluate()
        k = self.k_ticks
        summed = 0
        for j in range(1, self.trials):
            n = k + 2 * j
            summed += binom.pmf(k=(n - k) / 2, n=n, p=p)

        return summed
