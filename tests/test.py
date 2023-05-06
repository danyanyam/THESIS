from tqdm import tqdm
from src.parsers.bybit import BybitUpdateMessage
from src.parsers.binance import BinanceUpdateMessage


class Parser:
    def __init__(self, source):
        self.parser = BinanceUpdateMessage if source == 'binance' else \
            BybitUpdateMessage
        self.results = []

    def parse(self, message):
        if message.startswith('wss'):
            return None

        return self.parser(raw=message)

    def read(self, path):
        with open(path, 'r') as f:
            data = f.readlines()

        for path in tqdm(data):
            res = self.parse(path)

            if res is not None:
                continue

            self.results.append(res)

        return self.results


p = Parser('binance')
p.read('../data/BINANCE.ws.3.0')

assert len(p) > 0

print(p)
