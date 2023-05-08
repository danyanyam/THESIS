import datetime as dt
from typing import List
from decimal import Decimal

from pydantic import BaseModel, validator, Field, root_validator
from src.parsers.base import BaseUpdateMessage


class BinanceTradeData(BaseModel):
    event_type: str = Field(alias="e")
    ts: dt.datetime = Field(alias="E")
    symbol: str = Field(alias="s")
    agg_trade_id: int = Field(alias="a")
    price: Decimal = Field(alias="p")
    quantity: Decimal = Field(alias="q")
    first_trade_id: int = Field(alias="f")
    last_trade_id: int = Field(alias="l")
    trade_time: dt.datetime = Field(alias="T")
    is_market_maker: bool = Field(alias="m")
    is_ignore: bool = Field(alias="M")
    side: str = None


class BinanceTrade(BaseModel):
    stream: str
    data: BinanceTradeData

    @validator("stream")
    def validate_stream(cls, v):
        if "aggTrade" in v:
            return v
        else:
            raise ValueError(f"Invalid stream: {v}")


class BinanceDepthData(BaseModel):
    event_type: str = Field(alias="e")
    event_time: dt.datetime = Field(alias="E")
    symbol: str = Field(alias="s")
    first_update_id: int = Field(alias="U")
    last_update_id: int = Field(alias="u")
    bids: List[List[Decimal]] = Field(alias="b")
    asks: List[List[Decimal]] = Field(alias="a")

    @root_validator
    def check_non_empty(cls, v):
        assert any((len(v['bids']), len(v['asks'])))
        return v


class BinanceDepth(BaseModel):
    stream: str
    data: BinanceDepthData


class BinanceUpdateMessage(BaseUpdateMessage):
    raw: str
    ts: dt.datetime = None
    payload: BinanceTrade | BinanceDepth = None
