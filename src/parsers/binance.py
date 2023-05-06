import json
import datetime as dt
from typing import List

from src.parsers.base import BaseUpdateMessage

from pydantic import BaseModel, validator, Field, condecimal, root_validator


class TradeData(BaseModel):
    event_type: str = Field(alias="e")
    event_time: dt.datetime = Field(alias="E")
    symbol: str = Field(alias="s")
    agg_trade_id: int = Field(alias="a")
    price: float = Field(alias="p")
    quantity: float = Field(alias="q")
    first_trade_id: int = Field(alias="f")
    last_trade_id: int = Field(alias="l")
    trade_time: dt.datetime = Field(alias="T")
    is_market_maker: bool = Field(alias="m")
    is_ignore: bool = Field(alias="M")


class Trade(BaseModel):
    stream: str
    data: TradeData

    @validator("stream")
    def validate_stream(cls, v):
        if "aggTrade" in v:
            return v
        else:
            raise ValueError(f"Invalid stream: {v}")


class DepthData(BaseModel):
    event_type: str = Field(alias="e")
    event_time: dt.datetime = Field(alias="E")
    symbol: str = Field(alias="s")
    first_update_id: int = Field(alias="U")
    last_update_id: int = Field(alias="u")
    bids: List[List[condecimal(decimal_places=8)]] = Field(alias="b")
    asks: List[List[condecimal(decimal_places=8)]] = Field(alias="b")


class Depth(BaseModel):
    stream: str
    data: DepthData


class BinanceUpdateMessage(BaseUpdateMessage):
    raw: str
    ts: dt.datetime = None
    payload: Trade | Depth = None
