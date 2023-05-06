import datetime as dt
from decimal import Decimal
from typing import List, Literal
from pydantic import BaseModel, validator, Field

from src.parsers.base import BaseUpdateMessage

TICK_DIRECTION_VALUES = Literal["PlusTick", "ZeroPlusTick", "MinusTick", "ZeroMinusTick"]


class TradeData(BaseModel):
    symbol: str = Field(alias="symbol")
    tick_direction: TICK_DIRECTION_VALUES = Field(alias="tick_direction")
    price: Decimal = Field(alias="price")
    quantity: Decimal = Field(alias="size")

    ts: dt.datetime = Field(alias="trade_time_ms")
    side: Literal["Buy", "Sell"] = Field(alias="side")
    trade_id: str = Field(alias="trade_id")
    is_block_trade: bool = Field(alias="is_block_trade")

    @validator("ts")
    def convert_ts_to_datetime(cls, v):
        if isinstance(v, str):
            return dt.datetime.fromisoformat(v)
        return v

    @validator("is_block_trade")
    def convert_str_to_bool(cls, v):
        if isinstance(v, str):
            return v == 'true'
        return v


class Trade(BaseModel):
    """
    {"topic":"trade.LINKUSDT",
    "data":[{"symbol":"LINKUSDT","tick_direction":"ZeroMinusTick",
    "price":"7.211","size":0.2,"timestamp":"2023-04-06T13:34:21.000Z",
    "trade_time_ms":"1680788061967","side":"Buy",
    "trade_id":"8b196fcc-6c95-58d6-ac60-fbecfa41e051",
    "is_block_trade":"false"}]}
    """
    stream: str = Field(alias="topic")
    data: list[TradeData]

    @validator("stream")
    def validate_stream(cls, v):
        if "trade" in v:
            return v
        else:
            raise ValueError(f"Invalid TRADE stream: {v}")


class TechnicalData(BaseModel):
    """
    {"success":true,"ret_msg":"",
    "conn_id":"19746d61-7b54-4c17-8a56-81c0fe3dadbe",
    "request":{"op":"subscribe","args":["trade.EOSUSDT"]}}
    """
    success: bool
    ret_msg: str
    conn_id: str
    request: dict[str, str | list[str]]

    @validator("success", pre=True)
    def convert_str_to_bool(cls, v):
        if isinstance(v, str):
            return v.lower() == "true"
        return v


class OBLevel(BaseModel):
    price: Decimal
    size: Decimal = -1  # in delete messages, this is not used
    id: int
    side: Literal["Buy", "Sell"]
    symbol: str


class DeltaData(BaseModel):
    delete: List[OBLevel] = []
    update: List[OBLevel] = []
    insert: List[OBLevel] = []
    bid1_id: int
    ask1_id: int


class SnapshotData(BaseModel):
    order_book: List[OBLevel]


class Depth(BaseModel):
    """
    {"topic":"orderBook_200.100ms.LINKUSDT","type":"delta",
    "data":{
    "delete":[],
    "update":[{"price":"7.206","symbol":"LINKUSDT", "id":"720600000","side":"Buy","size":4897.1},
              {"price":"7.210","symbol":"LINKUSDT","id":"721000000","side":"Buy", "size":2460.2}],
    "insert":[],
    "bid1_id":"721100000",
    "ask1_id":"721200000"},
    "cross_seq":"37918425639","timestamp_e6":"1680788174112322"}
    """
    stream: str = Field(alias="topic")
    type: Literal["delta", "snapshot"] = Field(alias="type")
    data: DeltaData | SnapshotData = Field(alias="data")
    cross_seq: int = Field(alias="cross_seq")
    ts: dt.datetime = Field(alias="timestamp_e6")

    @validator("stream")
    def validate_stream(cls, v):
        if "orderBook" in v:
            return v
        else:
            raise ValueError(f"Invalid DEPTH stream: {v}")


class BybitUpdateMessage(BaseUpdateMessage):
    raw: str
    ts: dt.datetime = None
    payload: Trade | Depth | TechnicalData = None
