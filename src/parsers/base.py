import json
import datetime as dt
from pydantic import BaseModel, root_validator, validator


class Trade(BaseModel):
    ...


class Depth(BaseModel):
    ...


class BaseUpdateMessage(BaseModel):
    raw: str
    ts: dt.datetime = None
    payload: Trade | Depth = None

    class Config:
        fields = {'raw': {'exclude': True}}

    @root_validator(pre=True)
    def fill(cls, v):
        v['ts'], v['payload'] = v['raw'].split(": ")
        return v

    @validator("ts", pre=True)
    def convert_string_to_float(cls, v):
        if isinstance(v, str):
            return float(v)
        return v

    @validator("payload", pre=True)
    def convert_string_to_json(cls, v):
        if isinstance(v, str):
            return json.loads(v, parse_float=str)
        return v
