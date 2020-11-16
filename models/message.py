from sqlalchemy import Column, Integer, String, Float, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from random import uniform
from dateutil.parser import parse

Base = declarative_base()

class Message(Base):
    __tablename__ = 'messages'
    _count = 0
    _id = Column("id", Integer, primary_key=True)
    key = Column(String(32), nullable=False)
    value = Column(Float, nullable=False)
    ts = Column(DateTime(), nullable=False, server_default=func.now())

    def __init__(self, _id=None, key=None, value=None, ts=None):
        Message._count += 1
        self._id = _id or Message._count
        self.key = key
        self.value = value
        self.ts = ts

    def to_json(self):
        return dict(
            key=str(self.key),
            value=str(self.value),
            ts=str(self.ts)
        )

    def __repr__(self):
        return '<Message(key=%s, value=%s, ts=%s)>' % (
            self.key, self.value, self.ts
        )

    def __str__(self):
        return self.__repr__()

    @classmethod
    def from_dict(cls, data: dict):
        if cls.validate_dict(data):
            return cls(
                key = data["key"],
                value = data["value"],
                ts = cls.normalize_ts(data["ts"])
            )
        else:
            return None

    @classmethod
    def normalize_ts(cls, value):
        if type(value) == str:
            return parse(value)
        elif type(value) == float:
            return datetime.fromtimestamp(value)
        else:
            return value
    
    @classmethod
    def random(cls, key_prefix="A", min_val=0, max_val=1000000):
        return cls(
            key=f"{key_prefix}{Message._count}",
            value=uniform(min_val,max_val),
            ts=datetime.now()
        )

    @classmethod
    def validate_dict(cls, data: dict):
        compare_set = {"key","value","ts"}
        if compare_set.intersection(set(data.keys())) != compare_set:
            return False

        return True
