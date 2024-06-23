from typing import List

import json5
from pydantic import BaseModel


class ItemParams(BaseModel):
    ae_comp: str
    ae_project: str
    ae_version: str
    division: str
    duration: float
    event: str
    flags: str
    frame_rate: float
    height: int
    item_name: str
    location: str
    marker_times: List[float]
    source_appearance: float
    source_duration: float
    source_name: str
    speed_divisor: int
    variant: str
    width: int

    def __contains__(self, name):
        if hasattr(self, name):
            return True
        return False

    def __getitem__(self, name):
        return getattr(self, name)

    @classmethod
    def from_json5(cls, file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json5.load(file)
            return cls(**data)
