from pydantic import BaseModel


class OutputParams(BaseModel):
    destination_path: str
