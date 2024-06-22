from pydantic import BaseModel


class ItemParams(BaseModel):
    ae_comp: str
    ae_output: str
    ae_project: str
    ae_project_timestamp: str
    creation_timestamp: str
    name: str

    def __contains__(self, name):
        if hasattr(self, name):
            return True
        return False

    def __getitem__(self, name):
        return getattr(self, name)
