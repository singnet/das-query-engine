from typing import Optional
from hyperon_das.utils import das_error


class Context:

    CONTEXT_NODE_TYPE = "Context"

    def __init__(self, name: str, handle: str):
        if not name:
            das_error(ValueError("Invalid empty context name"))
        if not handle:
            das_error(ValueError("Invalid empty context handle"))
        self.name = name
        self.handle = handle
