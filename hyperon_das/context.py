from typing import Any, List

from hyperon_das.utils import QueryAnswer


class Context:
    CONTEXT_NODE_TYPE = "Context"

    def __init__(self, context_node: dict[str, Any], query_answers: List[List[QueryAnswer]]):
        self.name = context_node['name']
        self.handle = context_node['_id']
        self.query_answers = query_answers
