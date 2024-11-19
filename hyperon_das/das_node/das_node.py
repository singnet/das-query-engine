from typing import List, Optional, Dict
import uuid
from hyperon_das.das_node.star_node import StarNode
from hyperon_das.das_node.remote_iterator import RemoteIterator
from hyperon_das_node import Message, MessageFactory
from hyperon_das.das_node.query_element import QueryElement



class DASNode(StarNode):
    PATTERN_MATCHING_QUERY = "pattern_matching_query"

    def __init__(self, node_id: str = None, server_id: str = None):
        super().__init__(node_id, server_id)
        self.initialize()



    def pattern_matcher_query(self, tokens: list, context: str = ""):
        if self.is_server:
            raise ValueError("pattern_matcher_query() is not available in DASNode server.")

        # TODO: Update this when requestor is set in basic Message
        query_id = self.next_query_id()
        # print(query_id, tokens)
        args = [query_id] + tokens
        self.send(DASNode.PATTERN_MATCHING_QUERY, args, self.server_id)
        return RemoteIterator(query_id)

    def next_query_id(self) -> str:
        port = self.next_query_port
        limit = 0
        if self.is_server:
            limit = (self.first_query_port + self.last_query_port) // 2 - 1
            if self.next_query_port > limit:
                self.next_query_port = self.first_query_port
        else:
            limit = self.last_query_port
            if self.next_query_port > limit:
                self.next_query_port = (self.first_query_port + self.last_query_port) // 2

        query_id = f"{self.local_host}:{port}"
        self.next_query_port += 1
        return query_id

    def message_factory(self, command: str, args: list) -> Message:
        # print("DASNode", "message_factory")
        message = super().message_factory(command, args)
        if message:
            return message
        # if command == DASNode.PATTERN_MATCHING_QUERY:
        #     return PatternMatchingQuery(command, args)
        return None

    def initialize(self):
        self.first_query_port = 60000
        self.last_query_port = 61999
        self.local_host = self.node_id().split(":")[0]  # Extracting the host part of node_id
        if self.is_server:
            self.next_query_port = self.first_query_port
        else:
            self.next_query_port = (self.first_query_port + self.last_query_port) // 2

    # Private attributes (Python doesn't have access specifiers, but this is by convention)
    local_host: str
    next_query_port: int
    first_query_port: int
    last_query_port: int
