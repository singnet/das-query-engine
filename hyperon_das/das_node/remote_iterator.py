from hyperon_das.das_node.query_element import QueryElement
from hyperon_das_node import MessageBrokerType
from hyperon_das.das_node.query_answer import QueryAnswer
from hyperon_das.das_node.query_node import QueryNodeServer

class RemoteIterator(QueryElement):
    def __init__(self, local_id: str):
        self.local_id = local_id
        self.remote_input_buffer = None
        self.setup_buffers()

    def __del__(self):
        self.graceful_shutdown()

    def graceful_shutdown(self):
        if self.remote_input_buffer:
            self.remote_input_buffer.graceful_shutdown()

    def setup_buffers(self):
        # Assuming QueryNodeServer and MessageBrokerType are defined elsewhere
        print("local_server", self.local_id)
        self.remote_input_buffer = QueryNodeServer(self.local_id, MessageBrokerType.GRPC)

    def finished(self) -> bool:
        return (self.remote_input_buffer.is_query_answers_finished() and
                self.remote_input_buffer.is_query_answers_empty())

    def pop(self) -> QueryAnswer:
        # Assuming QueryNode has a method `pop_query_answer` that returns a QueryAnswer
        return self.remote_input_buffer.pop_query_answer()
