from typing import List

from hyperon_das.cache.attention_broker_gateway import AttentionBrokerGateway
from hyperon_das.context import Context
from hyperon_das.utils import QueryAnswer


class CacheController:
    def __init__(self, system_parameters):
        self.system_parameters = system_parameters
        if self.enabled():
            self.attention_broker = AttentionBrokerGateway(system_parameters)

    def enabled(self):
        return self.system_parameters.get("cache_enabled")

    def regard_query_answer(self, query_answer: List[QueryAnswer]):
        if not self.enabled():
            return
        joint_count = {}
        for query_answer_object in query_answer:
            handle_set = query_answer_object.get_handle_set()
            self.attention_broker.correlate(handle_set)
            count = query_answer_object.get_handle_count()
            for key in count.keys():
                total = joint_count.get(key, 0) + count.get(key)
                joint_count[key] = total
        self.attention_broker.stimulate(joint_count)

    def add_context(self, context: Context):
        if not self.enabled():
            return
        for query_answer in context.query_answers:
            self.regard_query_answer(query_answer)
