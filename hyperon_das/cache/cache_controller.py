import grpc
import attention_broker_pb2 as attention_broker
import attention_broker_pb2_grpc as ab_grpc
import common_pb2 as common

from hyperon_das.context import Context
from hyperon_das.logger import logger

ATTENTION_BROKER_HOSTNAME = 'localhost'
ATTENTION_BROKER_PORT = 27000

class CacheController:

    def __init__(self, **kwargs):
        self.disabled = kwargs.get('cache_disabled', True)
        if self.disabled:
            return
        self.attention_broker_url = f'{ATTENTION_BROKER_HOSTNAME}:{ATTENTION_BROKER_PORT}'
        logger().info(f'Pinging AttentionBroker at {self.attention_broker_url}')
        answer = self._ping_attention_broker()
        logger().info(answer.msg)

    def _ping_attention_broker(self) -> str:
        with grpc.insecure_channel(self.attention_broker_url) as channel:
            stub = ab_grpc.AttentionBrokerStub(channel)
            return stub.ping(common.Empty())

    def add_context(context: Context):
        if self.disabled:
            return
        pass
        

