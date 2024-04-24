import pytest
import time
import grpc
from concurrent import futures
import hyperon_das.grpc.common_pb2 as common
import hyperon_das.grpc.attention_broker_pb2 as attention_broker
import hyperon_das.grpc.attention_broker_pb2_grpc as ab_grpc
from hyperon_das.cache.cache_controller import ATTENTION_BROKER_PORT, CacheController

ATTENTION_BROKER_STUB_URL = f'localhost:{ATTENTION_BROKER_PORT}'

class AttentionBrokerMock(ab_grpc.AttentionBrokerServicer):
    def ping(self, request, context):
        return common.Ack(error=0, msg='OK') 
    def shutdown(self, request, context):
        assert False

def up_attention_broker_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ab_grpc.add_AttentionBrokerServicer_to_server(AttentionBrokerMock(), server)
    server.add_insecure_port("[::]:" + str(ATTENTION_BROKER_PORT))
    server.start()
    return server

def down_attention_broker_server(server):
    server.stop(1)
    time.sleep(1)

class TestCacheController:

    def test_creation(self):
        grpc_server = up_attention_broker_server()
        CacheController(cache_disabled=False)
        down_attention_broker_server(grpc_server)

    def test_add_context(self):
        assert True
