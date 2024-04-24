import grpc
import attention_broker_pb2 as attention_broker
import common_pb2 as common
import attention_broker_pb2_grpc as ab_grpc


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = ab_grpc.AttentionBrokerStub(channel)
        response = stub.ping(common.Empty())
    print(response.msg)

if __name__ == "__main__":
    run()
