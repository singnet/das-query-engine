from concurrent import futures

import grpc

import hyperon_das.grpc.attention_broker_pb2_grpc as ab_grpc
import hyperon_das.grpc.common_pb2 as common


class AttentionBroker(ab_grpc.AttentionBrokerServicer):
    def ping(self, request, context):
        return common.Ack(error=0, msg='OK')

    def shutdown(self, request, context):
        return common.Empty()


def serve():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ab_grpc.add_AttentionBrokerServicer_to_server(AttentionBroker(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
