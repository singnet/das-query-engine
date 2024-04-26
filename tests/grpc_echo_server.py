from concurrent import futures

import echo_pb2
import echo_pb2_grpc
import grpc


class Echo(echo_pb2_grpc.EchoServicer):
    def echo(self, request, context):
        return echo_pb2.EchoResponse(msg=request.msg)


def serve():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    echo_pb2_grpc.add_EchoServicer_to_server(Echo(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
