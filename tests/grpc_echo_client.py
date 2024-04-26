import echo_pb2
import echo_pb2_grpc
import grpc


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = echo_pb2_grpc.EchoStub(channel)
        response = stub.echo(echo_pb2.EchoRequest(msg="blah"))
    print("Greeter client received: " + response.msg)


if __name__ == "__main__":
    run()
