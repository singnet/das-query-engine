from unittest import mock

import pytest

from hyperon_das.cache.attention_broker_gateway import AttentionBrokerGateway


class TestAttentionBrokerGateway:
    def test_creation(self):
        with pytest.raises(ValueError):
            AttentionBrokerGateway({})
        with pytest.raises(ValueError):
            AttentionBrokerGateway({'attention_broker_hostname': 'localhost'})
        with pytest.raises(ValueError):
            AttentionBrokerGateway({'attention_broker_port': 27000})
        # successful creation is tested in a integration test because it requires
        # actual connection to a GRPC server

    @mock.patch("hyperon_das.grpc.attention_broker_pb2_grpc.AttentionBrokerStub", mock.MagicMock())
    @mock.patch("hyperon_das.cache.attention_broker_gateway.grpc", mock.MagicMock())
    def test_creation_successful(self):
        ab = AttentionBrokerGateway(
            {"attention_broker_hostname": "localhost", "attention_broker_port": 27000}
        )
        assert isinstance(ab, AttentionBrokerGateway)
        assert ab is not None

    @mock.patch("hyperon_das.cache.attention_broker_gateway.grpc_types")
    @mock.patch("hyperon_das.cache.attention_broker_gateway.grpc", mock.MagicMock())
    @mock.patch(
        "hyperon_das.cache.attention_broker_gateway.AttentionBrokerStub",
        return_value=mock.MagicMock(),
    )
    def test_stimulate(self, attention_broker_stub: mock.patch, grpc_types: mock.patch):
        handle_count = {"handle1", "handle2"}
        ab = AttentionBrokerGateway(
            {"attention_broker_hostname": "localhost", "attention_broker_port": 27000}
        )
        grpc_types.HandleCount.return_value = handle_count
        ab.stimulate(handle_count)
        grpc_types.HandleCount.assert_called_once_with(handle_count=handle_count)
        attention_broker_stub.return_value.stimulate.assert_called_once_with(handle_count)

    @mock.patch("hyperon_das.grpc.attention_broker_pb2_grpc.AttentionBrokerStub", mock.MagicMock())
    @mock.patch("hyperon_das.cache.attention_broker_gateway.grpc", mock.MagicMock())
    def test_stimulate_error(self):
        ab = AttentionBrokerGateway(
            {"attention_broker_hostname": "localhost", "attention_broker_port": 27000}
        )
        with pytest.raises(ValueError):
            ab.stimulate(None)

    @mock.patch("hyperon_das.cache.attention_broker_gateway.grpc_types")
    @mock.patch("hyperon_das.cache.attention_broker_gateway.grpc", mock.MagicMock())
    @mock.patch(
        "hyperon_das.cache.attention_broker_gateway.AttentionBrokerStub",
        return_value=mock.MagicMock(),
    )
    def test_correlate(self, attention_broker_stub: mock.patch, grpc_types: mock.patch):
        handle_count = {"handle1", "handle2"}
        ab = AttentionBrokerGateway(
            {"attention_broker_hostname": "localhost", "attention_broker_port": 27000}
        )
        grpc_types.HandleList.return_value = handle_count
        ab.correlate(handle_count)
        grpc_types.HandleList.assert_called_once_with(handle_list=handle_count)
        attention_broker_stub.return_value.correlate.assert_called_once_with(handle_count)

    @mock.patch("hyperon_das.grpc.attention_broker_pb2_grpc.AttentionBrokerStub", mock.MagicMock())
    @mock.patch("hyperon_das.cache.attention_broker_gateway.grpc", mock.MagicMock())
    def test_correlate_error(self):
        ab = AttentionBrokerGateway(
            {"attention_broker_hostname": "localhost", "attention_broker_port": 27000}
        )
        with pytest.raises(ValueError):
            ab.correlate(None)
