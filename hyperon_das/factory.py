from abc import abstractmethod
from typing import Any, Protocol

from hyperon_das.constants import DasType
from hyperon_das.das import DistributedAtomSpaceClient, DistributedAtomSpaceServer


class IDasFactory(Protocol):
    @abstractmethod
    def create_client_das(self):
        ...  # pragma no cover

    @abstractmethod
    def create_server_das(self):
        ...  # pragma no cover


class DasFactory:
    def __init__(self, das_type: DasType) -> None:
        self.type = das_type

    def create_client_das(self):
        return DistributedAtomSpaceClient

    def create_server_das(self):
        return DistributedAtomSpaceServer


def das_factory(factory: IDasFactory, kwargs: Any):
    client_das = factory.create_client_das()
    server_das = factory.create_server_das()

    if factory.type == DasType.CLIENT.value:
        return client_das(kwargs)

    if factory.type == DasType.SERVER.value:
        return server_das(kwargs)
