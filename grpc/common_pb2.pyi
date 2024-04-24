from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Ack(_message.Message):
    __slots__ = ("error", "msg")
    ERROR_FIELD_NUMBER: _ClassVar[int]
    MSG_FIELD_NUMBER: _ClassVar[int]
    error: bool
    msg: str
    def __init__(self, error: bool = ..., msg: _Optional[str] = ...) -> None: ...
