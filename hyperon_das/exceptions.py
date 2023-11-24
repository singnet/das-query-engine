class BaseException(Exception):
    """
    Base class to exceptions
    """

    def __init__(self, message: str, details: str = ""):
        self.message = message
        self.details = details

        super().__init__(self.message, self.details)


class DasTypeException(BaseException):
    ...  # pragma no cover


class MethodNotAllowed(BaseException):
    ...  # pragma no cover


class QueryParametersException(BaseException):
    ...  # pragma no cover


class UnexpectedQueryFormat(BaseException):
    ...  # pragma no cover


class InvalidAssignment(BaseException):
    ...  # pragma no cover


class InitializeDasServerException(BaseException):
    ...  # pragma no cover


class ConnectionServerException(BaseException):
    ...  # pragma no cover


class RetryException(BaseException):
    ...  # pragma no cover
