class BaseException(Exception):
    """
    Base class to exceptions
    """

    def __init__(self, message: str, details: str = ""):
        self.message = message
        self.details = details

        super().__init__(self.message, self.details)


class DatabaseTypeException(BaseException):
    ...  # pragma no cover


class MethodNotAllowed(BaseException):
    ...  # pragma no cover
