from datetime import datetime
from functools import wraps
from time import time
from typing import Callable

from hyperon_das_atomdb.exceptions import (
    ConnectionServerException,
    RetryException,
)
from hyperon_das_atomdb.logger import logger


def set_is_toplevel(function: Callable) -> Callable:
    @wraps(function)
    def wrapper(*args, **kwargs):
        result = function(*args, **kwargs)
        result['is_toplevel'] = True
        return result

    return wrapper


def retry(attempts: int, timeout_seconds: int):
    def decorator(function: Callable) -> Callable:
        @wraps(function)
        def wrapper(*args, **kwargs):
            waiting_time_seconds = 0.2
            retry_count = 0
            timer_count = 0

            while retry_count < attempts and timer_count < timeout_seconds:
                try:
                    start_time = datetime.now()
                    response = function(*args, **kwargs)
                    end_time = datetime.now()
                    if response is not None:
                        return response
                except Exception as e:
                    raise ConnectionServerException(
                        message="An error occurs while connecting to the server",
                        details=str(e),
                    )
                else:
                    time.sleep(waiting_time_seconds)
                    retry_count += 1
                    timer_count += int((end_time - start_time).total_seconds())

            raise RetryException(
                message='The number of attempts has been exceeded or a timeout has occurred',
                details={
                    'attempts': retry_count,
                    'time': f'{timer_count} seconds',
                },
            )

        return wrapper

    return decorator


def record_execution_time():
    def decorator(function: Callable) -> Callable:
        @wraps(function)
        def wrapper(*args, **kwargs):
            start_time = time()
            result = function(*args, **kwargs)
            time_spent = '{0:.3f} seconds'.format(time() - start_time)
            logger().debug(
                f'Execution of "{function.__module__}.{function.__name__}" took {time_spent}'
            )
            return result

        return wrapper

    return decorator
