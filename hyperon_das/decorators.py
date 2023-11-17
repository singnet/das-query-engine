from functools import wraps
from time import time
from typing import Callable
from hyperon_das.logger import logger


def record_execution_time():
    def decorator(function: Callable) -> Callable:
        @wraps(function)
        def wrapper(*args, **kwargs):
            start_time = time()
            logger().info(
                f'Execution of "{function.__module__}.{function.__qualname__}" starting...'
            )
            print(
                f'Execution of "{function.__module__}.{function.__qualname__}" starting...'
            )
            result = function(*args, **kwargs)
            time_spent = '{0:.5f} seconds'.format(time() - start_time)
            logger().info(
                f'Execution of "{function.__module__}.{function.__qualname__}" took {time_spent}'
            )
            print(
                f'Execution of "{function.__module__}.{function.__qualname__}" took {time_spent}'
            )
            return result

        return wrapper

    return decorator