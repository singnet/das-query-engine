import time
from functools import wraps
from typing import Callable

from hyperon_das.exceptions import ConnectionServerException
from hyperon_das.logger import logger


def retry(attempts: int, timeout_seconds: int):
    def decorator(function: Callable) -> Callable:
        @wraps(function)
        def wrapper(*args, **kwargs):
            waiting_time_seconds = 0.2
            retry_count = 0
            timer_count = 0

            while retry_count < attempts and timer_count < timeout_seconds:
                try:
                    start_time = time.time()
                    response = function(*args, **kwargs)
                    end_time = time.time()
                    if response is not None:
                        logger().debug(
                            f'{retry_count + 1} successful connection attempt at [host={args[1]}]'
                        )
                        return response
                except Exception as e:
                    raise ConnectionServerException(
                        message="An error occurs while connecting to the server",
                        details=str(e),
                    )
                else:
                    logger().debug(f'{retry_count + 1} unsuccessful connection attempt')
                    time.sleep(waiting_time_seconds)
                    retry_count += 1
                    timer_count += end_time - start_time
            port = f':{args[2]}' if args[2] else ''
            message = (
                f'Failed to connect to remote Das {args[1]}'
                + port
                + f' - attempts:{retry_count} - time_attempted: {timer_count}'
            )
            logger().info(message)
            raise ConnectionError(message)

        return wrapper

    return decorator
