import logging
from functools import wraps
from time import sleep
from typing import Callable

_logger = logging.getLogger(__name__)

def backoff(
    exceptions: tuple[Exception],
    start_sleep_time=0.1,
    factor=2,
    border_sleep_time=10,
    except_logger: Callable[[], None] = lambda: _logger.exception(
        "Connection error"
    )
):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного
    времени ожидания (border_sleep_time)
        
    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    sleep_time = start_sleep_time

    def func_wrapper(func):

        @wraps(func)
        def inner(*args, **kwargs):
            nonlocal sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                # except:
                    except_logger()

                sleep_time = (sleep_time * factor) \
                    if (sleep_time * factor) < border_sleep_time \
                    else border_sleep_time

                sleep(sleep_time)

        return inner

    return func_wrapper