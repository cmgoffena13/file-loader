import os
import time
from functools import wraps
from typing import Optional


def retry(attempts: int = 3, delay: float = 0.25, backoff: float = 2.0):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            wait = delay
            for i in range(attempts):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    if i == attempts - 1:
                        raise e
                    time.sleep(wait)
                    wait *= backoff

        return wrapper

    return decorator


def get_error_location(exception: Exception) -> Optional[str]:
    if not exception.__traceback__:
        return None

    tb = exception.__traceback__
    while tb.tb_next:
        tb = tb.tb_next
    frame = tb.tb_frame
    filename = os.path.basename(frame.f_code.co_filename)
    return f"{filename}:{tb.tb_lineno}"
