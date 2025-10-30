import time
from functools import wraps


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
