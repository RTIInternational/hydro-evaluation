import os
import time
from functools import wraps

import grids.config as config


def get_cache_dir(create: bool = True):

    if not os.path.exists(config.NWM_CACHE) and create:
        os.mkdir(config.NWM_CACHE)

    if not os.path.exists(config.NWM_CACHE):
        raise NotADirectoryError

    return config.NWM_CACHE


def profile(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        fn_kwargs_str = ', '.join(f'{k}={v}' for k, v in kwargs.items())
        print(f'\n{fn.__name__}({fn_kwargs_str})')

        # Measure time
        t = time.perf_counter()
        retval = fn(*args, **kwargs)
        elapsed = time.perf_counter() - t
        print(f'Time   {elapsed:0.4}')

        # Measure memory
        # mem, retval = memory_usage((fn, args, kwargs), retval=True, timeout=200, interval=1e-7)

        # print(f'Memory {max(mem) - min(mem)}')
        return retval

    return inner
