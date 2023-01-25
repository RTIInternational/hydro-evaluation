import os
import time
from functools import wraps
from pathlib import Path

import grids.config as config


def get_cache_dir(create: bool = True):

    if not os.path.exists(config.NWM_CACHE_DIR) and create:
        os.mkdir(config.NWM_CACHE_DIR)

    if not os.path.exists(config.NWM_CACHE_DIR):
        raise NotADirectoryError

    return config.NWM_CACHE_DIR

def make_parent_dir(filepath):
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)

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
