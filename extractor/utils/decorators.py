"""Decorators helpers for elegance, debug, efficiency and others"""

# %% --- imports ---
import functools
import asyncio
from datetime import datetime


class Nothing:
    pass


# %% --- helpers ---
def label_fn(fn):
    return f"{fn.__name__} <{fn.__module__}>"


# %% --- decorator ---
def _decorator(d):
    """Augument decorator d for better docs"""

    def __decorator(fn, *args, **kwargs):
        return functools.update_wrapper(d(fn, *args, **kwargs), fn)

    return __decorator


decorator = _decorator(_decorator)


# %% --- profile ---
@decorator
def profile(fn, label=None, return_duration=False):
    """Time running function and prints duration

    Parameters
    ----------
    fn: function
        Function to be timed
    label (optional): string
        Label used in prints, defaults to 'function name' <'function path'>

    Returns
    -------
    function
        Same fn from parameters
    """

    if not label:
        label = f"{fn.__name__} <{fn.__module__}>"

    def _profile(*args, **kwargs):
        print(f"{label}: started")
        start = datetime.now()
        result = fn(*args, **kwargs)
        end = datetime.now()
        duration = end - start
        print(f"{label}: duration={duration}")
        if return_duration:
            return duration, result
        else:
            return result

    return _profile


def timeout(seconds):
    @decorator
    def _timeout(fn):
        async def __timeout(*args, **kwargs):
            return await asyncio.wait_for(fn(*args, **kwargs), timeout=seconds)

        return __timeout

    return _timeout


#  %% --- n_arier ---
def n_arier(default=Nothing):
    @decorator
    def _n_arier(f):
        """
        Given binary function f(x, y), return an n_ary function.
        f(x, y, z) = f(x, f(y,z)), etc. Also allow f(x) = x.
        """

        def n_ary(*args):
            if len(args) == 0:
                if default is Nothing:
                    raise TypeError("n_ary needs at least one argument")
                else:
                    return default
            elif len(args) == 1:
                return args[0]
            else:
                return f(args[0], n_ary(*args[1:]))

        return n_ary

    return _n_arier
