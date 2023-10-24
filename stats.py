import numpy as np


def aggregate_ret(r):
    """
    returns the result of compounding the set of returns in r
    """
    return np.prod(1 + r) - 1


def compound(r):
    """
    returns the result of compounding the set of returns in r
    """
    return np.expm1(np.log1p(r).sum())
