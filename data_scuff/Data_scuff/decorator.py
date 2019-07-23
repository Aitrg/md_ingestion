# -*- coding: utf-8 -*-
'''
Created on 08-Jun-2018

@author: srinivasan
'''
import functools
import logging
import time


def instrumentation(logger, level=None, __format='%s: %s ms'):
    if level is None:
        level = logging.DEBUG

    def decorator(fn):

        @functools.wraps(fn)
        def inner(*args, **kwargs):
            start = time.time()
            result = fn(*args, **kwargs)
            duration = time.time() - start
            logger.log(level, __format, repr(fn), duration * 1000)
            return result

        return inner

    return decorator


def sleep(seconds=5):
    
    def decorator(fn):
    
        @functools.wraps(fn)
        def inner(*args, **kwargs):
            sleep(seconds)
            return fn(*args, **kwargs)

        return inner

    return decorator


class __SingletonWrapper:

    def __init__(self, cls):
        self.__wrapped__ = cls
        self._instance = None

    def __call__(self, *args, **kwargs):
        if self._instance is None:
            self._instance = self.__wrapped__(*args, **kwargs)
        return self._instance


def singleton(cls):
    return __SingletonWrapper(cls)

