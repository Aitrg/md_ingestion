'''
Created on 03-Jun-2018

@author: srinivasan
'''
import datetime
from itertools import groupby
import random
import string

"""
Convert date into iso date format
"""


def isoformat_as_datetime(s):
    return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')


"""
truncate date object based on the resolution
"""


def truncate_datetime(t, resolution):
    resolutions = ['year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond']
    if resolution not in resolutions:
        raise KeyError("Resolution is not valid: {0}".format(resolution))
    args = []
    for r in resolutions:
        args += [getattr(t, r)]
        if r == resolution:
            break
    return datetime.datetime(*args)


"""
Create the Random string
"""


def random_string(length=6, alphabet=string.ascii_letters + string.digits):
    return ''.join([random.choice(alphabet) for _ in range(length)])


def groupby_dict(i, keyfunc=None):
    return dict((k, list(v)) for k, v in groupby(sorted(i, key=keyfunc), keyfunc))
    
