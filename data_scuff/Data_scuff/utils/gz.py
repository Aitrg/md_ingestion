'''
Created on 20-Jun-2018

@author: srinivasan
'''
import struct

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO
from gzip import GzipFile

import six
import re

if six.PY2:

    def read1(gzf, size=-1):
        return gzf.read(size)

else:

    def read1(gzf, size=-1):
        return gzf.read1(size)


def gunzip(data):
    """Gunzip the given data and return as much data as possible.

    This is resilient to CRC checksum errors.
    """
    f = GzipFile(fileobj=BytesIO(data))
    output_list = []
    chunk = b'.'
    while chunk:
        try:
            chunk = read1(f, 8196)
            output_list.append(chunk)
        except (IOError, EOFError, struct.error):
            if output_list or getattr(f, 'extrabuf', None):
                try:
                    output_list.append(f.extrabuf[-f.extrasize:])
                finally:
                    break
            else:
                raise
    return b''.join(output_list)


_is_gzipped = re.compile(br'^application/(x-)?gzip\b').search
_is_octetstream = re.compile(br'^(application|binary)/octet-stream\b').search


def is_gzipped(response):
    """Return True if the response is gzipped, or False otherwise"""
    ctype = response.headers.get('Content-Type', b'').lower()
    cenc = response.headers.get('Content-Encoding', b'').lower()
    return (_is_gzipped(ctype) or
            (_is_octetstream(ctype) and cenc in (b'gzip', b'x-gzip')))


def gzip_magic_number(response):
    return response.body[:3] == b'\x1f\x8b\x08'
