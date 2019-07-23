'''
Created on 20-Jun-2018

@author: srinivasan
'''
import zlib

from scrapy.downloadermiddlewares.httpcompression import HttpCompressionMiddleware, \
    ACCEPTED_ENCODINGS

from Data_scuff.utils.gz import gunzip

try:
    import brotli
    ACCEPTED_ENCODINGS.append(b'br')
except ImportError:
    pass


class CustomHttpCompressionMiddleWare(HttpCompressionMiddleware):
    
    def _decode(self, body, encoding):
        if encoding == b'gzip' or encoding == b'x-gzip':
            body = gunzip(body)
        if encoding == b'deflate':
            try:
                body = zlib.decompress(body)
            except zlib.error:
                body = zlib.decompress(body, -15)
        if encoding == b'br' and b'br' in ACCEPTED_ENCODINGS:
            body = brotli.decompress(body)
        return body
