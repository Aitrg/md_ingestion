'''
Created on 19-Jun-2018

@author: srinivasan
'''
import logging

from scrapy.core.downloader.handlers.http11 import HTTP11DownloadHandler, \
    ScrapyAgent, _ResponseReader
from twisted.web.iweb import UNKNOWN_LENGTH
from twisted.internet import defer

logger = logging.getLogger(__name__)


class CustomHttpDownloadHandler(HTTP11DownloadHandler):
    
    def download_request(self, request, spider):
        agent = CustomScrapyAgent(contextFactory=self._contextFactory, pool=self._pool)
        return agent.download_request(request)


class CustomScrapyAgent(ScrapyAgent):
    
    def _cb_bodyready(self, txresponse, request):
        if txresponse.length == 0:
            return txresponse, b'', None
        maxsize = request.meta.get('download_maxsize', self._maxsize)
        warnsize = request.meta.get('download_warnsize', self._warnsize)
        expected_size = txresponse.length if txresponse.length != UNKNOWN_LENGTH else -1
        fail_on_dataloss = request.meta.get('download_fail_on_dataloss', self._fail_on_dataloss)
        if maxsize and expected_size > maxsize:
            error_msg = ("Cancelling download of %(url)s: expected response "
                         "size (%(size)s) larger than download max size (%(maxsize)s).")
            error_args = {'url': request.url, 'size': expected_size, 'maxsize': maxsize}
            logger.error(error_msg, error_args)
            txresponse._transport._producer.loseConnection()
            raise defer.CancelledError(error_msg % error_args)

        if warnsize and expected_size > warnsize:
            logger.warning("Expected response size (%(size)s) larger than "
                           "download warn size (%(warnsize)s) in request %(request)s.",
                           {'size': expected_size, 'warnsize': warnsize, 'request': request})

        def _cancel(_):
            txresponse._transport._producer.abortConnection()

        d = defer.Deferred(_cancel)
        txresponse.deliverBody(_Custom_ResponseReader(
            d, txresponse, request, maxsize, warnsize, fail_on_dataloss))
        self._txresponse = txresponse
        return d


class _Custom_ResponseReader(_ResponseReader):

    def dataReceived(self, bodyBytes):
        if self._finished.called:
            return
        self._bodybuf.write(bodyBytes)
        self._bytes_received += len(bodyBytes)
        from humanize import naturalsize
        logger.debug("Received %(bytes)10s",
                         {'bytes': naturalsize(self._bytes_received)})
        if self._maxsize and self._bytes_received > self._maxsize:
            logger.error("Received (%(bytes)s) bytes larger than download "
                         "max size (%(maxsize)s) in request %(request)s.",
                         {'bytes': self._bytes_received,
                          'maxsize': self._maxsize,
                          'request': self._request})
            self._bodybuf.truncate(0)
            self._finished.cancel()

        if self._warnsize and self._bytes_received > self._warnsize and not self._reached_warnsize:
            self._reached_warnsize = True
            logger.warning("Received more bytes than download "
                           "warn size (%(warnsize)s) in request %(request)s.",
                           {'warnsize': self._warnsize,
                            'request': self._request})