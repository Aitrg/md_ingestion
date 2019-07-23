'''
Created on 28-Jun-2018

@author: srinivasan
'''
import json
import logging

from scrapy.exceptions import NotConfigured, NotSupported
from scrapy.selector.unified import Selector
from scrapy.spiders import Spider
from scrapy.utils.iterators import csviter, xmliter
from scrapy.utils.spider import iterate_spider_output

from Data_scuff.extensions.Iterators import exceliter, pcsviter
from Data_scuff.spiders.__common import DataFormatterMixin, \
    LookupDatareaderMixin

logger = logging.getLogger(__name__)


class PandasCSVFeedSpider(Spider):

    delimiter = None 
    quotechar = None 
    headers = None

    def process_results(self, response, results):
        return results

    def adapt_response(self, response):
        return response

    def parse_row(self, response, row):
        raise NotImplementedError

    def parse_rows(self, response):
        for row in pcsviter(response, self.delimiter, self.headers, self.quotechar):
            ret = iterate_spider_output(self.parse_row(response, row))
            for result_item in self.process_results(response, ret):
                yield result_item
    
    def parse(self, response):
        if not hasattr(self, 'parse_row'):
            raise NotConfigured('You must define parse_row method in order to scrape this CSV feed')
        response = self.adapt_response(response)
        return self.parse_rows(response)

"""
Json Feed Spider
"""


class JsonFeedSpider(Spider, DataFormatterMixin, LookupDatareaderMixin):
    
    start_node = None

    def process_results(self, response, results):
        return results
    
    def adapt_response(self, response):
        return response

    def parse_row(self, response, row):
        raise NotImplementedError

    def parse_rows(self, response):
        jsonresponse = json.loads(response.body_as_unicode())[self.start_node] if self.start_node else  json.loads(response.body_as_unicode())
        rows = jsonresponse if isinstance(jsonresponse, list) else [jsonresponse]  
        for row in rows:
            ret = iterate_spider_output(self.parse_row(response, row))
            for result_item in self.process_results(response, ret):
                yield result_item
    
    def parse(self, response):
        if not hasattr(self, 'parse_row'):
            raise NotConfigured('You must define parse_row method in order to scrape this JSON feed')
        response = self.adapt_response(response)
        return self.parse_rows(response)

"""
Excel Feed Spiders
"""


class ExcelFeedSpider(Spider, DataFormatterMixin, LookupDatareaderMixin):
    
    header = 0
    skiprows = None
    dropna_thresh = None
    isEmpty_lineHeader = False
    sheet_name = 0,
    names = None,
    index_col = None,
    usecols = None,
    squeeze = False,
    dtype = None,
    engine = None,
    converters = None,
    true_values = None,
    false_values = None,
    nrows = None,
    na_values = None,
    parse_dates = False,
    date_parser = None,
    thousands = None,
    comment = None,
    skipfooter = 0
    
    def process_results(self, response, results):
        return results
    
    def adapt_response(self, response):
        return response

    def parse_row(self, response, row):
        raise NotImplementedError

    def parse_rows(self, response):
        for row in exceliter(response, self.header, self.skiprows, self.sheet_name,
                             self.dropna_thresh, self.isEmpty_lineHeader):
            ret = iterate_spider_output(self.parse_row(response, row))
            for result_item in self.process_results(response, ret):
                yield result_item
    
    def parse_excel(self, response):
        self.logger.info("parse data from url {}".format(response.url))
        if not hasattr(self, 'parse_row'):
            raise NotConfigured('You must define parse_row method in order to scrape this Excel feed')
        response = self.adapt_response(response)
        return self.parse_rows(response)


class DocTablesFeedSpider(Spider):
    pass

"""
Multiple csv feed spider
"""


class MCSVFeedSpider(Spider, DataFormatterMixin, LookupDatareaderMixin):
    delimiter = None  
    quotechar = None
    headers = None

    def process_results(self, response, results):
        return results

    def adapt_response(self, response):
        return response

    def parse_row(self, response, row):
        raise NotImplementedError

    def parse_rows(self, response):
        for row in csviter(response, self.delimiter, self.headers, self.quotechar):
            ret = iterate_spider_output(self.parse_row(response, row))
            for result_item in self.process_results(response, ret):
                yield result_item

    def parse(self, response):
        raise NotImplementedError
    
    def parse_Csv(self, response):
        if not hasattr(self, 'parse_row'):
            raise NotConfigured('You must define parse_row method in order to scrape this CSV feed')
        response = self.adapt_response(response)
        return self.parse_rows(response)

"""
Multiple Json Feed Spiders
"""


class MJsonFeedSpider(Spider, DataFormatterMixin, LookupDatareaderMixin):
    start_node = None

    def process_results(self, response, results):
        return results
    
    def adapt_response(self, response):
        return response

    def parse_row(self, response, row):
        raise NotImplementedError

    def parse_rows(self, response):
        jsonresponse = json.loads(response.body_as_unicode())[self.start_node] if self.start_node else  json.loads(response.body_as_unicode())
        rows = jsonresponse if isinstance(jsonresponse, list) else [jsonresponse]  
        for row in rows:
            ret = iterate_spider_output(self.parse_row(response, row))
            for result_item in self.process_results(response, ret):
                yield result_item
    
    def parse(self, response):
        raise NotImplementedError
    
    def parse_json(self, response):
        if not hasattr(self, 'parse_row'):
            raise NotConfigured('You must define parse_row method in order to scrape this JSON feed')
        response = self.adapt_response(response)
        return self.parse_rows(response)

"""
Multi Xml Feed Spider
"""


class MXMLFeedSpider(Spider, DataFormatterMixin, LookupDatareaderMixin):
    iterator = 'iternodes'
    itertag = 'item'
    namespaces = ()

    def process_results(self, response, results):
        return results

    def adapt_response(self, response):
        return response

    def parse_node(self, response, selector):
        if hasattr(self, 'parse_item'):  # backward compatibility
            return self.parse_item(response, selector)
        raise NotImplementedError

    def parse_nodes(self, response, nodes):
        for selector in nodes:
            ret = iterate_spider_output(self.parse_node(response, selector))
            for result_item in self.process_results(response, ret):
                yield result_item
    
    def parse(self, response):
        raise NotImplementedError

    def parse_XML(self, response):
        if not hasattr(self, 'parse_node'):
            raise NotConfigured('You must define parse_node method in order to scrape this XML feed')
        response = self.adapt_response(response)
        if self.iterator == 'iternodes':
            nodes = self._iternodes(response)
        elif self.iterator == 'xml':
            selector = Selector(response, type='xml')
            self._register_namespaces(selector)
            nodes = selector.xpath('//%s' % self.itertag)
        elif self.iterator == 'html':
            selector = Selector(response, type='html')
            self._register_namespaces(selector)
            nodes = selector.xpath('//%s' % self.itertag)
        else:
            raise NotSupported('Unsupported node iterator')

        return self.parse_nodes(response, nodes)

    def _iternodes(self, response):
        for node in xmliter(response, self.itertag):
            self._register_namespaces(node)
            yield node

    def _register_namespaces(self, selector):
        for (prefix, uri) in self.namespaces:
            selector.register_namespace(prefix, uri)
