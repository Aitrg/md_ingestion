'''
Created on 12-May-2018

@author: srinivasan
'''
from urllib.parse import urljoin

import scrapy

from Data_scuff.spiders.__common import CommonSpider
from Data_scuff.utils.utils import  ExtractPDFTables


class CityOfChino(CommonSpider):
    
    name = "cityofchino"
    allowed_domains = ["cityofchino.org"]
    URL = "http://www.cityofchino.org"
    start_urls = ["http://www.cityofchino.org/government-services/community-development/building/building-permit-activity-reports", ]
    
    def parse(self, response):
        self.logger.info('we are going to process  on %s', response.url)
        urls = [urljoin(self.URL, s.xpath("@href").extract()[0])
            for s in response.css("div#widget_4_466_622 p > a") 
          if "Detail" in s.xpath("text()").extract()[0]]
        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.extractPdfContent)
    
    def extractPdfContent(self, response):
        extract = ExtractPDFTables()
        self.logger.info(extract.extractTableData(response))
        final_vals = []
        for v in [[self.addEmptyVal(k) for k in i if len(k) > 6] \
                   for i in extract.extractTableData(response)]:
            keys = v.pop(0)
            final_vals += [dict(zip(keys, kv)) for kv in v]
        return final_vals
    
    def addEmptyVal(self, l):
        if len(l) < 8 and not l[0].isdigit():
            l.insert(0, "")
        return l
