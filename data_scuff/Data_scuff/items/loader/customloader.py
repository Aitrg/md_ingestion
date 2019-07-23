'''
Created on 14-Jun-2018

@author: srinivasan
'''
from scrapy.loader import ItemLoader
from scrapy.utils.python import flatten


class ExtendItemLoader(ItemLoader):
    
    def add_xpathWithCondition(self, field_name, conditionxpath, successXpath, failXpath, *processors, **kw):
        
        xpath_val = successXpath if self.selector.xpath(conditionxpath).extract_first() \
            is not None else failXpath
        values = flatten([self.selector.xpath(xpath).extract() for xpath in [xpath_val]])
        self.add_value(field_name, values, *processors, **kw)
    
    def add_xpath_and_css(self, field_name, xpaths, csss, *processors, **kw):
        pass
