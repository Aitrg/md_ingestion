'''
Created on 07-Feb-2019

@author: srinivasan
'''

from scrapy.http.request import Request


class CaptureFailureMiddleware:
    pass


class RecrawlMiddleware:
    
    def __init__(self, settings):
        pass

    """def process_spider_output(self, response, result, spider):
        if hasattr(spider, "recrawl") and spider.recrawl:
            for x in result:
                pass
        else:
            yield result
        for x in result:
            if  isinstance(x, Request) and hasattr(spider, "recrawl") and spider.recrawl:
                print("-------------------------------------------------------------->", spider.start)
            elif isinstance(x, Request):
                pass
            else:
                yield x"""
    
    @classmethod   
    def from_crawler(cls, crawler):
        return cls(crawler.settings)
    

class AddExtraColumnsMiddleware:
    
    def __init__(self, settings):
        self.settings = settings
        self.modify_field_export = False
    
    def process_spider_output(self, response, result, spider):
        """
        modify the spider fields information
        """
        if not self.modify_field_export:
            self.modify_field_export = True
            spider.custom_settings.get('FIELDS_TO_EXPORT').append('unique_id')
            spider.custom_settings.get('TOP_HEADER').update({'unique_id':'unique_id'})
        for x in result:
            if not isinstance(x, Request):
                # x['unique_id'] = str(response.meta['unique_id'])
                x['unique_id'] = str(response.meta.get('unique_id'))
            yield x
    
    @classmethod   
    def from_crawler(cls, crawler):
        return cls(crawler.settings)
