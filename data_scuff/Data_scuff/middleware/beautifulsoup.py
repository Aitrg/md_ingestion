'''
Created on 13-Aug-2018

@author: srinivasan
'''
from bs4 import BeautifulSoup

# BEAUTIFULSOUP_PARSER = "html5lib"  # or BEAUTIFULSOUP_PARSER = "lxml"


class BeautifulSoupMiddleware:
    
    def __init__(self, settings):
        super(BeautifulSoupMiddleware, self).__init__()
        self.parser = settings.get('BEAUTIFULSOUP_PARSER', "html.parser")
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)
    
    def process_response(self, request, response, spider):
        return response.replace(body=str(BeautifulSoup(response.body, self.parser)))