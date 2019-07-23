'''
Created on 29-Jun-2018

@author: srinivasan
'''
from Data_scuff.third_party.toripchanger.changer import TorIpChanger
from scrapy.settings import Settings

ip_changer = TorIpChanger(reuse_threshold=10)


class TorProxyMiddleware:
    
    _requests_count = 0
    
    def __init__(self, settings):
        self.proxy_url = settings.get('PROXY_URL')
        self.roate_per_request = Settings.getint('ROTATE_PROXY_PER_REQUEST', 10)
    
    def process_request(self, request, spider):
        if self.proxy_url:
            self._requests_count += 1
            if self._requests_count > self.roate_per_request:
                self._requests_count = 0 
                ip_changer.get_new_ip()
            request.meta['proxy'] = self.proxy_url