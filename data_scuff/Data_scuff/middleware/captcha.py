'''
Created on 26-Jul-2018

@author: srinivasan
'''

from bs4 import BeautifulSoup
import logging
from scrapy.exceptions import IgnoreRequest, CloseSpider

from Data_scuff.extensions.solver.captchacoder import CaptchaCoderApi

logger = logging.getLogger(__name__);


class CaptchaMiddleware:
    
    def __init__(self, baseurl, api_key, max_retries=5):
        self.baseurl = baseurl
        self.api_key = api_key
        self.max_retries = max_retries
    
    def findCaptchaUrl(self, page, img_key, formid=0):
        soup = BeautifulSoup(page, 'lxml')
        forms = soup.find_all("form")
        if len(forms) != 1 and formid == 0:
            logger.warning("Unable to find an image in the CAPTCHA form. please set the formid")
            return None
        form = forms[formid]
        return form.find("img", img_key).get('src')
    
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls(baseurl=crawler.settings.get('CAPTCHA_CODER_BASE_URL'),
                          api_key=crawler.settings.get('CAPTCHA_CODER_API_KEY'),
                          max_retries=crawler.settings.getint('CAPTCHA_CODER_MAX_RETRY', 5))
        return middleware
    
    def process_response(self, request, response, spider):
        if not request.meta.get('solve_captcha', False):
            return response
        if request.meta.get('captcha_id', None) or request.meta.get('captcha_class', None):
            captcha_key = {"id" : request.meta.get('captcha_id', '')} or {"class" : request.meta.get('captcha_class', '')}
            captchaUrl = response.urljoin(self.findCaptchaUrl(response.text, captcha_key))
            resolver = CaptchaCoderApi(self.baseurl, self.api_key)
            solved = resolver.resolveImgCaptcha(captchaUrl)
        elif request.meta.get('site_key', None):
            kwargs = {'site_key':request.meta.get('site_key')}
            resolver = CaptchaCoderApi(self.baseurl, self.api_key, **kwargs)
            captchaUrl = request.url
            solved = resolver.resolver(request.url)
        else:
            raise CloseSpider('we could not able to find captcha type. please set in request meta')
        if solved:
            response.request = request
            response.meta['captchaUrl'] = captchaUrl
            response.meta['solved_catpcha'] = solved
            return response
        else:
            if request.meta.get('catpcha_retries', 0) == self.max_retries:
                logging.warning('max retries for captcha reached for {}'.format(request.url))
                raise IgnoreRequest 
            request.meta['dont_filter'] = True
            request.meta['captcha_retries'] = request.meta.get('captcha_retries', 0) + 1
            return request
