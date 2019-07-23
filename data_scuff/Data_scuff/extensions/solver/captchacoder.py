'''
Created on 22-Nov-2018

@author: srinivasan
'''
from random import randint
import requests
import base64


class CaptchaCoderApi:
    
    def __init__(self, baseurl, api_key, **kwargs):
        self.baseurl = baseurl
        self.api_key = api_key
        self.site_key = kwargs.get('site_key', None)
    
    def resolver(self, lastparam):
        rando_id = str(randint(1, 1000))
        if self.site_key:
            """
            google captcha
            """
            __url = '{}?action=upload&captchatype=3&key={}&sitekey={}&gen_task_id={}&pageurl={}'.format(self.baseurl, self.api_key, self.site_key, rando_id, lastparam) 
        ret = requests.get(__url)
        return ret.text
    
    def resolveImgCaptcha(self, img_url):
        data = {'action':'upload',
                'key':self.api_key,
                'file':self.__get_as_base64(img_url),
                'gen_task_id':str(randint(1, 1000)),
                'captchatype':'2'} 
        r = requests.post(url=self.baseurl, data=data) 
        print(r)
        result = r.text
        if 'Error' in result:
            result = None
        return result
    
    def balance_query(self):
        data = {'action':'refund',
                'key':self.api_key,
                'gen_task_id':str(randint(1, 1000))} 
        r = requests.post(url=self.baseurl, data=data) 
        print(r.json())
        result = r.text
        if 'Error' in result:
            result = None
        return result
    
    def __get_as_base64(self, url):
        return base64.b64encode(requests.get(url).content)
    
