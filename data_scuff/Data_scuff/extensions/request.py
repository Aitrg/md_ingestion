'''
Created on 15-Jul-2018

@author: srinivasan
'''
from urllib.parse import urlencode

import requests
from scrapy.http.request import Request
from scrapy.http.request.form import FormRequest
from scrapy.http.response.html import HtmlResponse
from scrapy.utils.python import to_bytes, is_listlike


class SkipSpecialCharEncFormRequest(FormRequest):

    def __init__(self, *args, **kwargs):
        formdata = kwargs.pop('formdata', None)
        if formdata and kwargs.get('method') is None:
            kwargs['method'] = 'POST'

        super(FormRequest, self).__init__(*args, **kwargs)

        if formdata:
            items = formdata.items() if isinstance(formdata, dict) else formdata
            querystr = _urlencode(items, self.encoding)
            if self.method == 'POST':
                self.headers.setdefault(b'Content-Type', b'application/x-www-form-urlencoded')
                self._set_body(querystr)
            else:
                self._set_url(self.url + ('&' if '?' in self.url else '?') + querystr)


def _urlencode(seq, enc):
    values = [(to_bytes(k, enc), to_bytes(v, enc))
              for k, vs in seq
              for v in (vs if is_listlike(vs) else [vs]) if v != '+']
    return urlencode(values, doseq=1)


class viewStateFormRequest(FormRequest):
    
    def __init__(self, *args, **kwargs):
        url = kwargs.get('url', None)
        formdata = kwargs.get('formdata', None)
        if url and formdata:
            kwargs['formdata'] = self.__update_in_alist(formdata, '__VIEWSTATE', self.__getviewState(url))
        super().__init__(*args, **kwargs)
    
    def __getviewState(self, url):
        req = requests.get(url)
        response = HtmlResponse(url=url, body=req.text, encoding='utf-8')
        return response.xpath("//input[@name='__VIEWSTATE']/@value").extract_first() or\
             response.xpath("//input[@id='__VIEWSTATE']/@value").extract_first()

    def __update_in_alist(self, alist, key, value):
        return [(k, v) if (k != key) else (key, value) for (k, v) in alist]


class SeleniumRequest(Request):
    
    def __init__(self, wait_time=None, wait_until=None, screenshot=False, *args, **kwargs):
        self.wait_time = wait_time
        self.wait_until = wait_until
        self.screenshot = screenshot
        super().__init__(*args, **kwargs)
