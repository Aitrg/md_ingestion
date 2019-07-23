'''
Created on 24-Jun-2018

@author: srinivasan
'''
import json
import urllib

from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError

from Data_scuff.utils.utils import Utils
import certifi


class Command(ScrapyCommand):
    
    requires_project = False
    default_settings = {'LOG_ENABLED': False}

    def syntax(self):
        return "[options] <url>"

    def short_desc(self):
        return "extracting form data from url"
    
    def run(self, args, opts):
        if len(args) != 1:
            raise UsageError()
        url = args[0]
        if not Utils.is_valid_url(url):
            print('please use valid url-{}'.format(url))
            return
        request = urllib.request.Request(url)
        response = urllib.request.urlopen(request, cafile=certifi.where())
        body = Utils.parse_form(response.read().decode(response.headers.get_content_charset()))
        if not body or len(body) < 0:
            print('no form is present in  {}'.format(url))
        print(json.dumps(body, indent=4))
