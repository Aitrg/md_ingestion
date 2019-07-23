'''
Created on 16-Nov-2018

@author: srinivasan
'''
from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError
import tldextract


class Command(ScrapyCommand):
    
    requires_project = False
    default_settings = {'LOG_ENABLED': False}

    def syntax(self):
        return "[options] <URL>"

    def short_desc(self):
        return "Check Similar Ticket"
    
    def run(self, args, _):
        if len(args) != 1:
            raise UsageError()
        url = args[0]
        ext = tldextract.extract(url)
        domain = ".".join([ext.domain, ext.suffix])
        print("Current Domain: " + domain)
        jira_ids = []
        for name in self.crawler_process.spider_loader.list():
            spidercls = self.crawler_process.spider_loader.load(name)
            if domain in spidercls.allowed_domains and spidercls.custom_settings:
                __id = spidercls.custom_settings['JIRA_ID']
                if __id:
                    jira_ids.append(__id)
        if jira_ids:
            print("Similar Tickets IDS: [{}]".format(','.join(jira_ids)))
        else:
            print("Domain {} new to our system. so there is no reference".format(domain))
