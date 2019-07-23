'''
Created on 22-Jul-2018

@author: srinivasan
'''
import logging

from jinja2 import Environment
from scrapy import signals
from scrapy.mail import MailSender

from Data_scuff import  config

logger = logging.getLogger(__name__)


class StatsMailSendPipeline:
    
    def __init__(self, settings):
        self.settings = settings
    
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        statsMail = cls(settings)
        crawler.signals.connect(statsMail.spider_closed,
                                signal=signals.spider_closed)
        return statsMail
    
    def spider_closed(self, spider):
        jira_id = spider.custom_settings['JIRA_ID']
        stats = spider.crawler.stats.get_stats()
        if 'downloader/exception_count' in stats  \
            and stats['downloader/exception_count'] > 0:
            subject = "failed"
        else:
            subject = "succeed"
        try:
            mailsender = MailSender.from_settings(self.settings)
            '''
            mailsender.send(to=self.settings.getlist('JOB_NOTIFICATION_EMAILS'),
                        subject='JIRA ID:{}  job ends with {}'.format(jira_id, subject),
                        body=Environment().from_string(config.HTML).render({'stats':stats,
                                                                            'jira':jira_id}),
                        mimetype='text/html', _callback=self._catch_mail_sent)
            '''
        except Exception:
            pass    
        
    def _catch_mail_sent(self, **kwargs):
        logger.info("Mail Send Notification")
