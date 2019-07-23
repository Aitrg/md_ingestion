# -*- coding: utf-8 -*-
'''
Created on 22-Jul-2018

@author: srinivasan
'''
import logging

from jinja2.environment import Environment
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.mail import MailSender
from twisted.internet import task

from Data_scuff import config

logger = logging.getLogger(__name__)


class PerodicStatsSender:
    
    def __init__(self, crawler):
        if not crawler.settings.getbool('PERODICSTATS_MAIL_ENABLED'):
            raise NotConfigured('PERODICSTATS_MAIL_ENABLED must be set')
        self.crawler = crawler
        self.notify_mails = crawler.settings.getlist('PERODICSTATS_NOTIFY_MAIL')
        self.check_interval = crawler.settings.getfloat('PERODICSTATS_CHECK_INTERVAL_MIN')
        self.mail = MailSender.from_settings(crawler.settings)
        crawler.signals.connect(self.engine_started, signal=signals.engine_started)
        crawler.signals.connect(self.engine_stopped, signal=signals.engine_stopped)
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)
    
    def engine_started(self):
        self.tsk = task.LoopingCall(self.__collectStatsAndSendMail)
        self.tsk.start(self.check_interval * 60, now=True)
    
    def __collectStatsAndSendMail(self):
        stats = self.crawler.stats.get_stats()
        self.mail.send(to=self.notify_mails,
                        subject='{} PerodicStats Report Mail'.format(self.crawler.settings.get('JIRA_ID')),
                        body=Environment().from_string(config.HTML).render({'stats':stats}),
                        mimetype='text/html',
                         _callback=self._catch_mail_sent)
    
    def _catch_mail_sent(self, **kwargs):
        logger.info("Mail Send Notification")
    
    def engine_stopped(self):
        if self.tsk.running:
            self.tsk.stop()
            
