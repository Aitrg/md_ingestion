'''
Created on 16-May-2018

@author: srinivasan
'''
import logging
from pymysql.cursors import DictCursor
from Data_scuff.services.dbservices import MysqlService

logger = logging.getLogger(__name__)
logger.setLevel('DEBUG')


class MysqlPipeline:
    
    stats_name = 'mysql_pipeline'
    
    def __init__(self, crawler):
        self.stats = crawler.stats
        self.settings = crawler.settings
        db_args = {
            'host': self.settings.get('MYSQL_HOST', 'localhost'),
            'port': self.settings.get('MYSQL_PORT', 3306),
            'user': self.settings.get('MYSQL_USER', None),
            'password': self.settings.get('MYSQL_PASSWORD', ''),
            'db': self.settings.get('MYSQL_DB', None),
            'charset': 'utf8',
            'cursorclass': DictCursor,
            'cp_reconnect': True,
        }
        self.retries = self.settings.get('MYSQL_RETRIES', 3)
        self.close_on_error = self.settings.get('MYSQL_CLOSE_ON_ERROR', True)
        self.upsert = self.settings.get('MYSQL_UPSERT', False)
        self.table = self.settings.get('MYSQL_TABLE', None)
        self.service = MysqlService(**db_args)
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)
    
    def preprocess_item(self, item):
        return item
    
    """
    Close the Spider connection
    """

    def close_spider(self, spider):
        self.service.close()
