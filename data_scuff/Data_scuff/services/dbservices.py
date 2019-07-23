'''
Created on 08-Jun-2018

@author: srinivasan
'''
from Data_scuff.db.client.mysqlclient import MysqlClient


class MysqlService:
    
    def __init__(self, *args, **kwargs):
        self.__client = MysqlClient(**kwargs)
    
    def get_row(self, query):  
        pass
    
    def get_rows(self, query):
        pass
    
    def execute(self, query):
        pass
    
    def close(self):
        if self.__client:
            self.__client.close()
