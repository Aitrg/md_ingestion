'''
Created on 16-May-2018

@author: srinivasan
'''
from Data_scuff.decorator import singleton
from twisted.enterprise import adbapi


@singleton
class MysqlClient:
    
    def __init__(self, *args, **kwargs):
        self.__db = adbapi.ConnectionPool('pymysql', **kwargs)
    
    """
    get Data Base Connection from pool
    """

    def getDbConnection(self):
        return self.__db
    
    """
    Close the data base connection
    """

    def close(self):
        if self.__db:
            self.__db.close()
    
