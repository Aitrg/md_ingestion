'''
Created on 21-Jul-2018

@author: srinivasan
'''
import datetime


class DateUtils:
    
    @staticmethod
    def isoformat_as_datetime(s):
        return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')
    
    @staticmethod
    def datetoString(val, __format='%m/%d/%Y'):
        from dateutil import parser
        dt = parser.parse(val)
        return dt.strftime(__format)
    
    @staticmethod
    def jsonDatetoDate(val, __format='%m/%d/%Y'):
        
        if val and len(val) >= 10:
            import re
            TimestampUtc = re.split('\(|\)', val)[1][:10]
            dt = datetime.datetime.fromtimestamp(int(TimestampUtc))
            return dt.strftime(__format)
        return ''

