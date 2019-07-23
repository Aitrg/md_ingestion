'''
Created on 11-Aug-2018

@author: srinivasan
'''
import re
from Data_scuff.utils.utils import Utils
from Data_scuff.utils.datacleaning import DataCleaning


class DataCorrectionPipeline:
    
    def __init__(self, settings):
        self.settings = settings
        self.preprocessor = DataCleaning()
    
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        return pipeline
    
    def process_item(self, item, spider):
        if item:
            for name, value in item.items():
                if name not in ['sourceName', 'ingestion_timestamp', 'url']:
                    if isinstance(value, list):
                        item[name] = [self.__blankUnwantedValue(self.preprocessor.clean(
                            self.___cleanData(self.__htmlEntitytoUnicode(v)))) for v in value]
                    else:
                        item[name] = self.__blankUnwantedValue(self.preprocessor.clean(
                            self.___cleanData(self.__htmlEntitytoUnicode(value))))
        return item
    
    """
    remove blank values
    """

    def __blankUnwantedValue(self, val):
        __val = val.replace('$', '')
        if __val and (__val in ['0', '0.0'] or
                       all(v == '0' for v in __val)):
            return ''
        return val
    
    def ___cleanData(self, val):
        return re.sub(r'\.+', ".", re.sub(r"\s\s+", " ",
                      re.sub(r"[\n\t\"]*", "",
                              val.replace('|', " "))))
    
    def __htmlEntitytoUnicode(self, val):
        if val:
            import html
            return html.unescape(val)
        else:
            return ''


class DataReplacePipeline:
    
    def __init__(self, settings):
        self.settings = settings
    
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        return pipeline
    
    def process_item(self, item, spider):
        # startsWith
        # endsWith
        # Contains
        # Equals
        replace_val = self.settings.getdict('REPLACE_VALUES', None)
        if replace_val and len(replace_val) > 0:
            for name, vals in replace_val.items():
                for val in vals:
                    if name in item:
                        
                        if isinstance(item[name], list):
                            item[name] = [val.get('replace', None) or '' 
                                         if self.__check(v, val['value'],
                                             val.get('type', None)
                            if val.get('type', None) 
                                else 'equals') else v for v in item[name]]
                        else:
                            item[name] = val.get('replace', None) or \
                                '' if item[name] == val['value'] else item[name]
        return item
    
    def __check(self, v1, v2, condition="Equals"):
        if v1 == '' or v1 == None or v2 == None: 
            return False
        check_cond = {'startswith':v1.startswith(v2),
                      'endswith':v1.endswith(v2),
                      'equals':v1 == v2,
                      'contains':v2 in v1}
        return check_cond.get(condition.lower(), False)


class AddCommonColumnPipeline:

    def __init__(self, settings):
        self.settings = settings
        self.timestamp = Utils.getingestion_timestamp()
    
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        return pipeline
    
    def process_item(self, item, spider):
        item['ingestion_timestamp'] = self.timestamp
        return item


class DataValidatorPipeline:
    
    def __init__(self, settings):
        self.settings = settings
    
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        return pipeline
    
    def process_item(self, item, spider):
        return item
