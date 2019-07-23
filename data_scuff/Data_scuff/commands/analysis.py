'''
Created on 17-Feb-2019

@author: srinivasan
'''
import os
from scrapy.commands import ScrapyCommand

from scrapy.exceptions import UsageError

from Data_scuff.utils.fileutils import FileUtils
import pandas as pd


class Command(ScrapyCommand):
    
    requires_project = False
    default_settings = {'LOG_ENABLED': False}

    def short_desc(self):
        return "start to analysis tracking table"
    
    def syntax(self):
        return "[options] <Jira_id> <tracking_file>"
    
    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        
    def run(self, args, opts):
        if len(args) != 2:
            raise UsageError()
        jira_id = args[0]
        tracking_filename = args[1]
        tracking_path = os.path.join(self.settings.get('REQUEST_TRACKING_PATH'), jira_id,
                              tracking_filename)
        if not FileUtils.isExist(tracking_path):
            print("tracking path is not exists:{0}".format(tracking_path))
            return
    
        class AnalysisTracking:
            
            def __init__(self, tracking_path, out_path):
                self.tracking_path = tracking_path
                self.out_path = out_path
            
            def analysis(self):
                df = pd.read_csv(self.tracking_path, sep='|')
                df["status"].fillna(0.0, inplace=True)
                request = df.loc[df['type'] == 'request'].reset_index(drop=True)[['unique_id']]
                response = df.loc[df['type'] == 'response'].reset_index(drop=True)[['unique_id', 'status']]
                df = pd.merge(request, response, how='left', on=['unique_id'])
                df["status"].fillna(0.0, inplace=True)
                res = df.groupby(['status']).size().reset_index(name='counts')
                res.to_csv(self.out_path, sep='|', encoding='utf-8',
                           index=False)
                print(res)

        class_analysis = AnalysisTracking(tracking_path,
                                    os.path.join(self.settings.get('REQUEST_TRACKING_PATH'),
                                                                 jira_id,
                              'analysis.csv'))
        class_analysis.analysis()
