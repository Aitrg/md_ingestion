'''
Created on 17-Feb-2019

@author: srinivasan
'''
import os
from scrapy.commands import ScrapyCommand

from scrapy.exceptions import UsageError
import pandas as pd 

from Data_scuff.utils.fileutils import FileUtils


class Command(ScrapyCommand):
    requires_project = False
    default_settings = {'LOG_ENABLED': False}
    
    def short_desc(self):
        return "collecting missing URL"
    
    def syntax(self):
        return "[options] <Jira_id> <tracking_file> <data_file> <final_method_name>"
    
    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        
    def run(self, args, opts):
        if len(args) != 4:
            raise UsageError()
        jira_id = args[0]
        tracking_filename = args[1]
        data_file = args[2]
        final_method_name = args[3]
        tracking_path = os.path.join(self.settings.get('REQUEST_TRACKING_PATH'), jira_id,
                              tracking_filename)
        if not FileUtils.isExist(tracking_path):
            print("tracking path is not exists:{0}".format(tracking_path))
            return
        data_path = os.path.join(self.settings.get('STORAGE_DIR'), jira_id,
                              data_file)
        if not FileUtils.isExist(data_path):
            print("data_path path is not exists:{0}".format(data_path))
            return
        final_df = self.__collectMissingUrl(data_path, tracking_path, final_method_name)
        if not final_df.empty:
            out_path = os.path.join(self.settings.get('REQUEST_TRACKING_PATH'),
                                     jira_id, 'missing_url.csv')
            final_df.to_csv(out_path, sep='|', encoding='utf-8',
                           index=False)
        else:
            print("No Missing URL Captured")
    
    def __collectMissingUrl(self, data_path,
                            tracking_path, final_method_name):
        df = pd.read_csv(tracking_path, sep='|')
        if final_method_name not in list(df['callback'].unique()):
            print("passing Method not found:{0}".format(data_path))
            return
        request = df.loc[(df['type'] == 'request') 
                  & (df['callback'] == final_method_name)
                  ].reset_index(drop=True)
        response = df.loc[(df['type'] == 'response') 
                          & (df['callback'] == final_method_name)
                          ].reset_index(drop=True)[['unique_id', 'status']]
        response.columns = ['unique_id', 'response_status']
        data = pd.read_csv(data_path, sep='|', skiprows=1).drop_duplicates(
                subset=['unique_id'], keep='first')[['unique_id']]
        df = pd.merge(request, data, on=['unique_id'], how="outer", indicator=True)
        left = df[df['_merge'] == 'left_only'].reset_index(drop=True)
        return pd.merge(left, response, on=['unique_id'], how="left")
    
