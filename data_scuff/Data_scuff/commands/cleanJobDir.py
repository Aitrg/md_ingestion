'''
Created on 20-Sep-2018

@author: srinivasan
'''
from __future__ import print_function
from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError
import os
from Data_scuff.utils.fileutils import FileUtils


class Command(ScrapyCommand):
    
    requires_project = False
    default_settings = {'LOG_ENABLED': False}

    def short_desc(self):
        return "clean Job Directory"
    
    def syntax(self):
        return "[options] <Spidername>"
    
    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
    
    def run(self, args, opts):
        if len(args) != 1:
            raise UsageError() 
        spidername = args[0]
        if args[0] not in self.crawler_process.spider_loader.list():
            print("Spider not available: {}".format(spidername))
            return
        jobDir = os.path.join(self.settings.get('JOB_DIR_PAUSE_RESUME'), spidername)
        if FileUtils.isExist(jobDir):
            FileUtils.deletePath(jobDir)
            print("Job Directory is deleted- {}".format(jobDir))
