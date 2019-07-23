'''
Created on 15-Aug-2018

@author: srinivasan
'''
import logging
import os, shutil

from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError
import tempfile

logger = logging.getLogger(__name__)


class Command(ScrapyCommand):
    
    requires_project = False
    default_settings = {'LOG_ENABLED': False}

    def short_desc(self):
        return "Clear Cache/Storage/temp folder"
    
    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        parser.add_option("--Cache", dest="Cache", action="store_true",
            help="Clear the Cache Directory")
        parser.add_option("--Storage", dest="Storage", action="store_true",
            help="Clear the Storage Directory")
        parser.add_option("--temp", dest="temp", action="store_true",
            help="Clear temporary Directory")
        parser.add_option("--All", dest="All", action="store_true",
            help="Clear the Cache,Storage and temporary Directory")
    
    def run(self, args, opts):
        cache_dir = os.path.join(os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), os.pardir)), 'cache')
        storage_dir = os.path.join(os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), os.pardir)), 'storage')
        temp_dir = tempfile.gettempdir()
        if opts.All:
            self.__clearDirectory(cache_dir)
            self.__clearDirectory(storage_dir)
            self.__clearDirectory(temp_dir)
        elif opts.Cache:
            self.__clearDirectory(cache_dir)
        elif opts.Storage:
            self.__clearDirectory(storage_dir)
        elif opts.temp:
            self.__clearDirectory(temp_dir)
        else:
            raise UsageError()
    
    def __clearDirectory(self, folder):
        print("started to deleting the folder {}".format(folder))
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path): 
                    shutil.rmtree(file_path)
            except Exception as e:
                print(e)
