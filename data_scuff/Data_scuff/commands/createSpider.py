'''
Created on 23-Jun-2018

@author: srinivasan
'''
from __future__ import print_function

from importlib import import_module
import os
from os.path import join, dirname, abspath, exists, splitext
import shutil
import string

import scrapy
from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError
from scrapy.utils.template import string_camelcase

from Data_scuff.extensions.template import render_templatefile
from Data_scuff.utils.utils import Utils
from Data_scuff.utils.fileutils import FileUtils


def sanitize_module_name(module_name):
    module_name = module_name.replace('-', '_').replace('.', '_')
    if module_name[0] not in string.ascii_letters:
        module_name = "a" + module_name
    return module_name


class Command(ScrapyCommand):

    requires_project = False
    default_settings = {'LOG_ENABLED': False}

    def syntax(self):
        return "[options] <JiraID>  <requirement_excel_path>"

    def short_desc(self):
        return "Create new spider using pre-defined templates"

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        parser.add_option("-l", "--list", dest="list", action="store_true",
            help="List available templates")
        parser.add_option("-e", "--edit", dest="edit", action="store_true",
            help="Edit spider after creating it")
        parser.add_option("-d", "--dump", dest="dump", metavar="TEMPLATE",
            help="Dump template to standard output")
        parser.add_option("-t", "--template", dest="template", default="basic",
            help="Uses a custom template.")
        parser.add_option("--force", dest="force", action="store_true",
            help="If the spider already exists, overwrite it with the template")
        parser.add_option("--custom", dest="custom", action="store_true",
            help="add header from the excel file and paste it in new spider")

    def run(self, args, opts):
        if opts.list:
            self._list_templates()
            return
        if opts.dump:
            template_file = self._find_template(opts.dump)
            if template_file:
                with open(template_file, "r") as f:
                    print(f.read())
            return
        if len(args) != 2:
            raise UsageError()
        jiraid, requirement_path = args[0:2]
        module = sanitize_module_name(jiraid)
        if not os.path.exists(requirement_path):
            print("File not found: {}".format(requirement_path))
            return
        if not os.path.isfile(requirement_path):
            print("{} - it is not file".format(requirement_path))
            return
        isvalid = self.__isVaildExcel(requirement_path)
        if not isvalid[0]:
            print("Exception: {}".format(isvalid[1]))
            return
        import pandas as pd
        import tldextract
        data = pd.read_excel(requirement_path, sheet_name=0, skiprows=[0])
        my_dict = data.dropna(how='all').head(1).fillna('').to_dict(orient='records')[0]
        url = my_dict['url']
        __name = my_dict['sourceName'].lower()  or my_dict['sourcename'].lower()
        name = "".join(__name.split()).replace("-", "_")
        ext = tldextract.extract(url)
        domain = ".".join([ext.domain, ext.suffix])
        if self.settings.get('BOT_NAME') == module:
            print("Cannot create a spider with the same name as your project")
            return
        try:
            spidercls = self.crawler_process.spider_loader.load(name)
        except KeyError:
            pass
        else:
            if not opts.force:
                print("Spider %r already exists in module:" % name)
                print("  %s" % spidercls.__module__)
                return
        template_file = self._find_template(opts.template)
        if template_file:
            self._genspider(module, jiraid, name, domain, requirement_path, url,
                             opts.template, template_file, opts)
            if opts.edit:
                self.exitcode = os.system('scrapy edit "%s"' % name)
    
    """
    check is it valid excel format
    """

    def __isVaildExcel(self, requirement_path):
        from xlrd import open_workbook, XLRDError
        try:
            open_workbook(requirement_path)
        except XLRDError as e:
            return (False, e)
        else:
            return (True, '')
    
    """
    get headers from requirement document
    """

    def __headers(self, requirement_path):
        return Utils.getExcelHeaders(requirement_path)

    """
    create spider
    """

    def _genspider(self, jiraid, module, name, domain, requirement_path, url,
                    template_name, template_file, opts):
        headers = self.__headers(requirement_path)
        val_header = headers.get('top_header')
        for k in ['sourceName', 'url', 'ingestion_timestamp']:
            val_header.pop(k)
        tvars = {
            'project_name': self.settings.get('BOT_NAME'),
            'ProjectName': string_camelcase(self.settings.get('BOT_NAME')),
            'module': module,
            'jiraid':jiraid,
            'name': name,
            'start_url':url,
            'username':Utils.getUsername(),
            'datetime':Utils.getCurrentDateTimeStr(),
            'domain': domain,
            'val_header':val_header,
            'ingestion_timestamp':'Utils.getingestion_timestamp()',
            'default_val':{'sourceName':name, 'url':url},
            'null_header':None,
            'feed_expo':None,
            'top_header':None,
            'classname': '%sSpider' % ''.join(s.capitalize() \
                for s in name.split('_'))
        } 
        try:        
            if self.settings.get('NEWSPIDER_MODULE'):
                spiders_module = import_module(self.settings['NEWSPIDER_MODULE'])
                spiders_dir = os.path.join(abspath(dirname(spiders_module.__file__)), jiraid)
                if os.path.exists(spiders_dir):
                    print("Spider %r jiraID already exists in module:" % jiraid)
                    return
                os.mkdir(spiders_dir)
            else:
                spiders_module = None
                spiders_dir = "."
                
            if opts.custom:
                import pprint
                pp = pprint.PrettyPrinter(indent=25, width=250)
                tvars['null_header'] = headers.get('null_header')
                tvars['feed_expo'] = pp.pformat(headers.get('feed_expo'))
                tvars['top_header'] = pp.pformat(headers.get('top_header'))
            spider_file = "%s.py" % join(spiders_dir, name)
            shutil.copyfile(template_file, spider_file)
            render_templatefile(spider_file, **tvars)
            if self.settings['CUSTOM_TEMPLATES_DIR']:
                _template_file = join(self.settings['CUSTOM_TEMPLATES_DIR'], 'items.py.tmpl')
                item_file = "%s.py" % join(spiders_dir, 'items')
                shutil.copyfile(_template_file, item_file)
                render_templatefile(item_file, **tvars)
                __init_file = "%s.py" % join(spiders_dir, '__init__')
                open(__init_file, 'a').close()
                # copy the requirement document in spider folder
                shutil.copyfile(requirement_path, join(spiders_dir, os.path.basename(requirement_path)))
                
            print("Created spider %r using template %r " % (name, \
                template_name), end=('' if spiders_module else '\n'))
            if spiders_module:
                print("in module:\n  %s.%s" % (spiders_module.__name__, module))
        except Exception as e:
            # delete the directory
            if spiders_dir:
                FileUtils.deletePath(spiders_dir)
            print(e)

    def _find_template(self, template):
        template_file = join(self.templates_dir(), '%s.tmpl' % template)
        if exists(template_file):
            return template_file
        print("Unable to find template: %s\n" % template)
        print('Use "scrapy genspider --list" to see all available templates.')

    def _list_templates(self):
        print("Available templates:")
        for filename in sorted(os.listdir(self.templates_dir())):
            if filename.endswith('.tmpl'):
                print("  %s" % splitext(filename)[0])
      
    def templates_dir(self):
        _templates_base_dir = self.settings['CUSTOM_TEMPLATES_DIR'] or \
                self.settings['TEMPLATES_DIR'] or \
            join(scrapy.__path__[0], 'templates')
        return join(_templates_base_dir, 'spiders')
