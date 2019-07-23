'''
Created on 15-Sep-2018

@author: srinivasan
'''
from dateutil.parser import parse
from scrapy import log
from scrapy.loader.processors import Compose

from Data_scuff.extensions.processors import default_output_processor


def parse_datetime(value):
    try:
        d = parse(value)
    except ValueError:
        log.msg('Unable to parse %s' % value, level=log.WARNING)
        return value
    else:
        return d.isoformat()


def parse_date(value):
    try:
        d = parse(value)
    except ValueError:
        log.msg('Unable to parse %s' % value, level=log.WARNING)
        return value
    else:
        return d.strftime("%Y-%m-%d")


default_out_parse_datetime = Compose(default_output_processor, parse_datetime)
default_out_parse_date = Compose(default_output_processor, parse_date)
