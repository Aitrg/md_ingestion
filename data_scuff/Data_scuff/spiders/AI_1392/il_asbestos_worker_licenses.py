# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-17 11:26:18
TICKET NUMBER -AI_1392
@author: ait
'''

from Data_scuff.extensions.feeds import  ExcelFeedSpider
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1392.items import IlAsbestosWorkerLicensesSpiderItem
from Data_scuff.spiders.__common import CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
from Data_scuff.spiders.__common import DataFormatterMixin,LookupDatareaderMixin


class IlAsbestosWorkerLicensesSpider(ExcelFeedSpider,DataFormatterMixin,LookupDatareaderMixin):
    name = '1392_il_asbestos_worker_licenses'
    allowed_domains = ['illinois.gov']
    start_urls = ['https://data.illinois.gov/dataset/378idph_asbestos_licensed_workers/resource/f3266216-1c0e-4326-acb7-0f4341d1b463']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1392_Licenses_Asbestos_Worker_IL_CurationReady'),
        'JIRA_ID':'AI_1392',
        'COOKIES_ENABLED':True,
        'DOWNLOAD_DELAY':3,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,

        # 'JOBDIR' : CustomSettings.getJobDirectory('il_asbestos_worker_licenses'),
        'TOP_HEADER':{'county': 'County', 'dba_name': '', 'permit_lic_desc': '', 'person_phone': 'Expr2', 'permit_lic_no': 'lic_id_number', 'permit_type': '', 'person_address_string': 'Address', 'person_name': 'tech_name + Last_name'},
        'FIELDS_TO_EXPORT':['permit_lic_no','person_name', 'dba_name', 'person_address_string', 'county', 'person_phone', 'permit_lic_desc','permit_type','sourceName',  'url',  'ingestion_timestamp'],
        'NULL_HEADERS':['county']
        }
    def parse(self, response):
        urls="https://data.illinois.gov/dataset/1ba86906-adb7-40db-a893-5ca97f09942e/resource/f3266216-1c0e-4326-acb7-0f4341d1b463/download/sgroupseheh-asbesinternet-listingworker-internet-listing.xls"
        yield scrapy.Request(urls, callback= self.parse_excel, dont_filter=True,encoding='utf-8')
    def parse_row(self, response, row):
        il = ItemLoader(item=IlAsbestosWorkerLicensesSpiderItem())
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'IL_Asbestos_Worker_Licenses')
        il.add_value('url', 'https://data.illinois.gov/dataset/378idph_asbestos_licensed_workers/resource/f3266216-1c0e-4326-acb7-0f4341d1b463')
        il.add_value('person_address_string', self.format__address_4(row['Expr1'],row['tech_city'],row['tech_state'],row['tech_zip']))
        il.add_value('person_name', row['tech_name']+' '+row['LAST_NAME'])
        il.add_value('permit_lic_desc', 'Asbestos Contractor License')
        il.add_value('dba_name', '')
        il.add_value('person_phone', row['Expr2'])
        il.add_value('county', row['COUNTY'])
        il.add_value('permit_lic_no', '0'+row['lic_id_number'] if len(row['lic_id_number'])<9 else row['lic_id_number'] )
        il.add_value('permit_type', 'asbestos_contractor_license')
        yield il.load_item()