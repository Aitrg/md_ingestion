# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-16 06:03:21
TICKET NUMBER -AI_1474
@author: Rajeev
'''
import scrapy
from Data_scuff.extensions.feeds import  ExcelFeedSpider
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1474.items import AlPodiatryLicensesSpiderItem
from Data_scuff.spiders.__common import CustomSettings
from Data_scuff.utils.utils import Utils
from Data_scuff.spiders.__common import DataFormatterMixin,LookupDatareaderMixin


class AlPodiatryLicensesSpider(ExcelFeedSpider,DataFormatterMixin,LookupDatareaderMixin):
    name = '1474_al_podiatry_licenses'
    allowed_domains = ['alabama.gov']
    start_urls = ['http://www.podiatryboard.alabama.gov/licensees.aspx']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1474_Licenses_Podiatry_AL_CurationReady'),
        'JIRA_ID':'AI_1474',
        # 'JOBDIR' : CustomSettings.getJobDirectory('AlPodiatryLicensesSpider'),
        'TOP_HEADER':{'company_name': 'Practice Name','company_phone': 'Office Phone #','controlled substance license #': 'Controlled Substance License #','dba_name': '','location_address_string': 'Address','permit_lic_desc': '','permit_lic_eff_date': 'Effective Date','permit_lic_exp_date': 'Expiration Date','permit_lic_no': 'License #','permit_type': '','person_name': 'Name'},
        'FIELDS_TO_EXPORT':['permit_lic_no','controlled substance license #','person_name','permit_lic_exp_date','permit_lic_eff_date','company_name','dba_name','location_address_string','company_phone','permit_lic_desc','permit_type','url','sourceName','ingestion_timestamp',
                         ],
        'NULL_HEADERS':['controlled substance license #']
        }
    
    def parse(self, response):
        extension = response.xpath('//*[@id="form1"]/div[4]/div[1]/div[3]/div[1]/h3/a/@href').extract_first()
        if extension:
            next_page_url ='http://www.podiatryboard.alabama.gov/'+extension
            yield scrapy.Request(url = next_page_url, callback=self.parse_excel, dont_filter=True)

    def parse_row(self, response, row):
        il = ItemLoader(item=AlPodiatryLicensesSpiderItem())
		#il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'AL_Podiatry_Licenses')
        il.add_value('url', 'http://www.podiatryboard.alabama.gov/licensees.aspx')
        il.add_value('permit_type', 'podiatry_license')
        il.add_value('permit_lic_exp_date', self.format_date(row['Expiration Date']))
        il.add_value('permit_lic_no', row['License #'])
        il.add_value('dba_name',self._getDBA(row['Practice Name'])[1])
        company_name = self._getDBA(row['Practice Name'])[0]
        company_name = company_name if len(company_name)>1 else row['First Name']+' '+ row['Last Name']
        il.add_value('permit_lic_desc', 'Podiatry License for '+ company_name if len(company_name)>1 else 'Podiatry License')
        location_address = self.format__address_4(row['Address'], row['City'], row['State'], row['Zip Code'])
        il.add_value('location_address_string', location_address if len(location_address)>2 else 'AL')
        il.add_value('person_name', row['First Name']+' '+ row['Last Name'])
        il.add_value('permit_lic_eff_date', self.format_date(row['Effective Date']))
        il.add_value('controlled substance license #', row['Controlled Substance License #'])
        il.add_value('company_name', company_name) 
        il.add_value('company_phone', row['Office Phone #'])
        return il.load_item()