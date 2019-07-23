# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-18 05:05:46
TICKET NUMBER -AI_1390
@author: muhil
'''

from Data_scuff.extensions.feeds import  ExcelFeedSpider
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import scrapy
from Data_scuff.spiders.AI_1390.items import IlHospitalLicensesSpiderItem
from Data_scuff.spiders.__common import CustomSettings
from Data_scuff.utils.utils import Utils
from Data_scuff.spiders.__common import DataFormatterMixin,LookupDatareaderMixin


class IlHospitalLicensesSpider(ExcelFeedSpider,DataFormatterMixin,LookupDatareaderMixin):
    name = '1390_il_home_nursing_agency_licenses'
    allowed_domains = ['illinois.gov']
    start_urls = ['https://data.illinois.gov/dataset/410idph_hospital_directory/resource/9bdedb85-77f3-490a-9bbd-2f3f5f227981']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1390_Licenses_Home_Nursing_Agency_IL_CurationReady'),
        'JIRA_ID':'AI_1390',
        'DOWNLOAD_DELAY':5,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('il_hospital_licenses'),
        'TOP_HEADER':{                        'company_name': 'Hospitals/End Stage Renal Disease/Pregnancy Termination Specialty Centers',
                         'company_phone': 'Phone',
                         'company_subtype': 'Type',
                         'county': 'County',
                         'dba_name': '',
                         'location_address_string': 'Address',
                         'permit_lic_desc': '',
                         'permit_lic_exp': 'Exp. Date',
                         'permit_lic_no': 'License #/Medicare #',
                         'permit_type': ''},
        'FIELDS_TO_EXPORT':[  'company_name', 'dba_name','location_address_string','county','company_phone','permit_lic_no','company_subtype','permit_lic_exp','permit_lic_desc','permit_type','sourceName','url',     'ingestion_timestamp' ],
        'NULL_HEADERS':['county']
        }

    # Do any adaptations you need here
    #def adapt_response(self, response):
    #    return response
    
    def parse(self, response):
        url="https://data.illinois.gov/dataset/40a663cc-0de1-4b81-b2f0-e9ceb80b8f14/resource/dbb13747-c6c8-4187-afa1-725cc8cf78c2/download/siqueryinterns-2018-2019illinois.govhome-nursing-agencies-march-2019.xls"
        yield scrapy.Request(url, callback= self.parse_excel, dont_filter=True,encoding='utf-8')

    def parse_row(self, response, row):
        print(row)        
        il = ItemLoader(item=IlHospitalLicensesSpiderItem())
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('url', 'https://data.illinois.gov/dataset/410idph_hospital_directory/resource/9bdedb85-77f3-490a-9bbd-2f3f5f227981')
        il.add_value('sourceName', 'IL_Home_Nursing_Agency_Licenses')
        il.add_value('permit_type', "nursing_home_license")
        name=self._getDBA(row['Home Nursing Agencies'])
        company_name=str(name[0]).replace(' -','') if ' -' in str(name[0]) else name[0]
        address=self.format__address_4(row['Address'],row['City'],'IL',str(row['Zip']) if '.' not in str(row['Zip']) else str(row['Zip'])[:str(row['Zip']).rfind('.')])
        il.add_value('dba_name', name[1])
        il.add_value('permit_lic_no', row.get('License #',''))
        il.add_value('permit_lic_exp', self.format_date(row.get('Exp. Date','')) if row.get('Exp. Date') else '')
        il.add_value('company_name', company_name)
        il.add_value('location_address_string', address)
        il.add_value('county', row.get('County',''))
        il.add_value('permit_lic_desc',"Medical License for "+company_name if company_name else "Medical License")
        il.add_value('company_phone', row.get('Phone',''))
        il.add_value('company_subtype', row.get('Type','') if row.get('Type','') else 'Nursing Home License')
        yield il.load_item()