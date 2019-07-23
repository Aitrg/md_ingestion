# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-12 08:57:14
TICKET NUMBER -AI_1451
@author: velsystems
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1451.items import MsSosSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import datetime
import scrapy
import json
import re

class MsSosSpider(CommonSpider):
    name = 'ms_sos'
    allowed_domains = ['ms.gov']
    post_url = 'https://corp.sos.ms.gov/corpreporting/Dataset/PublicSearch'
    start_urls = ['https://corp.sos.ms.gov/corpreporting/Corp/BusinessSearch3']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1451_SOS_MS_CurationReady'),
        'JIRA_ID':'AI_1451',
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        'JOBDIR' : CustomSettings.getJobDirectory('ms_sos'),
        'TOP_HEADER':{'company_name': 'BusinessName',
                         'company_subtype': 'EntityType',
                         'county': 'County',
                         'creation_date': 'FormationDate',
                         'dba_name_': '',
                         'dba_name': 'OtherNames',
                         'domiciletype': 'DomicileType',
                         'entity_id': 'BusinessId',
                         'location_address_string': 'AddressLine+city_state+postal code',
                         'naics': 'NAICSCode1+NAICSCode2+NAICSCode3',
                         'non_profit_indicator': '',
                         'permit_type': '',
                         'status': 'Status'},
        'FIELDS_TO_EXPORT':['entity_id', 'company_name', 'dba_name_', 'dba_name', 'company_subtype', 'non_profit_indicator', 'domiciletype', 'status', 'naics', 'location_address_string', 'county', 'creation_date', 'permit_type', 'sourceName', 'url', 'ingestion_timestamp'],
        'NULL_HEADERS':['domiciletype', 'county'],
        'REPLACE_VALUES':{
            'dba_name': [{'value':'null', 'replace': ''}],
            'status': [{'value':'Undefined', 'replace': ''}],
            'company_name': [{'value':'null', 'replace': ''}],
            'county': [{'value':'NONE', 'replace': ''}, {'value':'None', 'replace': ''}, {'value':'none', 'replace': ''}],
            'location_address_string': [{'value':'', 'replace': 'MS'}, {'value':'N A, NA, MS NULL', 'replace': 'MS'}, {'value':'Unknown Address', 'replace': 'MS'}, 
                {'value':'NONE', 'replace': 'MS'}, {'value':'None', 'replace': 'MS'}, {'value':'none', 'replace': 'MS'}, {'value':'N/A', 'replace': 'MS'}, 
                {'value':'N/a', 'replace': 'MS'}, {'value':'n/a', 'replace': 'MS'}, {'value': 'NA, NULL, MS NULL', 'replace': 'MS'},
                {'value':'Unknown Address, MS', 'replace': 'MS'}, {'value':'UNKNOWN, MS', 'replace': 'MS'}]
            }
        }
    
    page_no = 1
    page_no_list = []
    is_first_page = True

    def format_address(self, address1, city, state, zip_code):
        return ", ".join([ y.strip() for y in [address1, city,
                 " ".join([i.strip() for i in [state, zip_code] if i])] if y])

    def parse(self, response):
        form_data = {
            'sort': 'BusinessName-asc',
            'page': '1',
            'pageSize': '20',
            'group': '',
            'filter': ''
        }
        yield scrapy.FormRequest(url=self.post_url, method="POST", formdata=form_data, callback=self.parse_details, dont_filter=True)
    
    def parse_details(self, response):
        jsonresponse = json.loads(response.text.strip())
        print('----------------------------------------------------------------')
        print('---------------------------------------------------------------- Pageno: ', self.page_no)
        
        ## Calculate Total Pages
        if self.is_first_page == True:
            total_records = jsonresponse['Total']
            if total_records is not None:
                if int(total_records) > 20:
                    total_pages = int(total_records) / int(20)
                    if (total_pages).is_integer() is True:
                        total_pages = int(total_pages)
                    else:
                        total_pages = int(total_pages) + int(1)
                else:
                    total_pages = int(1)
            else:
                total_records = 0
            
            if total_pages == 1:
                self.page_no_list = []
            else:
                for page in range(2, int(total_pages) + 1):
                    self.page_no_list.append(str(page))
        
        for i in range(0, len(jsonresponse['Data'])):
            name = jsonresponse['Data'][i]['BusinessName']
            if name is not None and name.strip() != '':
                company_name = self._getDBA(name)[0]
                company_name = re.sub(r'[\(\[].*?[\)\]]', '', company_name)
                company_name = company_name.replace('-DBA', '').replace('-Dba', '').replace('-dba', '').strip()
                dba_name = self._getDBA(name)[1]
            else:
                company_name = ''
                dba_name = ''
            
            if company_name is not None and company_name.strip() != '':
                company_name = company_name.strip()
            elif dba_name is not None and dba_name.strip() != '':
                company_name = dba_name.strip()
            else:
                company_name = ''
            
            other_name = jsonresponse['Data'][i]['OtherNames']
            if other_name is not None and other_name.strip() != '':
                other_name = re.sub(r'[\(\[].*?[\)\]]', '', other_name)
                other_name = re.sub(r' DBA| dba| Dba|dba |DBA', '', other_name)
                other_name = re.sub(r'\s+', ' ', other_name.strip())
                other_name = other_name.replace(' ;', ';').replace('-DBA', '').replace('-Dba', '').replace('-dba', '').strip()
            else:
                other_name = ''

            if 'Non-Profit' in jsonresponse['Data'][i]['EntityType']:
                indicator = 'Yes'
            else:
                indicator = ''
            
            naics_code1 = jsonresponse['Data'][i]['NAICSCode1']
            naics_code2 = jsonresponse['Data'][i]['NAICSCode2']
            naics_code3 = jsonresponse['Data'][i]['NAICSCode3']
            naics_code123 = (naics_code1 if naics_code1 else '') + ('; ' if naics_code1 and naics_code2 else '') + (naics_code2 if naics_code2 else '') + ('; ' if naics_code2 and naics_code3 else '') + (naics_code3 if naics_code3 else '')

            address = jsonresponse['Data'][i]['AddressLine1']
            if address is not None and address.strip() != '' and str(address.strip()).lower() != 'null':
                address = address.replace('N/A, N/A', '').replace('N/A', '').replace('n/a', '').replace('N/a', '').replace('NONE', '').replace('None', '').replace('none', '').replace('NONE SHOWN', '').replace('NONE AT THIS TIME', '').strip()
            else:
                address = ''
            city = jsonresponse['Data'][i]['City']
            if city is not None and city.strip() != '' and str(city.strip()).lower() != 'null':
                city = city.replace('N/A', '').replace('n/a', '').replace('N/a', '').replace('none', '').strip()
            else:
                city = ''
            state = jsonresponse['Data'][i]['StateCode']
            if state is not None and state.strip() != '' and str(state.strip()).lower() != 'null':
                state = state.replace('N/A', '').replace('n/a', '').replace('N/a', '').strip()
            else:
                state = 'MS'
            zip_code = jsonresponse['Data'][i]['PostalCode']
            if zip_code is not None and zip_code.strip() != '' and str(zip_code.strip()).lower() != 'null':
                zip_code = zip_code.replace('N/A', '').replace('n/a', '').replace('N/a', '').replace('none', '').strip()
            else:
                zip_code = ''
            address = self.format_address(address, city, state, zip_code)

            if jsonresponse['Data'][i]['FormationDate'] is not None:
                TimestampUtc = jsonresponse['Data'][i]['FormationDate']
                TimestampUtc = re.split(r'\(|\)', TimestampUtc)[1]
                creation_date = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=(int(TimestampUtc)/1000))
                creation_date = self.format_date(creation_date)
            else:
                creation_date = ''

            il = ItemLoader(item=MsSosSpiderItem(),response=response)
            il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
            il.add_value('sourceName', 'MS_SOS')
            il.add_value('url', 'https://corp.sos.ms.gov/corpreporting/Corp/BusinessSearch3')
            il.add_value('entity_id', jsonresponse['Data'][i]['BusinessId'])
            il.add_value('company_name', company_name)
            il.add_value('dba_name_', dba_name)
            il.add_value('dba_name', other_name)
            il.add_value('company_subtype', jsonresponse['Data'][i]['EntityType'])
            il.add_value('non_profit_indicator', indicator)
            il.add_value('domiciletype', jsonresponse['Data'][i]['DomicileType'])
            il.add_value('status', jsonresponse['Data'][i]['Status'])
            il.add_value('naics', naics_code123)
            il.add_value('location_address_string', address)
            il.add_value('county', jsonresponse['Data'][i]['County'])
            il.add_value('creation_date', str(creation_date))
            il.add_value('permit_type', 'business_license')
            yield il.load_item()
        
        if len(self.page_no_list) > 0:
            self.is_first_page = False
            next_page = self.page_no_list.pop(0)
            self.page_no = str(next_page)
            
            form_data = {
                'sort': 'BusinessName-asc',
                'page': str(next_page),
                'pageSize': '20',
                'group': '',
                'filter': ''
            }
            yield scrapy.FormRequest(url=self.post_url, method="POST", formdata=form_data, callback=self.parse_details, dont_filter=True)