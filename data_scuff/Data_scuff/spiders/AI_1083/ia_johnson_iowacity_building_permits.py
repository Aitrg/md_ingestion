# -*- coding: utf-8 -*-
'''
Created on 2019-May-02 04:48:56
TICKET NUMBER -AI_1083
@author: Prazi
'''
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_1083.items import IaJohnsonIowacityBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.http import FormRequest, Request
from inline_requests import inline_requests
from scrapy.selector.unified import Selector
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils
from Data_scuff.utils.searchCriteria import SearchCriteria
import scrapy
import re
import requests
import json
import operator
import itertools
import datetime
class IaJohnsonIowacityBuildingPermitsSpider(CommonSpider):
    name = '1083_ia_johnson_iowacity_building_permits'
    allowed_domains = ['iowa-city.org']
    start_urls = ['http://www.iowa-city.org/IcgovApps/Tidemark/Search']
    main_url='http://www.iowa-city.org'
    handle_httpstatus_list= [500]
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1083_Permits_Buildings_IA_Johnson_IowaCity_CurationReady'),
        'JIRA_ID':'AI_1083',
        'DOWNLOAD_DELAY':0.5,
        'CONCURRENT_REQUESTS':1,
        'TRACKING_OPTIONAL_PARAMS':['case_number'],
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('IaJohnsonIowacityBuildingPermitsSpider'),
        'TOP_HEADER':{'case action-notes': 'Case Action-Notes','case actions-date': 'Case Actions-Date','case actions-description': 'Case Actions-Description','case actions-status': 'Case Actions-Status','inspection_date': 'Case Actions-Date.1','inspection_description': '','inspection_pass_fail': 'Case Actions-Status.1','inspection_type': '','location_address_string': 'Address','permit_lic_desc': 'Description','permit_lic_no': 'Case Number','permit_lic_status': 'Status','permit_type': '','violation_date': '','violation_type': ''},
        'FIELDS_TO_EXPORT':['permit_lic_no','permit_lic_status','location_address_string','permit_lic_desc','case actions-date','case actions-description','case actions-status','case action-notes','inspection_date','inspection_pass_fail','inspection_description','inspection_type','violation_date','violation_type','permit_type','sourceName','url','ingestion_timestamp'],
        'NULL_HEADERS':['case actions-date', 'case actions-description', 'case actions-status', 'case action-notes']
        }
    SearchCriteria = ['ELE','ABN', 'BLD', 'CSR', 'DAC', 'DEM', 'DRC', 'EXC', 'FAP', 'FLD', 'FSP', 'HPC', 'MEC', 'PLM', 'PSD', 'WTR']
    check_first = True
    def parse(self, response):
        if self.check_first:
            self.check_first = False
            self.search_element=self.SearchCriteria[int(self.start):int(self.end)]
        if len(self.search_element) > 0:
            param = self.search_element.pop(0)
            form_data={
                'SearchTerms.CaseNumber':str(param),
                'SearchTerms.CaseAddress': ''
            }
            next_url='http://www.iowa-city.org/IcgovApps/Tidemark/Search?Length=10'
            yield FormRequest(url=next_url,formdata=form_data,callback= self.parse_list, dont_filter=True)
    @inline_requests
    def parse_list(self,response):
        meta={}
        meta['case_action_date']=meta['inspection_date']=meta['inspection_type']=meta['case_action_notes']=meta['case_action_status']=meta['case_action_description']=meta['description']=meta['address']=meta['status']=meta['case_number']=meta['inspection_pass_fail']=meta['inspection_description']=meta['violation_date']=meta['violation_type']=''
        first_table=response.xpath('/html/body/div[2]/table//tr')[16185:19001]
        for i in first_table:
            meta['case_action_date']=meta['case_action_notes']=meta['case_action_status']=meta['case_action_description']=''
            meta['case_number']=i.xpath('td[1]/a/text()').extract_first()
            meta['status']=i.xpath('td[2]/text()').extract_first()
            address=i.xpath('td[3]/text()').extract_first()
            if address:
                meta['address']=address+', IowaCity, IA'
            else:
                meta['address']='IA'
            desc=self.data_clean(i.xpath('td[4]/text()').extract_first())
            if desc:
                meta['description']=desc
            else:
                meta['description']='Building Permit'
            yield self.save_to_csv(response,**meta)
            number_link=i.xpath('td[1]/a/@href').extract_first()
            next_page=self.main_url+str(number_link)
            link = yield scrapy.Request(url=next_page,dont_filter=True,meta={'optional':{'case_number':meta['case_number']}})
            status=link.status
            if status==500:
                pass
            else:
                table=link.xpath('/html/body/div[2]/table[1]//tr')[1:]
                if table:
                    for j in table:
                        meta['inspection_date']=meta['inspection_type']=meta['inspection_pass_fail']=meta['inspection_description']=meta['violation_date']=meta['violation_type']=''
                        meta['case_action_date']=j.xpath('td[1]/text()').extract_first()
                        meta['case_action_description']=self.data_clean(j.xpath('td[2]/text()').extract_first())
                        meta['case_action_status']=j.xpath('td[3]/text()').extract_first()
                        meta['case_action_notes']=j.xpath('td[4]/text()').extract()
                        meta['case_action_notes']=' '.join(meta['case_action_notes'])
                        if meta['case_action_description']:
                            if 'Inspection' in meta['case_action_description'] or 'Insp -' in meta['case_action_description'] or 'Initial inspection' in meta['case_action_description'] or 'Re-inspection' in meta['case_action_description'] or 'inspection' in meta['case_action_description']:
                                meta['inspection_date']=meta['case_action_date']
                                meta['inspection_type']='building_inspection'
                                meta['inspection_pass_fail']=meta['case_action_status']
                                meta['inspection_description']=meta['case_action_notes']
                                meta['violation_date']=meta['violation_type']=''
                                meta['case_action_status']=meta['case_action_notes']=''
                            if 'VIOLATION' in meta['case_action_description'] or 'violation' in meta['case_action_description']:
                                meta['violation_date']=meta['case_action_date']
                                meta['violation_type']='building_violation'
                            yield self.save_to_csv(response,**meta)
                else:
                    yield self.save_to_csv(response,**meta)
        if len(self.search_element) > 0:
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)
    def save_to_csv(self,response,**meta):
        il = ItemLoader(item=IaJohnsonIowacityBuildingPermitsSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'IA_Johnson_IowaCity_Building_Permits')
        il.add_value('url', 'http://www.iowa-city.org/IcgovApps/Tidemark/Search')
        il.add_value('permit_lic_no',meta['case_number'])
        il.add_value('permit_lic_status',meta['status'])
        il.add_value('location_address_string',meta['address'])
        il.add_value('permit_lic_desc',meta['description'])
        il.add_value('case actions-date',meta['case_action_date'])
        il.add_value('case actions-description',meta['case_action_description'])
        il.add_value('case actions-status',meta['case_action_status'])
        il.add_value('case action-notes',meta['case_action_notes'])
        il.add_value('inspection_date',meta['inspection_date'])
        il.add_value('inspection_type',meta['inspection_type'])
        il.add_value('inspection_pass_fail',meta['inspection_pass_fail'])
        il.add_value('inspection_description',meta['inspection_description'])
        il.add_value('violation_date',meta['violation_date'])
        il.add_value('violation_type',meta['violation_type'])
        il.add_value('permit_type', 'building_permit')
        return il.load_item()    
    def data_clean(self, value):
        if value:
            try:
                clean_tags = re.compile('<.*?>')
                desc_list = re.sub('\s+', ' ', re.sub(clean_tags, '', value))
                desc_list_rep = desc_list.replace('&amp;', '&')
                return desc_list_rep.strip()
            except:
                return ''
        else:
            return ''
    