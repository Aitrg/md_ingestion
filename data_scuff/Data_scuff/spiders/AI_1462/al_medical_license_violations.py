# -*- coding: utf-8 -*-
'''
Created on 2019-Jul-15 06:10:20
TICKET NUMBER -AI_1462
@author: Prazi
'''
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_1462.items import AlMedicalLicenseViolationsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
import json
from inline_requests import inline_requests
import re
import requests
import datetime
import time
import os
class AlMedicalLicenseViolationsSpider(CommonSpider):
    name = '1462_al_medical_license_violations'
    allowed_domains = ['igovsolution.com']
    start_urls = ['https://abme.igovsolution.com/online/Lookups/Publiclogfile.aspx']
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1462_Licenses_Medical_Violation_AL_CurationReady'),
        'JIRA_ID':'AI_1462',
        'DOWNLOAD_DELAY':0.5,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('AlMedicalLicenseViolationsSpider'),
        'TOP_HEADER':{'dba_name': '','location_address_string': 'Address','permit_lic_desc': '','permit_lic_no': 'License #','permit_subtype': 'License Type','permit_type': '','person_name': 'Name','violation_date': 'Latest Action Date','violation_description': 'Download_link','violation_subtype': 'Latest Action Taken','violation_type': ''},
        'FIELDS_TO_EXPORT':['person_name', 'dba_name', 'permit_subtype', 'permit_lic_no', 'location_address_string', 'violation_description', 'permit_lic_desc', 'violation_type', 'violation_date', 'violation_subtype', 'permit_type', 'sourceName', 'url', 'ingestion_timestamp'],
        'NULL_HEADERS':[]
        }
    def parse(self, response):
        j=1
        while (j<=12):
            data = json.dumps({'page': j,'pageSize': 100,'sdata': [],'sortby': "",'sortexp': ""})
            yield scrapy.Request('https://abme.igovsolution.com/online/JS_grd/Grid.svc/BindPublicFileDetails',dont_filter=True,method="POST", body=data, headers = {'Content-Type': 'application/json;charset=UTF-8','Referer': 'https://abme.igovsolution.com/online/Lookups/Publiclogfile.aspx', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36','Origin': 'https://abme.igovsolution.com  ','Accept': 'application/json, text/javascript, */*; q=0.01'},callback=self.parse_main_page)
            j +=1
    def parse_main_page(self,response):
        value1 = json.loads(response.body_as_unicode())
        value2=value1['d'].replace('},{','}~~{').split('[')[1].split(']')[0]
        value3=value2.split('~~')
        for i in value3:
            json_acceptable_string = i.replace("\\", "").replace('"administrative medicine"',"'administrative medicine'")
            d = json.loads(json_acceptable_string)
            person_name=d['FullName']
            permit_subtype=d['LicenseType']
            permit_lic_no=d['License_Number']
            if d['Address1'] and d['City'] and d['Zip']:
                location_address_string=d['Address1']+', '+d['City']+' '+d['Zip']
            violation_description=d['Publicfile']
            permit_lic_desc='Medical License for '+str(person_name)
            violation_type='professional_violation'
            vio=d['Action_Date']
            if '-' in vio:
                violation_date=''
            else:
                violation_date=time.strftime('%m/%d/%Y',time.gmtime(int(re.split('\(|\)', vio)[1]) / 1000.))
            violation_subtype=d['ActionTaken']
            il = ItemLoader(item=AlMedicalLicenseViolationsSpiderItem(),response=response)
            # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
            il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
            il.add_value('sourceName', 'AL_Medical_License_Violations')
            il.add_value('url', 'https://abme.igovsolution.com/online/Lookups/Publiclogfile.aspx')
            il.add_value('person_name',self._getDBA(person_name)[0])
            il.add_value('dba_name',self._getDBA(person_name)[1])
            il.add_value('permit_subtype',permit_subtype)
            il.add_value('permit_lic_no',permit_lic_no)
            il.add_value('location_address_string',location_address_string)
            il.add_value('violation_description',violation_description)
            il.add_value('permit_lic_desc',permit_lic_desc)
            il.add_value('violation_type',violation_type)
            il.add_value('violation_date',violation_date)
            il.add_value('violation_subtype',violation_subtype)
            il.add_value('permit_type', 'medical_license')
            yield il.load_item()