# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-18 04:22:12
TICKET NUMBER -AI_1372
@author: ait-python
'''
import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1372.items import IlDupageFoodInspectionsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import json
from inline_requests import inline_requests


class IlDupageFoodInspectionsSpider(CommonSpider):
    name = '1372_il_dupage_food_inspections'
    allowed_domains = ['dupagehealth.org']
    start_urls = ['https://eco.dupagehealth.org/#/pa1/search']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1372_Inspections_Food_IL_Dupage_CurationReady'),
        'JIRA_ID':'AI_1372',
        'COOKIES_ENABLED':True,
        'DOWNLOAD_DELAY':0.5,
        # 'TRACKING_OPTIONAL_PARAMS':['company_name'],
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        'CONCURRENT REQUESTS':1,
        # 'JOBDIR' : CustomSettings.getJobDirectory('IlDupageFoodInspectionsSpider'),
        'TOP_HEADER':{   'abate_date': '',
                         'abate_status': '',
                         'company_name': 'Facility Name',
                         'dba_name': '',
                         'inspection_date': '',
                         'inspection_subtype': 'Inspection Type',
                         'inspection_type': '',
                         'location_address_string': 'Address',
                         'violation_date': '',
                         'violation_description': '',
                         'violation_rule': '',
                         'violation_type': ''},
        'FIELDS_TO_EXPORT':[
                         'company_name',
                         'dba_name',
                         'location_address_string',
                         'inspection_subtype',
                         'inspection_date',
                         'inspection_type',
                         'violation_date',
                         'violation_rule',
                         'violation_description',
                         'abate_date',
                         'abate_status',
                         'violation_type',
                         'url',
                         'sourceName',
                         'ingestion_timestamp'],

        'NULL_HEADERS':[]
        }
    def parse(self,response):
        # print('---------------------',response.text)
        form_Data={"FacilityName":'%'} 

        yield scrapy.FormRequest(url="https://eco.dupagehealth.org/api/pressAgentClient/searchFacilities?PressAgentOid=168b2416-e0e5-4907-b866-a7b400f66c46",dont_filter=True, method='POST',formdata=form_Data,callback=self.parse_dtl)
    @inline_requests
    def parse_dtl(self,response):
        meta={}
        jsonresponse = json.loads(response.body_as_unicode())
        for i in jsonresponse:
            company_name=i['FacilityName']
            Address=i['Address']
            address1=(i['CityStateZip']).replace(' IL ',', IL ')
            location_address_string=str(Address)+', '+str(address1)
            ids=i['FacilityId']
            meta={'company_name':company_name,'location_address_string':location_address_string,'inspection_subtype':'','inspection_date':'','violation_date':'','violation_rule':'','violation_description':'','abate_date':'','abate_status':'','violation_type':'','inspection_type':''}

            linkjoin='https://eco.dupagehealth.org/api/pressAgentClient/programs?FacilityId='+str(ids)+'&PressAgentOid=168b2416-e0e5-4907-b866-a7b400f66c46'
            parse_get=yield scrapy.Request(url=linkjoin,dont_filter=True)
            jsonresponse1 = json.loads(parse_get.body_as_unicode())
            for j in jsonresponse1:
                programs_id=j['ProgramId']
                insjoin='https://eco.dupagehealth.org/api/pressAgentClient/inspections?PressAgentOid=168b2416-e0e5-4907-b866-a7b400f66c46&ProgramId='+str(programs_id)
                parse_ins=yield scrapy.Request(url=insjoin,dont_filter=True)
                ins_jsonresponse = json.loads(parse_ins.body_as_unicode())
                if ins_jsonresponse and len(ins_jsonresponse)>0:
                    for k in ins_jsonresponse:
                        meta['inspection_subtype']=k['service']
                        meta['inspection_date']=self.format_date((k['activity_date']).replace('T00:00:00',''))
                        meta['inspection_type']='health_inspection'
                        violation=k['violations']
                        print('---------------',meta['inspection_subtype'],meta['inspection_date'])
                        if violation:
                            meta['abate_status']=meta['abate_date']=''
                            for m in violation:
                                meta['violation_rule']=m['violation_description']
                                meta['violation_description']=m['v_memo']
                                meta['violation_type']='health_violation'
                                meta['violation_date']=meta['inspection_date']
                                if meta['violation_description']:
                                    if ' COS' in meta['violation_description'] or 'Corrected on-site' in meta['violation_description'] or '(COS)' in meta['violation_description']:
                                        meta['abate_status']='COS'
                                        meta['abate_date']=meta['inspection_date']
                                    else:
                                        meta['abate_status']=''
                                        meta['abate_date']=''
                                   
                                yield self.save_to_csv(parse_ins,**meta)

                        else:
                            meta['violation_description']=meta['violation_rule']=meta['violation_type']=meta['violation_date']=meta['abate_status']=meta['abate_date']=''
                            yield self.save_to_csv(parse_ins,**meta)
                else:
                    meta['inspection_type']=meta['inspection_date']=meta['inspection_subtype']=meta['violation_description']=meta['violation_rule']=meta['violation_type']=meta['violation_date']=meta['abate_status']=meta['abate_date']=''
                    yield self.save_to_csv(parse_ins,**meta)


    def save_to_csv(self, response, **meta):
        il = ItemLoader(item=IlDupageFoodInspectionsSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'IL_Dupage_Food_Inspections')
        il.add_value('url', 'https://eco.dupagehealth.org/#/pa1/search')
        il.add_value('location_address_string', meta['location_address_string'] if meta['location_address_string'] else 'IL')
        il.add_value('abate_date', meta['abate_date'])
        il.add_value('inspection_date', meta['inspection_date'])
        il.add_value('company_name', self._getDBA(meta['company_name'])[0] if meta['company_name'] else '')
        il.add_value('violation_type', meta['violation_type'])
        il.add_value('violation_description', meta['violation_description'])
        il.add_value('dba_name', self._getDBA(meta['company_name'])[1] if meta['company_name'] else '')
        il.add_value('inspection_type', meta['inspection_type'])
        il.add_value('violation_date', meta['violation_date'])
        il.add_value('abate_status', meta['abate_status'])
        il.add_value('inspection_subtype', meta['inspection_subtype'])
        il.add_value('violation_rule', meta['violation_rule'])
        return il.load_item()