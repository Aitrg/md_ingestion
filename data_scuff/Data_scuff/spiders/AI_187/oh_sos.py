from scrapy.shell import inspect_response
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import scrapy
import json
from Data_scuff.utils.searchCriteria import SearchCriteria
from Data_scuff.spiders.AI_187.items import OhSosSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.shell import inspect_response
from inline_requests import inline_requests
import json
import time

import scrapy
from scrapy.http import HtmlResponse
import os
import re
class OhSosSpider(CommonSpider):
    name = '187_oh_sos'
    allowed_domains = ['state.oh.us']
    start_urls = ['https://businesssearch.sos.state.oh.us/#busDialog']
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-187_Companies_SOS_OH_CurationReady'),
        'JIRA_ID':'AI_187',
         'DOWNLOAD_DELAY':.2,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('oh_sos'),
        'TOP_HEADER':{                        'company_name': 'Business Name',
                         'company_subtype': 'Filing Type',
                         'creation_date': '',
                         'dba_name': '',
                         'entity_id': 'Entity',
                         'inactive_date': '',
                         'location_address_string': 'Location+County+State',
                         'mixed_name': 'Agent/Incorporator Information',
                         'mixed_subtype': 'Agent/Incorporator',
                         'permit_lic_desc': '',
                         'permit_lic_exp_date': 'Exp Date',
                         'permit_type': '',
                         'person_address_string': 'Agent/Incorporator Address',
                         'status': 'Status'},
        'FIELDS_TO_EXPORT':[ 
                         'entity_id',
                         'company_name',
                         'dba_name',
                         'company_subtype',
                         'creation_date',
                        'permit_lic_exp_date',
                         'status',
                         'location_address_string',
                         'mixed_name',
                         'mixed_subtype',
                         'person_address_string',
                         'permit_lic_desc',
                         # 'inactive_date',
                         'permit_type',
                         'url',
                         'sourceName',
                         'ingestion_timestamp',],
        'NULL_HEADERS':['status']
        }
    def parse(self, response):
        rnd = lambda: str(int(round(time.time() * 1000)))
        self.searchkeys=[]
        if self.start.isalpha() and self.end.isalpha():
            self.searchkeys = SearchCriteria.strRange(self.start,self.end)
            if 'AND' in self.searchkeys:
                self.searchkeys.remove('AND')
            if 'NOT' in  self.searchkeys:
                self.searchkeys.remove('NOT')
            if 'BTI' in  self.searchkeys:
                self.searchkeys.remove('BTI')
        else:
            self.searchkeys = SearchCriteria.numberRange(self.start,self.end,1)
        # self.searchkeys=['INDUSTRIES']
        print(len(self.searchkeys))
        search=self.searchkeys.pop(0)
        # search='154 WEST RAYEN CORP'
        url='https://businesssearchapi.sos.state.oh.us/zyjLcCmoqeZffOn1ajJdsiek3tmuj9QtZVn{}_X?_={}'.format(str(search),rnd())
        yield scrapy.Request(url=url,callback=self.parse_data, dont_filter=True,meta={'search':search})
        self.ids=[]
    @inline_requests
    def parse_data(self,response):
        # inspect_response(response,self)
        rnd = lambda: str(int(round(time.time() * 1000)))
        json_res=json.loads(response.body_as_unicode())
        if not isinstance(json_res,int):
            json_res=json.loads(response.body_as_unicode())['data']
            if json.loads(response.body_as_unicode())['data']:
                module_dir = os.path.dirname(os.path.realpath(__file__))
                path=module_dir+'/readme.txt'
                with open(path,'a') as f:
                    if json_res:
                        f.write(response.meta['search']+"|"+str(json_res[0]['result_count'])+"\n")
                    else:
                        f.write(response.meta['search']+"|"+str('no record')+"\n")
                for data  in json_res:
                    if data.get('charter_num',''):
                        self.ids.append(data.get('charter_num',''))
                if self.ids:
                    id=self.ids.pop(0)
                    mix_url='https://businesssearchapi.sos.state.oh.us/Rtj0lqmmno6vaBwbRxU7TvnJY6RmAt0bipK{}?_={}'.format(id,rnd)
                    yield scrapy.Request(url=mix_url,dont_filter=True,callback=self.main_data,meta={'search':response.meta['search']})
            else:
                if self.searchkeys:
                    search=self.searchkeys.pop(0)
                    url='https://businesssearchapi.sos.state.oh.us/zyjLcCmoqeZffOn1ajJdsiek3tmuj9QtZVn{}_X?_={}'.format(str(search),rnd())
                    yield scrapy.Request(url=url,callback=self.parse_data, dont_filter=True,meta={'search':search})
        else:
            if self.searchkeys:
                search=self.searchkeys.pop(0)
                url='https://businesssearchapi.sos.state.oh.us/zyjLcCmoqeZffOn1ajJdsiek3tmuj9QtZVn{}_X?_={}'.format(str(search),rnd())
                yield scrapy.Request(url=url,callback=self.parse_data, dont_filter=True,meta={'search':search})
    def main_data(self,response):
        print("================>",response.meta['search'])
        # inspect_response(response,self)
        rnd = lambda: str(int(round(time.time() * 1000)))
        all_data=json.loads(response.body_as_unicode())['data']
        data=all_data[4].get("firstpanel")[0]
        location_address_string=''
        if data.get('business_location_name','') and len(data.get('business_location_name',''))>2:
            location_address_string+=data.get('business_location_name','')+", "
        if data.get('county_name','') and len(data.get('county_name',''))>2:
            location_address_string+=data.get('county_name','')+", "
        if data.get('state_name','') and len(data.get('state_name',''))>2:
            location_address_string+=data.get('state_name','')
        else:
            location_address_string+='OH'
        data_dic={}
        data_dic['entity_id']=data.get('charter_num','')
        com_dba=self._getDBA(data.get('business_name',''))
        data_dic['company_name']=com_dba[0] if com_dba[0] else com_dba[1]
        data_dic['dba_name']=com_dba[1]
        data_dic['company_subtype']=data.get('business_type') if data.get('business_type') else ''
        data_dic['creation_date']=self.format_date(data.get('effect_date'))
        data_dic['permit_lic_exp_date']=self.format_date(data.get('expiry_date'))
        data_dic['status']=data.get('status','')
        data_dic['location_address_string']=location_address_string if location_address_string else 'OH'
        data_dic['permit_lic_desc']='Business License for '+data_dic['company_name'] if data_dic['company_name'] else 'Business License'
        mix_json=all_data
        c=0
        for incorp in mix_json[3]['details']:
            c+=1
            mix_dba=self._getDBA(incorp.get('business_assoc_name'))
            if mix_dba[1] and  data_dic['dba_name']:
                yield self.save_csv(response,data_dic)
                data_dic['dba_name']=''
            else :
                data_dic['dba_name']=data_dic['dba_name']=mix_dba[1] if mix_dba[1] else data_dic['dba_name']                     
                il=self.save_csv(response,data_dic)
                il.add_value('mixed_name',mix_dba[0])
                il.add_value('mixed_subtype','Incorporator ')
                il.add_value('person_address_string','OH')
                yield il.load_item()
        for registrant in mix_json[1].get('registrant'):
            c+=1
            person_address=self.format__address_4(registrant.get('contact_addr1')+str(', '+registrant.get('contact_addr2') if len(registrant.get('contact_addr2').strip('-'))>2 else ''),registrant.get('contact_city'),registrant.get('contact_state'),registrant.get('contact_zip9'))
            mix_dba=self._getDBA(registrant.get('contact_name'))
            if mix_dba[1] and  data_dic['dba_name']:
                yield self.save_csv(response,data_dic)
                data_dic['dba_name']=''
            else:
                data_dic['dba_name']=mix_dba[1] if mix_dba[1] else data_dic['dba_name']
                il=self.save_csv(response,data_dic)
                il.add_value('mixed_name',mix_dba[0])
                il.add_value('mixed_subtype','Agent ')
                il.add_value('person_address_string',person_address)
                yield il.load_item()
        if c==0:
            yield self.save_csv(response,data_dic).load_item()
        if self.ids:
                id=self.ids.pop(0)
                mix_url='https://businesssearchapi.sos.state.oh.us/Rtj0lqmmno6vaBwbRxU7TvnJY6RmAt0bipK{}?_={}'.format(id,rnd)
                yield scrapy.Request(url=mix_url,dont_filter=True,callback=self.main_data,meta={'search':response.meta['search']})
        else:
            if self.searchkeys:
                search=self.searchkeys.pop(0)
                url='https://businesssearchapi.sos.state.oh.us/zyjLcCmoqeZffOn1ajJdsiek3tmuj9QtZVn{}_X?_={}'.format(str(search),rnd())
                yield scrapy.Request(url=url,callback=self.parse_data, dont_filter=True,meta={'search':search})
    def save_csv(self,response,data_dic):
        il = ItemLoader(item=OhSosSpiderItem(),response=response)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'OH_SOS')
        il.add_value('url', 'https://www5.sos.state.oh.us/ords/f?p=100:1:::NO:1:P1_TYPE:NAME')
        il.add_value('permit_type', 'business_license')
        for k in data_dic:
            il.add_value(k,data_dic[k])
        return il
