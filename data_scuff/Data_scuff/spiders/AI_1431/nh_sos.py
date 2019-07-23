# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-26 08:54:20
TICKET NUMBER -AI_1431
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1431.items import NhSosSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
from inline_requests import inline_requests
import re
import json
from urllib.parse import unquote
import string
from scrapy.shell import inspect_response
from Data_scuff.utils.searchCriteria import SearchCriteria


class NhSosSpider(CommonSpider):
    name = '1431_nh_sos'
    # name='1431'
    allowed_domains = ['nh.gov']
    start_urls = ['https://quickstart.sos.nh.gov/online/BusinessInquire']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1431_SOS_NH_CurationReady'),
        'JIRA_ID':'AI_1431',
        # 'DOWNLOAD_DELAY':,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        #'JOBDIR' : CustomSettings.getJobDirectory('nh_sos'),
        'TOP_HEADER':{                        'citizenship / state of formation': 'Citizenship / State of Formation',
                         'company_email': 'Business Email',
                         'company_name': 'Business Name',
                         'company_phone': 'Phone #',
                         'company_subtype': 'Business Type',
                         'creation_date': 'Business Creation Date',
                         'dba_name': 'Trade name_ Business Name',
                         'duration': 'Duration',
                         'entity_id': 'Business ID',
                         'homestate name': 'Homestate Name',
                         'inactive_date': 'Expiration Date',
                         'location_address_string': 'Principal Office Address',
                         'mail_address_string': 'Mailing Address',
                         'mixed_name': 'Registered Agent/Principal information name',
                         'mixed_subtype': 'Registered Agent/Principal Information Contact Title',
                         'naics_description': 'NAICS Code Description',
                         'non_profit_indicator': '',
                         'permit_type': 'permit_type',
                         'person_address_string': 'Agent/Principal Information Address',
                         'previous name': 'Previous Name',
                         'status': 'Business Status'},
        'FIELDS_TO_EXPORT':[                        
                          'company_name',
                          'entity_id',
                          'homestate name',
                          'previous name',
                          'company_subtype',
                          'non_profit_indicator',
                          'location_address_string',
                          'mail_address_string',
                          'duration',
                          'inactive_date',
                          'status',
                          'creation_date',
                          'citizenship / state of formation',
                          'company_email',
                          'company_phone',
                          'dba_name',
                          'mixed_subtype',
                          'mixed_name',
                          'person_address_string',
                         'naics_description',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp'],
        'NULL_HEADERS':['citizenship / state of formation', 'previous name', 'duration', 'homestate name']
        }

    check_first = True
    # li = []
    

    def parse(self, response):

        link='https://quickstart.sos.nh.gov'
        url=response.xpath('//iframe/@src').extract_first()
        image_url=link+url

        request=response.xpath('*//input[@name="__RequestVerificationToken"]/@value').extract_first()
        if self.check_first:
          self.searchkeys=[]
            # searchkeys=[]
          if self.start.isalpha() and self.end.isalpha():
            self.searchkeys = SearchCriteria.strRange(self.start,self.end)
          else:
            self.searchkeys = SearchCriteria.numberRange(self.start,self.end,1)
          self.check_first=False
        if len(self.searchkeys) > 0 :
            search=self.searchkeys.pop(0)

            formdata={'__RequestVerificationToken': request,
                        'rbBusinessNameSearch': 'StartsWith',
                        'rbBasicSearch': 'BusinessName',
                        'txtBusinessName':str(search),
                        'chkWithSimilarSoundingBusinessNames': 'false',
                        'txtHomeStateBusiness': '',
                        'chkWithSoundingBusinessHomeStateNames': 'false',
                        'txtBusinessID': '',
                        'txtFilingNumber': '',
                        'txtBusCreationDate': '',
                        'ddlBusinessType': '',
                        'ddlBusinessStatus': '',
                        'txtCity': '',
                        'txtZipCode': '',
                        'County': '',
                        'txtAgentName':'', 
                        'chkWithSimilarSoundingAgentNames': 'false',
                        'txtPrincipalName': '',
                        'chkWithSimilarSoundingPrincipalNames': 'false',
                        'txtCaptcha': self.getcaptchaCoder().resolveImgCaptcha(image_url),
                        'btnSearch': 'Search',
                        'hdnMessage': ''}

            yield scrapy.FormRequest(url='https://quickstart.sos.nh.gov/online/BusinessInquire/BusinessSearch',callback=self.parse_next,method='POST',formdata=formdata,dont_filter=True,meta={'page':2})

    @inline_requests
    def parse_next(self,response):

        table=response.xpath('//table[@id="xhtml_grid"]//tbody/tr')
        for row in table:

          data_dic={}
          # previous_name=''
          previous_name=row.xpath('.//td[4]/text()').extract_first()
          print ('__()((()()(',previous_name)
          rm=lambda data:'' if data is None else re.sub('\s+',' ',data)
          path='https://quickstart.sos.nh.gov'
          link=row.xpath('.//td/a/@href').extract_first()
          url_link=path+link
          
          scrap_data=yield scrapy.Request(url=url_link,dont_filter=True,meta={'page':response.meta['page'],'previous':previous_name})
          meta=scrap_data.meta
          if scrap_data.status==200 or scrap_data.status==302:
            print('------------',scrap_data.meta['previous'])
            data_dic['previous_name']=scrap_data.meta['previous']
            data_dic['company_name']=scrap_data.xpath('.//td[contains(text(),"Business Name:")]//ancestor::td/following-sibling::td/span/text()').extract_first()
            data_dic['business_id']=scrap_data.xpath('.//td[contains(text(),"Business ID:")]//ancestor::td/following-sibling::td/text()').extract_first()
            data_dic['business_type']=scrap_data.xpath('.//td[contains(text(),"Business Type:")]//ancestor::td/following-sibling::td[1]/text()').extract_first()

            if 'non profit' in data_dic['business_type'] or 'not profit' in data_dic['business_type'] or 'Nonprofit' in data_dic['business_type'] or 'Notprofit' in data_dic['business_type']:
              data_dic['non_profit_indicator']='Yes'
            else:
              data_dic['non_profit_indicator']=''
            
            data_dic['hostname']=scrap_data.xpath('.//span[contains(text(),"Name in State of")]//ancestor::td/following-sibling::td/text()').extract_first()
            if data_dic['business_type']:
                if 'Foreign' in data_dic['business_type']:
                    data_dic['host_name']=data_dic['hostname']
                else:
                    data_dic['host_name']=''

            data_dic['location_address']=scrap_data.xpath('.//td[contains(text(),"Principal Office Address:")]//ancestor::td/following-sibling::td[1]/text()').extract_first()
            data_dic['location_address']=data_dic['location_address'].replace(', USA','').replace('Unknown','').replace('%','').replace('UNKNOWN','').replace('NONE','').replace('(','').replace('(','')
            data_dic['location_address']=''.join(self.split(data_dic['location_address'],',',-1)).strip()


            data_dic['mailing_address']=scrap_data.xpath('.//td[contains(text(),"Mailing Address:")]//ancestor::td/following-sibling::td[1]/text()').extract_first()
            data_dic['mailing_address']=data_dic['mailing_address'].replace(', USA','').replace('Unknown','').replace('%','').replace('UNKNOWN','').replace('NONE','')
            data_dic['mailing_address']=''.join(self.split(data_dic['mailing_address'],',',-1)).strip()

            if data_dic['location_address']:
              data_dic['location_address_string']=data_dic['location_address']
            else:
              data_dic['location_address_string']=data_dic['mailing_address']
             
            data_dic['mailing_address_string']=''
            if data_dic['mailing_address']:
                if data_dic['mailing_address']==data_dic['location_address_string']:
                    data_dic['mailing_address_string']=''
                else:
                    data_dic['mailing_address_string']=data_dic['mailing_address']

            data_dic['duration']=scrap_data.xpath('.//td[contains(text(),"Duration:")]//ancestor::td/following-sibling::td/span/text()').extract_first()
            data_dic['business_status']=scrap_data.xpath('.//td[contains(text(),"Business Status:")]//ancestor::td/following-sibling::td/span/text()').extract_first()
            data_dic['inactive_date']=rm(scrap_data.xpath('.//td[contains(text(),"Expiration Date:")]//ancestor::td/following-sibling::td[1]/span/text()').extract_first())
            data_dic['business_creation_date']=scrap_data.xpath('.//td[contains(text(),"Business Creation Date:")]//ancestor::td/following-sibling::td/text()').extract_first()
            data_dic['state_of_formation']=scrap_data.xpath('.//td[contains(text(),"Citizenship")]//ancestor::td/following-sibling::td/text()').extract_first()
            if data_dic['state_of_formation'] is None:
              data_dic['state_of_formation']=scrap_data.xpath('.//td[contains(text(),"State of Incorporation:")]//ancestor::td/following-sibling::td/text()').extract_first()

            # td[contains(text(),"Citizenship / State of Incorporation:")


            data_dic['business_mail']=scrap_data.xpath('.//td[contains(text(),"Business Email:")]//ancestor::td/following-sibling::td/span/text()').extract_first()
            data_dic['phone']=scrap_data.xpath('.//td[contains(text(),"Phone #:")]//ancestor::td/following-sibling::td/text()').extract_first()
            data_dic['dba_name']=scrap_data.xpath('.//th[contains(text(),"Trade Name Information")]//ancestor::table/tbody/tr//td/a/text()').extract_first()
            
            data_dic['naics']=scrap_data.xpath('//td[2]//input/@value').extract_first()
            if data_dic['naics']:
                # naics_description=''
                if '/' in data_dic['naics']:
                    data_dic['naics']=data_dic['naics'].split('/')
                    if len(data_dic['naics'])==2:
                      data_dic['naics_description']=data_dic['naics'][1]
                    else:
                      data_dic['naics_description']=' '.join(data_dic['naics'][1:])
                else:
                    data_dic['naics_description']=data_dic['naics']
            else:
                data_dic['naics_description']=''

            data_dic['applicant']=scrap_data.xpath('.//span[contains(text(),"Trade Name Owned By")]/following::tr[3]/td/span/text()').extract()
            data_dic['applicant']=''.join(data_dic['applicant'])
            data_dic['applicant_name']=data_dic['applicant'].strip()
            # if applicant_name:
            data_dic['applicant_title']=scrap_data.xpath('.//span[contains(text(),"Trade Name Owned By")]//following::tr[3]/td[2]/text()').extract_first()
            data_dic['applicant_address']=scrap_data.xpath('.//span[contains(text(),"Trade Name Owned By")]//following::tr[3]/td[3]/text()').extract_first()
            if data_dic['applicant_address']:
              data_dic['applicant_address']=data_dic['applicant_address'].replace(', USA','').replace('UNKNOWN','').replace('unknown','').replace('NONE','')
              data_dic['applicant_address']=''.join(self.split(data_dic['applicant_address'],',',-1)).strip()
              data_dic['applicant_address']=data_dic['applicant_address'].replace('DBA','')

            data_dic['mixed_subtype']=data_dic['applicant_title']
            # mixed_name=applicant_name
            # person_addre']ss_string=applicant_address
            data_dic['agent']=scrap_data.xpath('.//th[contains(text(),"Registered Agent Information")]/following::tr[1]/td[2]/text()').extract()
            data_dic['agent']=''.join(data_dic['agent'])
            # mixed_name=''
            # mixed_subtype=''
            data_dic['agent_name']=data_dic['agent'].strip()
            # if agent_name:

            data_dic['agent_address']=scrap_data.xpath('.//th[contains(text(),"Registered Agent Information")]/following::tr[2]/td[2]/text()').extract_first()
            if data_dic['agent_address']:
              data_dic['agent_address']=data_dic['agent_address'].replace(', USA','').replace('UNKNOWN','').replace('unknown','').replace('NONE','').replace('DBA','')

              data_dic['agent_address']=''.join(self.split(data_dic['agent_address'],',',-1)).strip()

            data_dic['mixed_subtype']='Agent'
            # person_address_string=agent_address
            data_dic['person_address_string']=''
            print('\n\n')
            print('===========',data_dic['agent_name'])
            print('--------------',data_dic['applicant_name'])
            # if agent_name or applicant_name:
            if data_dic['agent_name']:
              data_dic['mixed_name']=data_dic['agent_name']
              data_dic['person_address_string']=data_dic['agent_address'] if data_dic['agent_address'] else 'NH'
              data_dic['mixed_subtype']='Agent'
              yield self.save_to_csv(response,data_dic).load_item()
            if data_dic['applicant_name']:
              data_dic['mixed_name']=data_dic['applicant_name']
              data_dic['person_address_string']=data_dic['applicant_address'] if data_dic['applicant_address'] else 'NH'
              data_dic['mixed_subtype']=data_dic['applicant_title']
              yield self.save_to_csv(response,data_dic).load_item()
            if not data_dic['applicant_name'] and not data_dic['agent_name']:
              data_dic['mixed_name']=''
              data_dic['person_address_string']=''
              data_dic['mixed_subtype']=''
              yield self.save_to_csv(response,data_dic).load_item()
          else:
            pass
            
        page=response.xpath('//a[text()="Next >"]/@href').extract_first()
        print('\n\n\n')
        print(response.meta['page'])
        if page:
            formdata1={ 'undefined': '',
                        'sortby': 'BusinessID',
                        'stype': 'a',
                        'pidx': str(response.meta['page'])}
             
            yield scrapy.FormRequest(url='https://quickstart.sos.nh.gov/online/BusinessInquire/BusinessSearchList',method='POST',dont_filter=True,formdata=formdata1,callback=self.parse_next,meta={'page':response.meta['page']+1})
 
        if len(self.searchkeys) > 0 :

            yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)
    def split(self,strng, sep, pos):
          strng = strng.split(sep)
          return sep.join(strng[:pos]), sep.join(strng[pos:])

    def save_to_csv(self,response,data_dic):


      il = ItemLoader(item=NhSosSpiderItem(),response=response)
      il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
      il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
      il.add_value('url', 'https://quickstart.sos.nh.gov/online/BusinessInquire')
      il.add_value('sourceName', 'NH_SOS')
      il.add_value('creation_date',data_dic['business_creation_date'].replace('NOT-AVAILABLE',''))
      il.add_value('dba_name',data_dic['dba_name'] if data_dic['dba_name'] and len(data_dic['dba_name']) > 3 else self._getDBA(data_dic['company_name'])[1])
      il.add_value('non_profit_indicator',data_dic['non_profit_indicator'])
      il.add_value('mail_address_string',data_dic['mailing_address_string'])
      il.add_value('status',data_dic['business_status'])
      il.add_value('citizenship / state of formation',data_dic['state_of_formation'])
      il.add_value('duration',data_dic['duration'])
      il.add_value('mixed_name', '' if data_dic['mixed_name'] is None else data_dic['mixed_name'])
      il.add_value('company_name',self._getDBA(data_dic['company_name'])[0])
      il.add_value('company_phone',data_dic['phone'].replace('NONE',''))
      il.add_value('inactive_date',data_dic['inactive_date'])
      il.add_value('homestate name',self._getDBA(data_dic['host_name'])[0])
      il.add_value('naics_description',data_dic['naics_description'])
      il.add_value('permit_type', 'business_license')
      il.add_value('mixed_subtype', data_dic['mixed_subtype'])
      il.add_value('previous name', data_dic['previous_name'])
      il.add_value('company_subtype',self._getDBA(data_dic['business_type'])[0])
      il.add_value('entity_id', data_dic['business_id'])
      il.add_value('location_address_string', data_dic['location_address_string'] if data_dic['location_address_string'] and len(data_dic['location_address_string'])>5 else 'NH')
      il.add_value('company_email',data_dic['business_mail'].replace('NONE',''))
      il.add_value('person_address_string',data_dic['person_address_string'])
      return il
    #                                 