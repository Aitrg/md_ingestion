# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-02 02:05:11
TICKET NUMBER -AI_1435
@author: Muhil
'''
import re
import os
import scrapy
from scrapy.spiders import CSVFeedSpider
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1435.items import VaSosSpiderItem
from Data_scuff.spiders.__common import CustomSettings
from Data_scuff.spiders.__common import DataFormatterMixin,LookupDatareaderMixin
from Data_scuff.utils.utils import Utils
import os
import pandas as pd

class VaSosSpider(CSVFeedSpider,LookupDatareaderMixin, DataFormatterMixin):
    name = '1435_va_sos'
    allowed_domains = ['virginia.gov']
    start_urls = ['http://www.scc.virginia.gov/clk/dwnld.aspx']
    # headers = ['id', 'name', 'description', 'image_link']
    # delimiter = '\t'
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1435_SOS_VA_CurationReady'),
        'JIRA_ID':'AI_1435',
         'DOWNLOAD_DELAY':.2,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('va_sos'),
        'TOP_HEADER':{                        'assessind': 'AssessInd',
                         'company_name': 'Name',
                         'creation_date': 'IncorpDate',
                         'dba_name': '',
                         'duration': 'Duration',
                         'entity_id': 'EntityID',
                         'incorpstate': 'IncorpState',
                         'industrycode': 'IndustryCode',
                         'location_address_string': 'Street1+street2+city+state+zip',
                         'mergerind': 'MergerInd',
                         'mixed_name': 'RA-Name',
                         'mixed_subtype': '',
                         'permit_type': '',
                         'person_address_string': 'RA-Street1+street2+city+state+zip',
                         'person_name': 'Officer Name',
                         'person_subtype': 'Officer Title',
                         'prinoffeffdate': 'PrinOffEffDate',
                         'ra-effdate': 'RA-EffDate',
                         'ra-loc': 'RA-Loc',
                         'ra-status': 'RA-Status',
                         'status': 'Status',
                         'statusdate': 'StatusDate',
                         'stock': 'Stock',
                         'stockind': 'StockInd',
                         'totalshares': 'TotalShares',
                         'type': 'Type'},
        'FIELDS_TO_EXPORT':[                        'type',
                         'entity_id',
                         'company_name',
                         'dba_name',
                         'status',
                         'statusdate',
                         'duration',
                         'creation_date',
                         'incorpstate',
                         'industrycode',
                         'location_address_string',
                         'prinoffeffdate',
                         'mixed_name',
                         'mixed_subtype',
                         'person_address_string',
                         'ra-effdate',
                         'ra-status',
                         'ra-loc',
                         'stockind',
                         'totalshares',
                         'mergerind',
                         'assessind',
                         'stock',
                         'person_name',
                         'person_subtype',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp'],
        'NULL_HEADERS':['type', 'statusdate', 'duration', 'incorpstate', 'industrycode', 'prinoffeffdate', 'ra-effdate', 'ra-status', 'ra-loc', 'stockind', 'totalshares', 'mergerind', 'assessind', 'stock']
        }
    def parse(self,response):
        module_dir = os.path.dirname(os.path.realpath(__file__))
        path=module_dir+'/file/Officer.csv'
        
        all_data = pd.DataFrame()
        data=pd.read_csv(path,engine='python')
        print('=============================>',self.start,'lp' in self.start)
        self.officer_dic={}
        for officer,lname,fname,mname,title in zip(data['EntityID'],data['OfficerLastName'],data['OfficerFirstName'],data['OfficerMiddleName'],data['OfficerTitle']):
            # print(officer,lname,fname,mname,title)
            # print(type(officer),str(officer))
            lis=[]
            if not self.officer_dic.get(str(officer)):
                lis.append(dict({'name':' '.join([str(fname),str(mname),str(lname)]),'title':title}))
                self.officer_dic[str(officer)]=lis.copy()
                lis.clear()
            else:
                lis=self.officer_dic[str(officer)]
                lis.append(dict({'name':' '.join([str(fname),str(mname),str(lname)]),'title':title}))
                self.officer_dic[str(officer)]=lis.copy()
                lis.clear()
        module_dir = os.path.dirname(os.path.realpath(__file__))
        if 'crop' in self.start:
            module_dir = os.path.dirname(os.path.realpath(__file__))
            path=module_dir+'/file/Corp.csv'
            yield scrapy.Request(url='file:///'+path,callback=self.parse_rows,dont_filter=True,encoding='utf-8',meta={'type':'CROP CSV'})
        if 'llc' in self.start:
            module_dir = os.path.dirname(os.path.realpath(__file__))
            path=module_dir+'/file/LLC.csv'
            yield scrapy.Request(url='file:///'+path,callback=self.parse_rows,dont_filter=True,encoding='utf-8',meta={'type':'LLC CSV'})
        if 'lp' in self.start:
            module_dir = os.path.dirname(os.path.realpath(__file__))
            path=module_dir+'/file/LPLP.csv'
            yield scrapy.Request(url='file:///'+path,callback=self.parse_rows,dont_filter=True,encoding='utf-8',meta={'type':'LP CSV'})
    def parse_row(self, response, row):
        data_dic={}
        data_dic['type']= response.meta.get('type')
        # F000071
        id=''
        if len(row['EntityID'])!=7 and str(row['EntityID']).isdigit():
            id='0'+row['EntityID']
        else:
            id=row['EntityID']
        data_dic['entity_id']= id
        name_dba=self._getDBA(row.get('Name'))
        data_dic['company_name'] =name_dba[0]
        data_dic['dba_name']= name_dba[1]
        data_dic['status']=str('AAAAA'+row['Status'])
        data_dic['statusdate']=row['StatusDate']
        data_dic['duration']=str('AAAAA'+row['Duration'])
        data_dic['creation_date']=row['IncorpDate']
        data_dic['incorpstate']=row['IncorpState']
        data_dic['industrycode']=str('AAAAA'+row['IndustryCode'])
        address=self.format__address_5(row.get('Street1',''),row.get('Street2',''),row.get('City',''),row.get('State',''),row.get('Zip',''))
        data_dic['location_address_string']=address if  re.sub(r'\s+',' ',address.strip()) else 'VA'
        data_dic['prinoffeffdate']= row['PrinOffEffDate']
        data_dic['mixed_name']= row['RA-Name']
        data_dic['mixed_subtype']='Agent'
        raaddress=self.format__address_5(row.get('RA-Street1',''),row.get('RA-Street2',''),row.get('RA-City',''),row.get('RA-State',''),row.get('RA-Zip',''))
        data_dic['person_address_string']=raaddress
        data_dic['ra-effdate']= row['RA-EffDate']
        data_dic['ra-status']= str('AAAAA'+row['RA-Status'])
        data_dic['ra-loc']= str('AAAAA'+row['RA-Loc'])
        data_dic['stockind']= str('AAAAA'+row['StockInd'])
        data_dic['totalshares']= str('AAAAA'+row['TotalShares'])
        data_dic['mergerind']= str('AAAAA'+row['MergerInd'])
        assessind='AAAAA'+str(row['AssessInd'])
        # print(assessind)
        data_dic['assessind']=assessind
        # print(type(row['AssessInd']),row['AssessInd'])
        data_dic['stock']= '; '.join([d for d in filter(lambda d:d.strip(),[row.get('Stock1',''),row.get('Stock2',''),row.get('Stock3',''),row.get('Stock4',''),row.get('Stock5',''),row.get('Stock6',''),row.get('Stock7',''),row.get('Stock8','')])])
        mix_name_type=self.officer_dic.get(row['EntityID']) if self.officer_dic.get(row['EntityID']) else self.officer_dic.get(id)
        if mix_name_type:
            for mix in mix_name_type:
                il=self.save_csv(response,data_dic)
                il.add_value('person_name', mix.get('name') if mix_name_type else '')
                il.add_value('person_subtype', mix.get('title') if mix_name_type else '')
                yield il.load_item()
                
        else:
            yield self.save_csv(response,data_dic).load_item()
    def save_csv(self,response,data_dic):
        il = ItemLoader(item=VaSosSpiderItem())
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'VA_SOS')
        il.add_value('url', 'http://www.scc.virginia.gov/clk/dwnld.aspx')
        il.add_value('permit_type','business_license')
        for k in data_dic:
            il.add_value(k,data_dic[k])
        return il

  #   def parse_row(self, response, row):
  #       il = ItemLoader(item=VaSosSpiderItem())
  #       il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
		# #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
  #       il.add_value('sourceName', 'va_sos')
  #       il.add_value('url', 'http://www.scc.virginia.gov/clk/dwnld.aspx')
  #       il.add_value('type', row['Type'])
  #       il.add_value('entity_id', row['EntityID'])
  #       il.add_value('company_name', row['Name'])
  #       il.add_value('dba_name', row[''])
  #       il.add_value('status', row['Status'])
  #       il.add_value('statusdate', row['StatusDate'])
  #       il.add_value('duration', row['Duration'])
  #       il.add_value('creation_date', row['IncorpDate'])
  #       il.add_value('incorpstate', row['IncorpState'])
  #       il.add_value('industrycode', row['IndustryCode'])
  #       il.add_value('location_address_string', row['Street1+street2+city+state+zip'])
  #       il.add_value('prinoffeffdate', row['PrinOffEffDate'])
  #       il.add_value('mixed_name', row['RA-Name'])
  #       il.add_value('mixed_subtype', row[''])
  #       il.add_value('person_address_string', row['RA-Street1+street2+city+state+zip'])
  #       il.add_value('ra-effdate', row['RA-EffDate'])
  #       il.add_value('ra-status', row['RA-Status'])
  #       il.add_value('ra-loc', row['RA-Loc'])
  #       il.add_value('stockind', row['StockInd'])
  #       il.add_value('totalshares', row['TotalShares'])
  #       il.add_value('mergerind', row['MergerInd'])
  #       il.add_value('assessind', row['AssessInd'])
  #       il.add_value('stock', row['Stock'])
  #       il.add_value('person_name', row['Officer Name'])
  #       il.add_value('person_subtype', row['Officer Title'])
  #       il.add_value('permit_type', row[''])
  #       return il.load_item()