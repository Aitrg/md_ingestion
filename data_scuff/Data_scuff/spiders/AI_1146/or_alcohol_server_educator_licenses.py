# -*- coding: utf-8 -*-
'''
Created on 2019-Jul-11 06:55:19
TICKET NUMBER -AI_1146
@author: Prazi
'''
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_1146.items import OrAlcoholServerEducatorLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import tabula
import pandas as pd
import re
import numpy as np
import scrapy
import tempfile
import os
class OrAlcoholServerEducatorLicensesSpider(CommonSpider):
    name = '1146_or_alcohol_server_educator_licenses'
    allowed_domains = ['oregon.gov']
    start_urls = ['https://www.oregon.gov/olcc/docs/service_permits_and_server_ed/in_person_class_provider_list.pdf','https://www.oregon.gov/olcc/docs/service_permits_and_server_ed/online_provider_list.pdf']
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1146_Licenses_Alcohol_Server_Educator_OR_CurationReady'),
        'JIRA_ID':'AI_1146',
        'HTTPCACHE_ENABLED':False,
		'COOKIES_ENABLED':True,
		'DOWNLOAD_DELAY':2,
		'COOKIES_DEBUG':True,
		'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('OrAlcoholServerEducatorLicensesSpider'),
        'TOP_HEADER':{'classes in/online course in': 'Classes in/Online Course in', 'company_name': 'Code', 'company_phone': 'Phone', 'company_website': 'Webmail', 'location_address_String': 'Address', 'type': 'Type','dba_name':''},
        'FIELDS_TO_EXPORT':['type', 'company_name','dba_name', 'classes in/online course in', 'location_address_String', 'company_phone', 'company_website', 'sourceName', 'url', 'ingestion_timestamp'],
        'NULL_HEADERS':['type', 'classes in/online course in']
        }
    def parse(self, response):
        meta={}
        file=self.storeGet_tempfile(response)
        def __extractPdf(self,response):
            if str(response.url)=='https://www.oregon.gov/olcc/docs/service_permits_and_server_ed/in_person_class_provider_list.pdf':
                meta['type_val']='In-person'
                meta['url']='https://www.oregon.gov/olcc/docs/service_permits_and_server_ed/in_person_class_provider_list.pdf'
                df = tabula.read_pdf(file,pages= '1',guess=False,columns=[235.109,493.431,679.875],area=[145.798,63.669,428.726,689.299],encoding='ISO-8859-1',pandas_options={'header': None})
            if str(response.url)=='https://www.oregon.gov/olcc/docs/service_permits_and_server_ed/online_provider_list.pdf':
                # pass
                meta['type_val']='Online'
                meta['url']='https://www.oregon.gov/olcc/docs/service_permits_and_server_ed/online_provider_list.pdf'
                df = tabula.read_pdf(file,pages= '1',guess=False,columns=[242.621,490.98,729.148],area=[92.908,36.083,426.644,726.468],encoding='ISO-8859-1',pandas_options={'header': None})
            asd=[df[i] for i in df.columns.values]
            result=pd.concat(asd).reset_index(drop=True)
            df = result.to_frame(name=None)
            df[1] = df.apply(lambda x:x[0] if str(x[0]).startswith('Code:') else np.nan, axis = 1)
            def fillUniqueNum(v):
                fillUniqueNum.change = False
                if str(v[0]).startswith('Code:'):
                    fillUniqueNum.change = True
                    fillUniqueNum.unique_num+=1
                return str(fillUniqueNum.unique_num)
            fillUniqueNum.change = False
            fillUniqueNum.unique_num = 1
            df[2]= df.apply(lambda v:fillUniqueNum(v), axis=1)
            df = df[[0, 1, 2]]
            df = df.groupby(2)
            for _, i in enumerate(df):
                x= pd.DataFrame(i[1]).reset_index(drop=True)
                x = x.drop(columns=2)
                if x.apply(len).values[0]>2:
                    x = x.dropna(how='all')
                    try:
                        x[0] = x.apply(lambda x:x[0] if not str(x[0]).startswith('Code:') else np.nan, axis = 1)
                        x = x.apply(lambda x: pd.Series(x.dropna().values))
                        x[2] = x[0][1:]
                        x = x.apply(lambda x: pd.Series(x.dropna().values))
                        x[3] = '^^^ '.join(x[2].tolist()[:-1])
                        x = x.drop(columns=2)
                        x= x.dropna()
                        x.columns = ['a', 'b', 'c']
                        final_df = x.to_dict('records')
                        yield final_df
                    except ValueError:
                        pass
        for col in __extractPdf(file,response):
            for row in col:
                check_val1=row['c']
                if 'Online Course in' in check_val1:
                    check_val=check_val1.split('Online Course in')
                if 'Classes in' in check_val1:
                    check_val=check_val1.split('Classes in')
                if check_val[0]:
                    meta['company_name']=row['a']+' '+check_val[0]
                else:
                    meta['company_name']=row['a']
                if meta['company_name']:
                    meta['company_name']=meta['company_name'].replace('^^^','')
                value=check_val
                value1=check_val[1]
                value1=value1.split('^^^')
                meta['class_in']=[]
                meta['phone']=[]
                for i in value1:
                    if '-' in i:
                        meta['phone'].append(i)
                    if '.org' in i or '.com' in i:
                        meta['email']=i
                    if '-' not in i and '.org' not in i and '.com' not in i:
                        meta['class_in'].append(i)
                meta['class_in']=' '.join(meta['class_in'])
                meta['phone']=';'.join(meta['phone'])
                yield self.save_to_csv(response,**meta)
    def save_to_csv(self,response,**meta):
        il = ItemLoader(item=OrAlcoholServerEducatorLicensesSpiderItem())
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'OR_Alcohol_Server_Educator_Licenses')
        il.add_value('url', meta['url'])
        il.add_value('type',meta['type_val'])
        il.add_value('company_name',self._getDBA(meta['company_name'])[0])
        il.add_value('dba_name',self._getDBA(meta['company_name'])[1])
        il.add_value('classes in/online course in',meta['class_in'])
        il.add_value('location_address_String', 'OR')
        il.add_value('company_phone',meta['phone'].replace('Phone:',''))
        il.add_value('company_website',meta['email'])
        return il.load_item()
    def storeGet_tempfile(self,response):
        outfd, temp_path = tempfile.mkstemp(prefix='', suffix='')
        with os.fdopen(outfd, 'wb') as pdf_file:
            pdf_file.write(response.body)
        return temp_path