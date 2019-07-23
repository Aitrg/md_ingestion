# -*- coding: utf-8 -*-
'''
Created on 2019-May-30 10:26:58
TICKET NUMBER -AI_953
@author: Prazi
'''
import pandas as pd
import numpy as np
import re
import tempfile
import scrapy
import re
import tabula
import requests
import io
import os
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_953.items import WaKittitasBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import datetime
class WaKittitasBuildingPermitsSpider(CommonSpider):
    name = '953_wa_kittitas_building_permits_2'
    allowed_domains = ['kittitas.wa.us']
    start_urls = ['https://www.co.kittitas.wa.us/cds/building/reports.aspx']
    custom_settings = {
        'HTTPCACHE_ENABLED':False,
        'FILE_NAME':Utils.getRundateFileName('2_AI_953_Permits_Buildings_WA_Kittitas_CurationReady'),
        'JIRA_ID':'AI_953',
        'COOKIES_ENABLED':True,
        'TRACKING_OPTIONAL_PARAMS':['url'],
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        #'JOBDIR' : CustomSettings.getJobDirectory('wa_kittitas_building_permits'),
        'TOP_HEADER':{'contractor_company': 'Contractor','contractor_dba':'','location_address_string': 'Permit Address','mail_address_string': 'Mailing Address','mixed_name': 'Name','dba_name':'','mixed_subtype': '','parcel #': 'Parcel #','permit_lic_eff_date': 'Issue Date','permit_lic_fee': 'Fees','permit_lic_no': 'Permit Number','permit_lic_value': 'Valuation','permit_subtype': 'Permit Type Name','permit_lic_desc':'','permit_type': '','report date': 'Report Date'},
        'FIELDS_TO_EXPORT':['report date','permit_lic_no','permit_subtype','location_address_string','permit_lic_value','permit_lic_fee','mixed_name','dba_name','mixed_subtype','mail_address_string','contractor_company','contractor_dba','parcel #','permit_lic_eff_date','permit_lic_desc','permit_type','sourceName','url','ingestion_timestamp'],
        'NULL_HEADERS':['report date', 'parcel #']
        }
    def parse(self, response):
        file2008=response.xpath('//*[@id="content"]/div[12]/p/a/@href').extract()[5:10]
        if file2008:
            for i in file2008:
                date=i.split('//')[1]
                date=date.replace('.pdf','').replace('Permit-Report-','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'date':date,'optional':{'url':link_url}})
    def __extractData(self, response):
        def rolling_group(val):
            if pd.notnull(val): 
                rolling_group.group += 1
            return rolling_group.group
        rolling_group.group = 0
        def joinFunc(g, column):
            col = g[column]
            joiner = "/"
            s = joiner.join([str(each) for each in col if pd.notnull(each)])
            s = re.sub("(?<=-)" + joiner, " ", s)
            s = re.sub(joiner * 2, joiner, s)
            return s
        file=self.storeGet_tempfile(response)
        def getDF(file,area,columns,pages):
            return tabula.read_pdf(file,pages=pages,silent = True,guess=False,area=area,columns=columns,encoding='ISO-8859-1',pandas_options={'header': None, 'error_bad_lines': False, 'warn_bad_lines': False}).replace('\r', ' ', regex=True).dropna(how='all')  
        df = getDF(file,[11.093,11.858,750.848,600.143],[114.368,310.973,461.678,600.143],'all')
        df.columns=['a','b','c','d']
        groups = df.groupby(df['d'].apply(rolling_group), as_index=False)
        groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
        final_df = groups.apply(groupFunct).fillna('')
        return final_df.to_dict('records')
    def pdf_content(self, response):
        meta={}
        meta['permit_number']=meta['permit_type']=meta['address']=meta['valuation']=meta['fees']=meta['issue_date']=meta['owner_name']=meta['contractor']=meta['parcel_number']=meta['date']=meta['dba_name']=meta['contractor_dba']=meta['mailing']=''
        for col in self.__extractData(response):
            meta['date']=response.meta['date']
            A = col['a']
            B = col['b']
            C = col['c']
            D = col['d']
            if A:
                A=A.replace('Permit Number:/','@')
                A=A.split('/')
            if B:
                B=B.replace('/APN:','@')
                B=B.split('/')
            if C:
                C=C.replace('Address:/','@').replace('Issued/','').replace('Iss/','')
                C=C.split('/@')
            meta['mixed_subtype']='Owner'
            meta['permit_lic_desc']='Building Permit'
            if D and '2008' in D:
                meta['issue_date'] = D.replace('Issued:','')
            try:
                if '-' in A[0] and len(A)==1:
                    meta['permit_number']=A[0].replace('@','')
                    if len(B)==2 and 'Owner:' in B[1]:
                        B=''.join(B[1])
                        B=B.split('@')
                        meta['parcel_number'] = B[1]
                        meta['owner_name'] = B[0].replace('Owner:','')
                        meta['address']=C[1].replace('@','').replace('/',', ').replace(' WA ',', WA').replace(' SR ',', SR ')
                        yield self.save_to_csv(response,**meta)
                    else:
                        B=''.join(B)
                        B=B.split('@')
                        meta['parcel_number'] = B[1]
                        meta['owner_name'] = B[0].replace('Owner:','')
                        if len(C)==2:
                            meta['address']=C[1].replace('@','').replace('/',', ').replace(' WA ',', WA').replace(' SR ',', SR ')
                        else:
                            meta['address']=C[0].replace('@','').replace('/',', ').replace(' WA ',', WA').replace(' SR ',', SR ')
                        yield self.save_to_csv(response,**meta)
                else:
                    if '-' in A[0]:
                        for (i,j,k) in zip(A,B,C):
                            if '@' in j:
                                meta['permit_number']=i.replace('@','')
                                B=''.join(j)
                                B=B.split('@')
                                meta['parcel_number'] = B[1]
                                meta['owner_name'] = B[0].replace('Owner:','')
                                meta['address']=k.replace('@','').replace('/',', ').replace(' WA ',', WA').replace(' SR ',', SR')
                                yield self.save_to_csv(response,**meta)
            except:
                pass
    def save_to_csv(self,response,**meta):
        il = ItemLoader(item=WaKittitasBuildingPermitsSpiderItem())
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'WA_Kittitas_Building_Permits')
        il.add_value('url', 'https://www.co.kittitas.wa.us/cds/building/reports.aspx')
        il.add_value('report date',meta['date'])
        il.add_value('permit_lic_no',meta['permit_number'])
        il.add_value('permit_subtype',meta['permit_type'])
        il.add_value('permit_lic_desc',meta['permit_lic_desc'])
        add=meta['address'].split(',')
        if len(add)>3:
            meta['address']=re.split("WA \d+",meta['address'])[0]+re.search("WA \d+",meta['address']).group()
        else:
            if 'PERMIT' in meta['address']:
                meta['address']='WA'
            else:
                meta['address']=meta['address']
        if ':,' in meta['address'] or ':AL,' in meta['address']:
            meta['address']=meta['address'].replace(':AL,',':,')
            meta['address']=meta['address'].split(':,')[1]
        if ',' not in meta['address']:
            meta['address']=meta['address']+', WA'
        il.add_value('location_address_string',meta['address'].replace('Address:','WA').replace('WA, WA','WA'))
        il.add_value('permit_lic_value',meta['valuation'])
        il.add_value('permit_lic_fee',meta['fees'])
        if meta['owner_name']:
            company_names=meta['owner_name']
            meta['company_name']=self._getDBA(company_names)[0]
            meta['dba_name']=self._getDBA(company_names)[1]
        il.add_value('mixed_name',meta['company_name'])
        il.add_value('dba_name',meta['dba_name'])
        il.add_value('mixed_subtype',meta['mixed_subtype'])
        il.add_value('mail_address_string',meta['mailing'])
        il.add_value('contractor_company',meta['contractor'])
        il.add_value('contractor_dba',meta['contractor_dba'])
        if 'T' in meta['parcel_number'] or 'F' in meta['parcel_number'] or 'M' in meta['parcel_number'] or 'B' in meta['parcel_number']:
            meta['parcel_number']=meta['parcel_number'].replace('B','T').replace('M','T').replace('F','T')
            il.add_value('parcel #',meta['parcel_number'].split('T')[0])
        else:
            il.add_value('parcel #',meta['parcel_number'])
        if ':' in meta['issue_date']:
            il.add_value('permit_lic_eff_date',meta['issue_date'].split(':')[1])
        else:
            il.add_value('permit_lic_eff_date',meta['issue_date'])
        il.add_value('permit_type', 'building_permit')
        return il.load_item()
    def storeGet_tempfile(self,response):
        outfd, temp_path = tempfile.mkstemp(prefix='', suffix='')
        with os.fdopen(outfd, 'wb') as pdf_file:
            pdf_file.write(response.body)
        return temp_path