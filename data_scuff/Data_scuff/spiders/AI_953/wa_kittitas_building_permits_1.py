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
    name = '953_wa_kittitas_building_permits_1'
    allowed_domains = ['kittitas.wa.us']
    start_urls = ['https://www.co.kittitas.wa.us/cds/building/reports.aspx']
    custom_settings = {
        'HTTPCACHE_ENABLED':False,
        'FILE_NAME':Utils.getRundateFileName('1_AI_953_Permits_Buildings_WA_Kittitas_CurationReady'),
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
        file2008=response.xpath('//*[@id="content"]/div[12]/p/a/@href').extract()[10:]
        file2007=response.xpath('//*[@id="content"]/div[13]/p/a/@href').extract()
        file2006=response.xpath('//*[@id="content"]/div[14]/p/a/@href').extract()
        file2005=response.xpath('//*[@id="content"]/div[15]/p/a/@href').extract()
        if file2006:
            file2='2006'
            for i in file2006:
                date=i.split('//')[1]
                date=date.replace('.pdf','').replace('Permit-Report-','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2007:    
            file2='2007'
            for i in file2007:
                date=i.split('//')[1]
                date=date.replace('.pdf','').replace('Permit-Report-','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2008:
            file2='2008'
            for i in file2008:
                date=i.split('//')[1]
                date=date.replace('.pdf','').replace('Permit-Report-','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2005:
            file2='2005'
            for i in file2005:
                date=i.split('//')[1]
                date=date.replace('.pdf','').replace('Permit-Report-','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
    def __extractData(self, response):
        url=response.url
        url=url.split('//')[2]
        url=url.replace('.pdf','converted.pdf')
        date=response.meta['date']
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
        if str(date)=='03/01/2006' or str(date)=='02/01/2006' or str(date)=='01/01/2006' or str(date)=='12/01/2005' or str(date)=='09/01/2007':
            module_dir = 'file:\\' + os.path.dirname(os.path.realpath(__file__))
            file=module_dir+'/'+str(url)
        else:
            file=self.storeGet_tempfile(response)
        def getDF(file,area,columns,pages):
            return tabula.read_pdf(file,pages=pages,silent = True,guess=False,area=area,columns=columns,encoding='ISO-8859-1',pandas_options={'header': None, 'error_bad_lines': False, 'warn_bad_lines': False}).replace('\r', ' ', regex=True).dropna(how='all')
        if str(date)=='12/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[129.195,283.635,523.215,705.345,780.615],'3-12')
            df.columns=['a','b','c','d','e']
        elif str(date)=='11/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[129.195,283.635,523.215,705.345,780.615],'4-18')
            df.columns=['a','b','c','d','e']
        elif str(date)=='10/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[129.195,283.635,523.215,705.345,780.615],'4-27')
            df.columns=['a','b','c','d','e']
        elif str(date)=='09/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[129.195,283.635,523.215,705.345,780.615],'5-20')
            df.columns=['a','b','c','d','e']
        elif str(date)=='08/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[129.195,283.635,523.215,705.345,780.615],'4-19')
            df.columns=['a','b','c','d','e']
        elif str(date)=='07/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[129.195,283.635,523.215,705.345,780.615],'4-19')
            df.columns=['a','b','c','d','e']
        elif str(date)=='06/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[129.195,283.635,523.215,705.345,780.615],'4-24')
            df.columns=['a','b','c','d','e']
        elif str(date)=='05/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[129.195,283.635,523.215,705.345,780.615],'5-17')
            df.columns=['a','b','c','d','e']
        elif str(date)=='04/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[129.195,283.635,523.215,705.345,780.615],'6-15')
            df.columns=['a','b','c','d','e']
        elif str(date)=='03/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[119.52,281.76,515.04,681.12,760.8],'5-15')
            df.columns=['a','b','c','d','e']
        elif str(date)=='02/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[119.52,281.76,515.04,681.12,760.8],'4-10')
            df.columns=['a','b','c','d','e']
        elif str(date)=='01/01/2006':
            df = getDF(file,[14.355,15.345,595.485,778.635],[119.52,281.76,515.04,681.12,760.8],'3-12')
            df.columns=['a','b','c','d','e']
        elif str(date)=='12/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[98.505,252.945,489.555,669.735,778.635],'4-13')
            df.columns=['a','b','c','d','e']
        elif str(date)=='11/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[98.505,252.945,489.555,669.735,778.635],'5-14')
            df.columns=['a','b','c','d','e']
        elif str(date)=='10/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[98.505,252.945,489.555,669.735,778.635],'5-18')
            df.columns=['a','b','c','d','e']
        elif str(date)=='09/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[126.225,294.525,530.145,689.535,782.585],'5-21')
            df.columns=['a','b','c','d','e']
        elif str(date)=='08/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[130.185,294.525,534.105,710.295,783.500],'5-25')
            df.columns=['a','b','c','d','e']
        elif str(date)=='07/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[130.185,294.525,534.105,710.295,783.500],'4-16')
            df.columns=['a','b','c','d','e']
        elif str(date)=='06/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[130.185,294.525,534.105,710.295,783.500],'4-20')
            df.columns=['a','b','c','d','e']
        elif str(date)=='05/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[130.185,294.525,534.105,710.295,783.500],'3-16')
            df.columns=['a','b','c','d','e']
        elif str(date)=='04/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[130.185,294.525,534.105,710.295,783.500],'3-18')
            df.columns=['a','b','c','d','e']
        elif str(date)=='03/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[130.185,294.525,534.105,710.295,783.500],'3-18')
            df.columns=['a','b','c','d','e']
        elif str(date)=='02/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[130.185,294.525,534.105,710.295,783.500],'3-9')
            df.columns=['a','b','c','d','e']
        elif str(date)=='01/01/2007':
            df = getDF(file,[14.355,15.345,595.485,778.635],[130.185,294.525,534.105,710.295,783.500],'3-12')
            df.columns=['a','b','c','d','e']
        elif str(date)=='02/01/2008':
            df = getDF(file,[14.355,15.345,595.485,778.635],[109.505,260.945,489.555,680.735,779.635],'4-11')
            df.columns=['a','b','c','d','e']
        elif str(date)=='01/01/2008':
            df = getDF(file,[14.355,15.345,595.485,778.635],[109.505,260.945,489.555,680.735,779.635],'3-12')
            df.columns=['a','b','c','d','e']
        elif str(date)=='12/01/2005':
            df = getDF(file,[14.355,15.345,595.485,778.635],[109.505,260.945,489.555,669.735,778.635],'4-11')
            df.columns=['a','b','c','d','e']
        groups = df.groupby(df['e'].apply(rolling_group), as_index=False)
        groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
        final_df = groups.apply(groupFunct).fillna('')
        return final_df.to_dict('records')
    def pdf_content(self, response):
        meta={}
        file2=response.meta['file2']
        meta['permit_number']=meta['permit_type']=meta['address']=meta['valuation']=meta['fees']=meta['issue_date']=meta['owner_name']=meta['contractor']=meta['parcel_number']=meta['date']=meta['dba_name']=meta['contractor_dba']=meta['mailing']=''
        for col in self.__extractData(response):
            if str(file2)=='2006' or str(file2)=='2007' or str(file2)=='2008' or str(file2)=='2005':
                if '$' in col['e'] and 'Total Valuation' not in col['d']:
                    meta['date']=response.meta['date']
                    A = col['a'].split('/')
                    B = col['b'].split('/')
                    C = col['c'].split('/')
                    D = col['d'].split('/')
                    E = col['e']
                    meta['permit_number'] = A[0]
                    if meta['permit_number']:
                        meta['permit_number']=meta['permit_number'].replace('31-Aug-07','')
                    if len(A)==2:
                        meta['issue_date'] = A[1]
                    meta['address'] = C[0] + ', WA'
                    meta['address']=meta['address'] if meta['address'] else 'WA'
                    meta['parcel_number'] = C[1]
                    if len(D)==3:
                        meta['permit_type'] = D[0]+D[1]
                    if len(D)==2:
                        meta['permit_type'] = D[0]
                    meta['valuation'] = E
                    meta['permit_lic_desc']=meta['permit_type']
                    meta['permit_lic_desc']=meta['permit_lic_desc'] if meta['permit_lic_desc'] else 'Building Permit'
                    if B[0]:
                        meta['owner_name'] = B[0]
                        meta['mixed_subtype']='Owner'
                        try:
                            meta['mailing'] = B[1] + ', '+B[2] +', '+B[3]
                        except:
                            meta['mailing'] = B[1] + ', '+B[2] 
                        meta['contractor']=meta['contractor_dba']=''
                        yield self.save_to_csv(response,**meta)
                    if len(D)==2:
                        meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=meta['mailing']=''
                        if 'OWNER' not in str(D[1]):
                            meta['contractor'] = D[1]
                            yield self.save_to_csv(response,**meta)
                    if len(D)==3:
                        meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=meta['mailing']=''
                        if 'OWNER' not in str(D[2]):
                            meta['contractor'] = D[2]
                            yield self.save_to_csv(response,**meta)
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
        il.add_value('location_address_string',meta['address'])
        if meta['valuation']:
            meta['valuation']=meta['valuation'].replace('$0.00','')
        il.add_value('permit_lic_value',meta['valuation'])
        il.add_value('permit_lic_fee',meta['fees'])
        if meta['owner_name']:
            company_names=meta['owner_name']
            meta['owner_name']=self._getDBA(company_names)[0]
            meta['dba_name']=self._getDBA(company_names)[1]
        il.add_value('mixed_name',meta['owner_name'])
        il.add_value('dba_name',meta['dba_name'])
        il.add_value('mixed_subtype',meta['mixed_subtype'])
        il.add_value('mail_address_string',meta['mailing'])
        il.add_value('contractor_company',meta['contractor'])
        il.add_value('contractor_dba',meta['contractor_dba'])
        il.add_value('parcel #',meta['parcel_number'])
        il.add_value('permit_lic_eff_date',meta['issue_date'])
        il.add_value('permit_type', 'building_permit')
        return il.load_item()
    def storeGet_tempfile(self,response):
        outfd, temp_path = tempfile.mkstemp(prefix='', suffix='')
        with os.fdopen(outfd, 'wb') as pdf_file:
            pdf_file.write(response.body)
        return temp_path