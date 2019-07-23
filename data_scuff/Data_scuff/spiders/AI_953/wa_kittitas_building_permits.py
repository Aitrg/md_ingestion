# -*- coding: utf-8 -*-
'''
Created on 2019-May-30 10:26:58
TICKET NUMBER -AI_953
@author: Prazi
'''
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_953.items import WaKittitasBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
import re
import tabula
import requests
import io
import os
import tempfile
from inline_requests import inline_requests
import numpy as np
import pandas as pd
import datetime
class WaKittitasBuildingPermitsSpider(CommonSpider):
    name = '953_wa_kittitas_building_permits'
    allowed_domains = ['kittitas.wa.us']
    start_urls = ['https://www.co.kittitas.wa.us/cds/building/reports.aspx']
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI_953_Permits_Buildings_WA_Kittitas_CurationReady'),
        'JIRA_ID':'AI_953',
        'COOKIES_ENABLED':True,
        'TRACKING_OPTIONAL_PARAMS':['url'],
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('WaKittitasBuildingPermitsSpider'),
        'TOP_HEADER':{'contractor_company': 'Contractor','contractor_dba':'','location_address_string': 'Permit Address','mail_address_string': 'Mailing Address','mixed_name': 'Name','dba_name':'','mixed_subtype': '','parcel #': 'Parcel #','permit_lic_eff_date': 'Issue Date','permit_lic_fee': 'Fees','permit_lic_no': 'Permit Number','permit_lic_value': 'Valuation','permit_subtype': 'Permit Type Name','permit_lic_desc':'','permit_type': '','report date': 'Report Date'},
        'FIELDS_TO_EXPORT':['report date','permit_lic_no','permit_subtype','location_address_string','permit_lic_value','permit_lic_fee','mixed_name','dba_name','mixed_subtype','mail_address_string','contractor_company','contractor_dba','parcel #','permit_lic_eff_date','permit_lic_desc','permit_type','sourceName','url','ingestion_timestamp'],
        'NULL_HEADERS':['report date', 'parcel #']
        }
    def parse(self, response):
        file2019=response.xpath('//*[@id="content"]/div[1]/p/a/@href').extract()
        file2018=response.xpath('//*[@id="content"]/div[2]/p/a/@href').extract()
        file2017=response.xpath('//*[@id="content"]/div[3]/p/a/@href').extract()
        file2016=response.xpath('//*[@id="content"]/div[4]/p/a/@href').extract()
        file2015=response.xpath('//*[@id="content"]/div[5]/p/a/@href').extract()
        file2014=response.xpath('//*[@id="content"]/div[6]/p/a/@href').extract()
        file2013=response.xpath('//*[@id="content"]/div[7]/p/a/@href').extract()
        file2012=response.xpath('//*[@id="content"]/div[8]/p/a/@href').extract()
        file2011=response.xpath('//*[@id="content"]/div[9]/p/a/@href').extract()
        file2010=response.xpath('//*[@id="content"]/div[10]/p/a/@href').extract()
        file2009=response.xpath('//*[@id="content"]/div[11]/p/a/@href').extract()
        file2008=response.xpath('//*[@id="content"]/div[12]/p/a/@href').extract()[:5]
        if file2019:
            file2='2019'
            for i in file2019:
                date=i.split('//')[1]
                date=date.replace('.pdf','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2018:
            file2='2018'
            for i in file2018:
                date=i.split('//')[1]
                date=date.replace('.pdf','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2017:
            file2='2017'
            for i in file2017:
                date=i.split('//')[1]
                date=date.replace('.pdf','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2016:
            file2='2016'
            for i in file2016:
                date=i.split('//')[1]
                date=date.replace('.pdf','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2015:
            file2='2015'
            for i in file2015:
                date=i.split('//')[1]
                date=date.replace('.pdf','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2014:
            file2='2014'
            for i in file2014:
                date=i.split('//')[1]
                date=date.replace('.pdf','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                print('==============date:::',date)
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2013:
            file2='2013'
            for i in file2013:
                date=i.split('//')[1]
                date=date.replace('.pdf','').replace('Permit-Report-','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2012:
            file2='2012'
            for i in file2012:
                date=i.split('//')[1]
                date=date.replace('.pdf','').replace('Permit-Report-','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2011:
            file2='2011'
            for i in file2011:
                date=i.split('//')[1]
                date=date.replace('.pdf','').replace('Permit-Report-','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2010:
            file2='2010'
            for i in file2010:
                date=i.split('//')[1]
                date=date.replace('.pdf','').replace('Permit-Report-','')
                d1=datetime.datetime.strptime(date,'%Y%m%d')
                date=datetime.datetime.strftime(d1,'%m/%d/%Y')
                link_url='https://www.co.kittitas.wa.us'+str(i)
                yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2,'date':date,'optional':{'url':link_url}})
        if file2009:
            file2='2009'
            for i in file2009:
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
    def pdf_content(self,response):
        meta={}
        file2=response.meta['file2']
        meta['permit_number']=meta['permit_type']=meta['address']=meta['valuation']=meta['fees']=meta['issue_date']=meta['owner_name']=meta['contractor']=meta['parcel_number']=meta['date']=meta['dba_name']=meta['contractor_dba']=meta['mailing']=''
        file=self.storeGet_tempfile(response)
        def getDf(file,pages):
            return tabula.read_pdf(file,pages=pages,spreadsheet=True,guess=False,encoding='ISO-8859-1',pandas_options={'header': 'infer'}).replace('\r', ' ', regex=True).dropna(how='all')
        df = getDf(file,'all')
        df = df.fillna('')
        for _, row in df.fillna('').iterrows():
            row=row.tolist()
            if str(file2)=='2019' or str(file2)=='2018' or str(file2)=='2017' or str(file2)=='2016' or str(file2)=='2015':
                if '-' in str(row[0]) and len(row)==9 or len(row)==10:
                    meta['date']=response.meta['date']
                    meta['permit_number']=str(row[0])
                    meta['permit_type']=str(row[1])
                    if str(row[1]):
                        meta['permit_lic_desc']=str(row[1])
                    else:
                        meta['permit_lic_desc']='Building Permit'
                    if str(row[2]):
                        meta['address']=str(row[2])+', WA'
                    else:
                        meta['address']='WA'
                    if str(meta['date'])=='02/01/2015':
                        meta['valuation']=str(row[5])
                        meta['fees']=str(row[6])
                        meta['issue_date']=str(row[7])
                        meta['parcel_number']=str(row[8])
                        if str(row[3]):
                            meta['owner_name']=str(row[3])
                            meta['mixed_subtype']='OWNER'
                            meta['contractor']=meta['contractor_dba']=''
                            yield self.save_to_csv(response,**meta)
                        if str(row[4]):
                            meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                            if 'OWNER' not in str(row[4]):
                                meta['contractor']=str(row[4])
                                yield self.save_to_csv(response,**meta)
                    elif str(meta['date'])=='01/01/2015':
                        meta['valuation']=str(row[4])
                        meta['fees']=str(row[5])
                        meta['issue_date']=str(row[7])
                        meta['parcel_number']=str(row[8])
                        if str(row[3]):
                            meta['owner_name']=str(row[3])
                            meta['mixed_subtype']='OWNER'
                            meta['contractor']=meta['contractor_dba']=''
                            yield self.save_to_csv(response,**meta)
                        if str(row[6]):
                            meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                            if 'OWNER' not in str(row[6]):
                                meta['contractor']=str(row[6])
                                yield self.save_to_csv(response,**meta)
                    else:
                        meta['valuation']=str(row[4])
                        meta['fees']=str(row[5])
                        meta['issue_date']=str(row[8])
                        meta['parcel_number']=str(row[7])
                        if str(row[3]):
                            meta['owner_name']=str(row[3])
                            meta['mixed_subtype']='OWNER'
                            meta['contractor']=meta['contractor_dba']=''
                            yield self.save_to_csv(response,**meta)
                        if str(row[6]):
                            meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                            if 'OWNER' not in str(row[6]):
                                meta['contractor']=str(row[6])
                                yield self.save_to_csv(response,**meta)
                elif '-' in str(row[0]) and len(row)==8:
                    meta['date']=response.meta['date']
                    meta['permit_number']=str(row[0])
                    meta['permit_type']=str(row[1])
                    if str(row[1]):
                        meta['permit_lic_desc']=str(row[1])
                    else:
                        meta['permit_lic_desc']='Building Permit'
                    if str(row[2]):
                        meta['address']=str(row[2])+', WA'
                    else:
                        meta['address']='WA'
                    meta['valuation']=str(row[3])
                    meta['fees']=str(row[4])
                    meta['issue_date']=str(row[7])
                    meta['parcel_number']=str(row[6])
                    if str(row[5]):
                        meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                        if 'OWNER' not in str(row[5]):
                            meta['contractor']=str(row[5])
                            yield self.save_to_csv(response,**meta)
                elif '-' in str(row[0]) and len(row)==7:
                    meta['date']=response.meta['date']
                    meta['permit_number']=str(row[0])
                    meta['permit_type']=str(row[1])
                    if str(row[1]):
                        meta['permit_lic_desc']=str(row[1])
                    else:
                        meta['permit_lic_desc']='Building Permit'
                    if str(row[2]):
                        meta['address']=str(row[2])+', WA'
                    else:
                        meta['address']='WA'
                    meta['valuation']=str(row[4])
                    meta['fees']=str(row[5])
                    if str(row[3]):
                        meta['owner_name']=str(row[3])
                        meta['mixed_subtype']='OWNER'
                        meta['contractor']=meta['contractor_dba']=''
                        yield self.save_to_csv(response,**meta)
                    if str(row[6]):
                        meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                        if 'OWNER' not in str(row[6]):
                            meta['contractor']=str(row[6])
                            yield self.save_to_csv(response,**meta)
            if str(file2)=='2014':
                if '-' in str(row[0]) and len(row)==9 or len(row)==10:
                    meta['date']=response.meta['date']
                    meta['permit_number']=str(row[0])
                    meta['permit_type']=str(row[1])
                    if str(row[1]):
                        meta['permit_lic_desc']=str(row[1])
                    else:
                        meta['permit_lic_desc']='Building Permit'
                    if str(meta['date'])=='12/01/2014' or str(meta['date'])=='06/01/2014' or str(meta['date'])=='05/01/2014' or str(meta['date'])=='04/01/2014' or str(meta['date'])=='03/01/2014' or str(meta['date'])=='01/01/2014':
                        if str(row[3]):
                            meta['address']=str(row[3])+', WA'
                        else:
                            meta['address']='WA'
                        if str(meta['date'])=='06/01/2014' or str(meta['date'])=='05/01/2014' or str(meta['date'])=='04/01/2014' or str(meta['date'])=='03/01/2014' or str(meta['date'])=='01/01/2014':
                            meta['valuation']=str(row[4])
                            meta['fees']=str(row[5])
                        else:
                            meta['valuation']=str(row[5])
                            meta['fees']=str(row[4])
                        meta['issue_date']=str(row[7])
                        meta['parcel_number']=str(row[8])
                        if str(row[2]):
                            meta['owner_name']=str(row[2])
                            meta['mixed_subtype']='OWNER'
                            meta['contractor']=meta['contractor_dba']=''
                            yield self.save_to_csv(response,**meta)           
                        if str(row[6]):
                            meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                            if 'OWNER' not in str(row[6]):
                                meta['contractor']=str(row[6])
                                yield self.save_to_csv(response,**meta)
                    else:
                        if str(row[2]):
                                meta['address']=str(row[2])+', WA'
                        else:
                            meta['address']='WA'
                        if str(meta['date'])=='02/01/2014':
                            meta['valuation']=str(row[5])
                            meta['fees']=str(row[6])
                            meta['issue_date']=str(row[7])
                            meta['parcel_number']=str(row[8])
                            if str(row[3]): 
                                meta['owner_name']=str(row[3])
                                meta['mixed_subtype']='OWNER'
                                meta['contractor']=meta['contractor_dba']=''
                                yield self.save_to_csv(response,**meta)           
                            if str(row[4]):
                                meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                                if 'OWNER' not in str(row[4]):
                                    meta['contractor']=str(row[4])
                                    yield self.save_to_csv(response,**meta)
                        elif str(meta['date'])=='07/01/2014':
                            meta['valuation']=str(row[4])
                            meta['fees']=str(row[5])
                            meta['issue_date']=str(row[8])
                            meta['parcel_number']=str(row[7])
                            if str(row[3]): 
                                meta['owner_name']=str(row[3])
                                meta['mixed_subtype']='OWNER'
                                meta['contractor']=meta['contractor_dba']=''
                                yield self.save_to_csv(response,**meta)           
                            if str(row[6]):
                                meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                                if 'OWNER' not in str(row[6]):
                                    meta['contractor']=str(row[6])
                                    yield self.save_to_csv(response,**meta)
                        else:
                            if len(row)==9 or len(row)==10:
                                meta['valuation']=str(row[4])
                                meta['fees']=str(row[5])
                                meta['issue_date']=str(row[7])
                                meta['parcel_number']=str(row[8])
                                if str(row[3]): 
                                    meta['owner_name']=str(row[3])
                                    meta['mixed_subtype']='OWNER'
                                    meta['contractor']=meta['contractor_dba']=''
                                    yield self.save_to_csv(response,**meta)           
                                if str(row[6]):
                                    meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                                    if 'OWNER' not in str(row[6]):
                                        meta['contractor']=str(row[6])
                                        yield self.save_to_csv(response,**meta)
                else:
                    if '-' in str(row[0]):
                        meta['date']=response.meta['date']
                        meta['permit_number']=str(row[0])
                        meta['permit_type']=str(row[1])
                        if str(row[1]):
                            meta['permit_lic_desc']=str(row[1])
                        else:
                            meta['permit_lic_desc']='Building Permit'
                        if str(row[2]):
                            meta['address']=str(row[2])+', WA'
                        else:
                            meta['address']='WA'
                        meta['valuation']=str(row[4])
                        meta['fees']=str(row[5])
                        meta['parcel_number']=str(row[7])
                        if str(row[3]): 
                            meta['owner_name']=str(row[3])
                            meta['mixed_subtype']='OWNER'
                            meta['contractor']=meta['contractor_dba']=''
                            yield self.save_to_csv(response,**meta)           
                        if str(row[6]):
                            meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                            if 'OWNER' not in str(row[6]):
                                meta['contractor']=str(row[6])
                                yield self.save_to_csv(response,**meta)
            if str(file2)=='2013' or str(file2)=='2012' or str(file2)=='2011' or str(file2)=='2010' or str(file2)=='2009':
                if '-' in str(row[0]) and len(row)==9 or len(row)==10:
                    meta['date']=response.meta['date']
                    meta['permit_number']=str(row[0])
                    meta['permit_type']=str(row[1])
                    if str(row[1]):
                        meta['permit_lic_desc']=str(row[1])
                    else:
                        meta['permit_lic_desc']='Building Permit'
                    if str(row[3]):
                        meta['address']=str(row[3])+', WA'
                    else:
                        meta['address']='WA'
                    meta['valuation']=str(row[5])
                    meta['fees']=str(row[4])
                    meta['issue_date']=str(row[7])
                    meta['parcel_number']=str(row[8])
                    if str(row[2]):
                        meta['owner_name']=str(row[2])
                        meta['mixed_subtype']='OWNER'
                        meta['contractor']=meta['contractor_dba']=''
                        yield self.save_to_csv(response,**meta)
                    if str(row[6]):
                        meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                        if 'OWNER' not in str(row[6]):
                            meta['contractor']=str(row[6])
                            yield self.save_to_csv(response,**meta)
            if str(file2)=='2008':
                if '-' in str(row[0]) and len(row)==9 or len(row)==10:
                    meta['date']=response.meta['date']
                    if str(meta['date'])=='12/01/2008' or str(meta['date'])=='09/01/2008':
                        meta['permit_number']=str(row[0])
                        meta['permit_type']=str(row[1])
                        if str(row[1]):
                            meta['permit_lic_desc']=str(row[1])
                        else:
                            meta['permit_lic_desc']='Building Permit'
                        if str(row[3]):
                            meta['address']=str(row[3])+', WA'
                        else:
                            meta['address']='WA'
                        meta['valuation']=str(row[5])
                        meta['fees']=str(row[4])
                        meta['issue_date']=str(row[7])
                        meta['parcel_number']=str(row[8])
                        if str(row[2]):
                            meta['owner_name']=str(row[2])
                            meta['mixed_subtype']='OWNER'
                            meta['contractor']=meta['contractor_dba']=''
                            yield self.save_to_csv(response,**meta)
                        if str(row[6]):
                            meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                            if 'OWNER' not in str(row[6]):
                                meta['contractor']=str(row[6])
                                yield self.save_to_csv(response,**meta)
                    elif str(meta['date'])=='11/01/2008' or str(meta['date'])=='10/01/2008':
                        meta['permit_number']=str(row[0])
                        meta['permit_type']=str(row[4])
                        if str(row[4]):
                            meta['permit_lic_desc']=str(row[4])
                        else:
                            meta['permit_lic_desc']='Building Permit'
                        if str(row[1]):
                            meta['address']=str(row[1])+', WA'
                        else:
                            meta['address']='WA'
                        meta['valuation']=str(row[6])
                        meta['fees']=str(row[5])
                        meta['issue_date']=str(row[3])
                        meta['parcel_number']=str(row[8])
                        if str(row[2]):
                            meta['owner_name']=str(row[2])
                            meta['mixed_subtype']='OWNER'
                            meta['contractor']=meta['contractor_dba']=''
                            yield self.save_to_csv(response,**meta)
                        if str(row[7]):
                            meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                            if 'OWNER' not in str(row[7]):
                                meta['contractor']=str(row[7])
                                yield self.save_to_csv(response,**meta)
                    elif str(meta['date'])=='08/01/2008':
                        meta['permit_number']=str(row[0])
                        meta['permit_type']=str(row[1])
                        if str(row[1]):
                            meta['permit_lic_desc']=str(row[1])
                        else:
                            meta['permit_lic_desc']='Building Permit'
                        if str(row[3]):
                            meta['address']=str(row[3])+', WA'
                        else:
                            meta['address']='WA'
                        meta['valuation']=str(row[6])
                        meta['fees']=str(row[5])
                        meta['issue_date']=str(row[4])
                        meta['parcel_number']=str(row[8])
                        if str(row[2]):
                            meta['owner_name']=str(row[2])
                            meta['mixed_subtype']='OWNER'
                            meta['contractor']=meta['contractor_dba']=''
                            yield self.save_to_csv(response,**meta)
                        if str(row[7]):
                            meta['owner_name']=meta['mixed_subtype']=meta['dba_name']=''
                            if 'OWNER' not in str(row[7]):
                                meta['contractor']=str(row[7])
                                yield self.save_to_csv(response,**meta)
        os.remove(file)
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
        if str(meta['address'])==', WA':
            meta['address']='WA'
        il.add_value('location_address_string',meta['address'].replace('UNKNOWN','').replace('UNKNOWN,','').replace('UNKNOWN ,',''))
        if meta['valuation']:
            meta['valuation']=meta['valuation'].replace('$0.00','').replace('$-','').replace('-','')
        if str(meta['valuation'])=='0':
            meta['valuation']=''
        il.add_value('permit_lic_value',meta['valuation'])
        if meta['fees']:
            meta['fees']=meta['fees'].replace('$-','').replace('-','')
        il.add_value('permit_lic_fee',meta['fees'])
        if meta['owner_name']:
            company_names=meta['owner_name']
            meta['owner_name']=self._getDBA(company_names)[0]
            meta['dba_name']=self._getDBA(company_names)[1]
        il.add_value('mixed_name',meta['owner_name'])
        il.add_value('dba_name',meta['dba_name'])
        il.add_value('mixed_subtype',meta['mixed_subtype'])
        il.add_value('mail_address_string',meta['mailing'])
        if meta['contractor']:
            company_names=meta['contractor']
            meta['contractor']=self._getDBA(company_names)[0]
            meta['contractor_dba']=self._getDBA(company_names)[1]
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