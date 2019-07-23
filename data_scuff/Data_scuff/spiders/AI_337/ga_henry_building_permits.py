# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-28 03:57:54
TICKET NUMBER -AI_337
@author: ait
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils

from Data_scuff.spiders.AI_337.items import GaHenryBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
from inline_requests import inline_requests
from scrapy.shell import inspect_response
import datetime
import datetime
import os
import re

class GaHenryBuildingPermitsSpider(CommonSpider):
    name = '337_ga_henry_building_permits'
    allowed_domains = ['sagesgov.com']
    start_urls = ['https://www.sagesgov.com/henrycounty-ga/Portal/Search.aspx']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-337_Permits_Buildings_GA_Henry_CurationReady'),
        'JIRA_ID':'AI_337',
        'COOKIES_ENABLED':True,
        'DOWNLOAD_DELAY':10,
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('ga_henry_building_permits'),
        'TOP_HEADER':{                        'contractor_address_string': 'General Contractor Address',
                         'contractor_email': '',
                         'contractor_phone': '',
                         'location_address_string': 'Address',
                         'mixed_contractor_name': 'General Contractor/company',
                         'mixed_email': 'Contacts Email',
                         'mixed_name': 'Contacts Name/company',
                         'mixed_phone': 'Contacts Phone',
                         'mixed_subtype': 'Contacts Type',
                         'parcel number': 'Parcel Number',
                         'permit_applied_date': 'SUBMITTED ON',
                         'permit_lic_desc': 'Project/Case Name',
                         'permit_lic_no': 'Project/Case #',
                         'permit_lic_status': 'Project/Case Status',
                         'permit_subtype': 'Process Type',
                         'permit_type': '',
                         'person_address_string': 'Contacts Address',
                         'project/case coordinator': 'Project/Case Coordinator',
                         'project/case coordinator phone': 'Project/Case Coordinator Phone',
                         'submitted by': 'Submitted By'},
        'FIELDS_TO_EXPORT':[                        
                         'permit_lic_no',
                         'permit_lic_desc',
                         'permit_lic_status',
                         'permit_subtype',
                         'location_address_string',
                          'submitted by',    
                          'permit_applied_date',  
                          'parcel number',              
                         'project/case coordinator',
                         'project/case coordinator phone',
                         'mixed_name',
                          'mixed_subtype',
                         'person_address_string',
                         'mixed_email', 
                         'mixed_phone',
                         'mixed_contractor_name',
                         'contractor_address_string',
                         'contractor_email',
                          'contractor_phone',      
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp',
                        
                         ],
        'NULL_HEADERS':['submitted by', 'project/case coordinator', 'parcel number', 'project/case coordinator phone']
        }

    def parse(self, response):
        self.remove_tag=lambda data:re.sub(r'\s+', ' ',data).replace('[\n\t\r"]','').strip() if data else ''
        # self.street=[i for i in range(0,10000)]
        self.street=[str(i)+"%" for i in range(0,100,)]
        self.streetnumber=self.street[int(self.start):int(self.end)]
        print(self.streetnumber)
        if self.streetnumber:       
            self.number=self.streetnumber.pop(0)
            # print("\n",self.number,"\n")
            key='6Lf2Y4MUAAAAAJhd44kibO_8-rrh1JpGS8pa81jZ'
            formdata={
                        '__EVENTTARGET': '',
                        '__EVENTARGUMENT':'', 
                        '__LASTFOCUS': '',
                        '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                        '__VIEWSTATEGENERATOR':response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first() , 
                        '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first() ,
                        'ctl00$ctl00$cphContent$cphMain$Search1$ddlClass': '1001',
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbInstanceNumber': '',
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbProjectName': '',
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$rptrDateFilter$ctl01$tfddlDateFilter$ddlTimeframe':'', 
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$rptrDateFilter$ctl02$tfddlDateFilter$ddlTimeframe':'', 
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$rptrDateFilter$ctl03$tfddlDateFilter$ddlTimeframe':'', 
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$rptrDateFilter$ctl04$tfddlDateFilter$ddlTimeframe':'', 
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$rptrDateFilter$ctl05$tfddlDateFilter$ddlTimeframe':'', 
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbStreetNumber': str(self.number),
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbStreetName': '',
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$ddlStreetType':'' ,
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbCity':'' ,
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbParcelNumber':'',
                        'g-recaptcha-response': self.getcaptchaCoder(key).resolver(response.url),
                        'ctl00$ctl00$cphContent$cphMain$ctrlCaptcha$txtCaptchaToken': self.getcaptchaCoder(key).resolver(response.url),
                        'ctl00$ctl00$cphContent$cphMain$btnSearch': 'Search'
                }
            header={
                    "Accept-Encoding": "gzip, deflate, br",
                    "Host": "www.sagesgov.com",
                    "Origin": "https://www.sagesgov.com",
                    "Referer": "https://www.sagesgov.com/henrycounty-ga/Portal/Search.aspx",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36"
            }
            yield scrapy.FormRequest.from_response(response,url='https://www.sagesgov.com/henrycounty-ga/Portal/Search.aspx',method="POST",formdata=formdata,headers=header,dont_filter=True,callback=self.parse_next,meta={'page':1})
    @inline_requests
    def parse_next(self,response):
        pageno=response.meta['page']
        now = datetime.datetime.now()
            # print(now)
        with open(os.path.dirname(os.path.realpath(__file__))+'/parse_number.txt','a') as f:
            f.write(str(self.number))
            f.write('\n')
#data scrape coding..................
        # print('\n\n\n','pagenumber: '+str(self.number))

        dic={}
        
        
        table=response.xpath("//table[@id='cphContent_cphMain_SearchResults1_gvSearchResults']//tr")[1:-1]
        if table:
            for row in table:
                i=row.xpath(".//td/a/@href").extract_first()
                if 'java' not in i:
                    link='https://www.sagesgov.com/henrycounty-ga/Portal/'+i
                    print('\n\n')
                    print("link:   ",link)
                    print('\n\n')
                    
                    dic['permit_applied_date']=self.format_date1(row.xpath('.//td[6]/text()').extract_first())
                    scrape_data=yield scrapy.Request(url=link,dont_filter=True)
                    # inspect_response(scrape_data,self)
                    dic['permit_lic_no']=scrape_data.xpath("//div[contains(text(),'Project/Case')]/following-sibling::div/text()").extract_first()
                    
                    dic['permit_lic_desc']=scrape_data.xpath("//div[contains(text(),'Project/Case Name')]/following-sibling::div/text()").extract_first()
                    
                    dic['permit_lic_status']=scrape_data.xpath("//div[contains(text(),'Status')]/following-sibling::div/text()").extract_first()
                    
                    dic['permit_subtype']=scrape_data.xpath("//div[contains(text(),'Process Type')]/following-sibling::div/text()").extract_first()
                    
                    dic['location_address']=scrape_data.xpath("//div[contains(text(),'Address')]/following-sibling::div/text()").extract_first()
                    if dic['location_address']:
                        if 'Georgia' in dic['location_address']:
                            d=dic['location_address'].strip().replace(" Georgia",'').split(" ")
                            d.insert(-1,", GA")
                            # print(d)
                            dic['location_address_string']=(' '.join(d).replace(" ,",','))
                        else:
                            dic['location_address_string']=dic['location_address']
                    else:
                        dic['location_address_string']="GA"
                    dic['submitted by']=scrape_data.xpath("//div[contains(text(),'Submitted By')]/following-sibling::div/text()").extract_first()
                    
                    dic['parcel number']=scrape_data.xpath("//div[contains(text(),'Parcel #')]/following-sibling::div/text()").extract_first()
                    coordinator=scrape_data.xpath("//div[contains(text(),'Project/Case Coordinator')]/following-sibling::div/text()").extract_first()
                    dic['project/case coordinator']=''
                    dic['project/case coordinator phone']=''
                    if coordinator:
                        p=re.findall(r'.\d+.',coordinator)
                        dic['project/case coordinator phone']=''.join(p)
                        if dic['project/case coordinator phone']:
                            dic['project/case coordinator']=coordinator.replace(dic['project/case coordinator phone'],'')
                        else:
                            dic['project/case coordinator']=coordinator
                            dic['project/case coordinator phone']=''
                    # print(dic)
                    contacts=scrape_data.xpath("//h3[contains(text(),'Contacts')]/following::div[1]//tr")[1:]
                    # inspect_response(scrape_data,self)
                    if contacts:
                        for contact in contacts:
                            if 'General Contractor'== contact.xpath(".//th/text()").extract_first():
                                dic['mixed_subtype']=''
                                dic['mixed_name']=''
                                dic['person_address_string']=''
                                dic['mixed_email']=''
                                dic['mixed_phone']=''
                                dic['mixed_contractor_name']=contact.xpath(".//td[1]/text()").extract_first()
                                dic['contractor_address_string']=self.address(contact.xpath(".//td[5]/text()").extract_first())

                                dic['contractor_email']=contact.xpath(".//td[3]/text()").extract_first()
                                dic['contractor_phone']=contact.xpath(".//td[4]/text()").extract_first()
                                yield self.save_csv(response,dic).load_item()
                            else:
                                pass
                            if 'General Contractor'!= contact.xpath(".//th/text()").extract_first():
                                dic['mixed_subtype']=contact.xpath(".//th/text()").extract_first()
                                dic['mixed_name']=contact.xpath(".//td[1]/text()").extract_first()
                                dic['person_address_string']=self.address(contact.xpath(".//td[5]/text()").extract_first())
                                dic['mixed_email']=contact.xpath(".//td[3]/text()").extract_first()
                                dic['mixed_phone']=contact.xpath(".//td[4]/text()").extract_first()
                                dic['mixed_contractor_name']=''
                                dic['contractor_address_string']=''
                                dic['contractor_email']=''
                                dic['contractor_phone']=''
                                yield self.save_csv(response,dic).load_item()
                    else:
                        yield self.save_csv(response,dic).load_item()

    #pagination.......................
            nxt_link=response.xpath("//td/span[contains(text(),'"+str(pageno)+"')]/following::td/a/@href").extract_first()
            if  nxt_link:
                print("next page link: ",nxt_link,'\n\n')
                print('next page no : ',response.meta['page']+1)
                form_args_pagn = JavaScriptUtils.getValuesFromdoPost(nxt_link)
                formdata={
                        "ctl00$ctl00$ScriptManager1"  :'ctl00$ctl00$cphContent$cphMain$UpdatePanel1|ctl00$ctl00$cphContent$cphMain$SearchResults1$gvSearchResults' ,
                        "__EVENTTARGET": form_args_pagn['__EVENTTARGET'],
                "__EVENTARGUMENT": form_args_pagn['__EVENTARGUMENT'],
                        "__VIEWSTATE" : response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                        "__VIEWSTATEGENERATOR"   : response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                        "__EVENTVALIDATION"   :response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                        # "__ASYNCPOST" : "true"  
                    }
                headers={
                        "Host": "www.sagesgov.com",
                        "Origin": "https://www.sagesgov.com",
                        "Referer": "https://www.sagesgov.com/henrycounty-ga/Portal/SearchResults.aspx",
                        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36",
                        # "X-MicrosoftAjax": "Delta=true",
                        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
                        "X-Requested-With": "XMLHttpRequest"

                }
                yield scrapy.FormRequest(url='https://www.sagesgov.com/henrycounty-ga/Portal/SearchResults.aspx',headers=headers,formdata=formdata,callback=self.parse_next,dont_filter=True,meta={'page':response.meta['page']+1})
            else:
                print("<<<<<<<<<<<no next page in link>>>>>>>>>>>")
        else:
            now = datetime.datetime.now()
            print(now)
            with open(os.path.dirname(os.path.realpath(__file__))+'/nodata.txt','a') as f:
                f.write(str(self.number))
                f.write('\n')
            print("<<<<<<<<<<<<<<<<<<<<<<<<<<<NO result in this page>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",self.number)  
        if self.streetnumber:
            self.number=self.streetnumber.pop(0)
            # print("next: ","\n",self.number)
            key='6Lf2Y4MUAAAAAJhd44kibO_8-rrh1JpGS8pa81jZ'
            formdata={
                        '__EVENTTARGET': '',
                        '__EVENTARGUMENT':'', 
                        '__LASTFOCUS': '',
                        '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                        '__VIEWSTATEGENERATOR':response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first() , 
                        '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first() ,
                        'ctl00$ctl00$cphContent$cphMain$Search1$ddlClass': '1001',
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbInstanceNumber': '',
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbProjectName': '',
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$rptrDateFilter$ctl01$tfddlDateFilter$ddlTimeframe':'', 
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$rptrDateFilter$ctl02$tfddlDateFilter$ddlTimeframe':'', 
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$rptrDateFilter$ctl03$tfddlDateFilter$ddlTimeframe':'', 
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$rptrDateFilter$ctl04$tfddlDateFilter$ddlTimeframe':'', 
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$rptrDateFilter$ctl05$tfddlDateFilter$ddlTimeframe':'', 
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbStreetNumber': str(self.number),
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbStreetName': '',
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$ddlStreetType':'' ,
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbCity':'' ,
                        'ctl00$ctl00$cphContent$cphMain$Search1$SearchOrViewFilters1$tbParcelNumber':'',
                        'g-recaptcha-response': self.getcaptchaCoder(key).resolver(response.url),
                        'ctl00$ctl00$cphContent$cphMain$ctrlCaptcha$txtCaptchaToken': self.getcaptchaCoder(key).resolver(response.url),
                        'ctl00$ctl00$cphContent$cphMain$btnSearch': 'Search'
                }
            header={
                    "Accept-Encoding": "gzip, deflate, br",
                    "Host": "www.sagesgov.com",
                    "Origin": "https://www.sagesgov.com",
                    "Referer": "https://www.sagesgov.com/henrycounty-ga/Portal/Search.aspx",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36"
            }
            yield scrapy.FormRequest.from_response(response,url='https://www.sagesgov.com/henrycounty-ga/Portal/Search.aspx',method="POST",formdata=formdata,headers=header,dont_filter=True,callback=self.parse_next,meta={'page':response.meta['page']})
        
    def save_csv(self,response,data_dic):
        il = ItemLoader(item=GaHenryBuildingPermitsSpiderItem(),response=response)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'GA_Henry_Building_Permits')
        il.add_value('url', 'https://www.sagesgov.com/henrycounty-ga/Portal/Search.aspx')
        il.add_value('permit_type', 'building_permit')
        for k in data_dic:
            il.add_value(k,(data_dic[k]))
        return il
    def address(self,address):
        if address.strip():
            d=address.strip().split(" ")
            d.insert(-2,",")
            # print(d)
            return(' '.join(d).replace(" ,",','))
        else:
            return ''
    def format_date1(self,s):
        if s:
            dat=datetime.datetime.strptime(s,'%b %d, %Y')
            dat1=datetime.datetime.strftime(dat,'%m/%d/%Y')
            return dat1
        else:
            return ''
        # # self.state['items_count'] = self.state.get('items_count', 0) + 1
        # il = ItemLoader(item=GaHenryBuildingPermitsSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        # il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        # il.add_value('sourceName', 'GA_Henry_Building_Permits')
        # il.add_value('url', 'https://www.sagesgov.com/henrycounty-ga/Portal/Search.aspx')
        # il.add_value('mixed_phone', '')
        # il.add_value('project/case coordinator', '')
        # il.add_value('permit_lic_status', '')
        # il.add_value('project/case coordinator phone', '')
        # il.add_value('parcel number', '')
        # il.add_value('permit_applied_date', '')
        # il.add_value('mixed_email', '')
        # il.add_value('contractor_email', '')
        # il.add_value('person_address_string', '')
        # il.add_value('mixed_name', '')
        # il.add_value('permit_type', 'building_permit')
        # il.add_value('location_address_string', '')
        # il.add_value('mixed_contractor_name', '')
        # il.add_value('contractor_address_string', '')
        # il.add_value('permit_lic_desc', '')
        # il.add_value('mixed_subtype', '')
        # il.add_value('contractor_phone', '')
        # il.add_value('permit_lic_no', '')
        # il.add_value('submitted by', '')
        # il.add_value('permit_subtype', '')
        # yield il.load_item()