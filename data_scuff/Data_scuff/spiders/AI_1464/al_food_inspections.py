# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-15 12:56:42
TICKET NUMBER -AI_1464
@author: ait
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import scrapy
from scrapy.http import HtmlResponse
from inline_requests import inline_requests
from scrapy.shell import inspect_response
import datetime
import re
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils
import json
import os
from Data_scuff.spiders.AI_1464.items import AlFoodInspectionsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils

class AlFoodInspectionsSpider(CommonSpider):
    name = '1464_al_food_inspections'
    allowed_domains = ['alabamapublichealth.gov']
    start_urls=['http://foodscores.state.al.us']
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1464_Inspections_Food_AL_CurationReady'),
        'JIRA_ID':'AI_1464',
        'COOKIES_ENABLED':True,
        'DOWNLOAD_DELAY':5,
        # 'CONCURRENT_REQUESTS': 1,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('al_food_inspections'),
        'TOP_HEADER':{'company_name': 'Establishment Name', 'county': 'County', 'dba_name': '', 'inspection_score': 'Score', 'inspection_date': 'Inspection date', 'inspection_type': '', 'location_address_string': 'Address', 'smoke free': 'Smoke Free'},
        'FIELDS_TO_EXPORT':['county', 'company_name','dba_name', 'location_address_string','smoke free',   'inspection_type', 'inspection_date', 'inspection_score', 'sourceName','url', 'ingestion_timestamp', ],
        'NULL_HEADERS':['county', 'smoke free']
        }

    def parse(self, response):
        self.response=response
        # res_data=response
        self.county_list=response.xpath("//select[@id='ctl00_ContentPlaceHolder1_DrpCnty']/option/@value").extract()[1:]
        self.county=response.xpath("//select[@id='ctl00_ContentPlaceHolder1_DrpCnty']/option/text()").extract()[1:]
        # self.county_list=['36','45']
        print(self.county_list)
        if self.county_list:
            self.county_data=self.county.pop(0)
            self.county_pop=self.county_list.pop(0)
            formdata={
                    "ctl00$ScriptManager1": "ctl00$UpdatePanel1|ctl00$ContentPlaceHolder1$BtnSearch",
                    "ctl00$ContentPlaceHolder1$TxtEstdNm": "",
                    "ctl00$ContentPlaceHolder1$DrpEstdType": "All",
                    "ctl00$ContentPlaceHolder1$txtCity": "",
                    "ctl00$ContentPlaceHolder1$DrpCnty": str(self.county_pop),
                    "__LASTFOCUS": "",
                    "__VIEWSTATE": response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                    "__VIEWSTATEGENERATOR": response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "__EVENTVALIDATION": response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                    "ctl00$ContentPlaceHolder1$BtnSearch.x": "60",
            }
            header={
                    "Accept": '*/*',
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Host": "foodscores.state.al.us",
                    "Origin": "http://foodscores.state.al.us",
                    "Referer": response.url,
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36",            }
            print(header)

            yield scrapy.FormRequest.from_response(response,callback=self.parse_next1,dont_filter=True,formdata=formdata,headers=header,dont_click=True,errback=self.errors,meta={"max_retry_times":3})
    def parse_next1(self,response):
        # inspect_response(response,self)
        res = HtmlResponse(response.url, body=str.encode(response.text))
        dic={}
        self.response=res
        table=res.xpath("//table[@id='ctl00_ContentPlaceHolder1_DtList']//tr")
        for i,row in enumerate(table):
            v=(str(i).zfill(2))
            company_name=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_DtList_ctl"+str(v)+"_LblEst']/text()").extract_first()

            dic['company_name']=self.dba_format(company_name)[0]
            dic['dba_name']=self.dba_format(company_name)[1]
            city=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_DtList_ctl"+str(v)+"_Label1']/text()").extract_first()
            address=res.xpath("//a[@id='ctl00_ContentPlaceHolder1_DtList_ctl"+str(v)+"_LnkAdd']/text()").extract_first()
            zipcode=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_DtList_ctl"+str(v)+"_Label2']/text()").extract_first()
            dic['location_address_string']=''
            if address:
                dic['location_address_string']+=address+', '
            if city:
                dic['location_address_string']+=city+' '
            if zipcode:
                dic['location_address_string']+=zipcode
            dic['smoke free']=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_DtList_ctl"+str(v)+"_LblSmoke']/text()").extract_first()
            dic['inspection_score']=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_DtList_ctl"+str(v)+"_LblScore']/text()").extract_first()
            dic['inspection_date']=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_DtList_ctl"+str(v)+"_LblInDt']/text()").extract_first()
            dic['inspection_type']='health_inspection' if dic['inspection_date'] else ''
            dic['county']=self.county_data
            yield self.save_csv(response,dic).load_item()

        if self.county_list:
            self.county_data=self.county.pop(0)
            self.county_pop=self.county_list.pop(0)
            print("\n\n\n")
            print(self.county_data)
            print("\n\n\n")

            formdata={
                    "ctl00$ScriptManager1": "ctl00$UpdatePanel1|ctl00$ContentPlaceHolder1$BtnSearch",
                    "ctl00$ContentPlaceHolder1$TxtEstdNm": "",
                    "ctl00$ContentPlaceHolder1$DrpEstdType": "All",
                    "ctl00$ContentPlaceHolder1$txtCity": "",
                    "ctl00$ContentPlaceHolder1$DrpCnty": str(self.county_pop),
                    "__LASTFOCUS": "",
                    "__VIEWSTATE": response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                    "__VIEWSTATEGENERATOR": response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "__EVENTVALIDATION": response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                    "ctl00$ContentPlaceHolder1$BtnSearch.x": "60",
            }
            header={
                    "Accept": '*/*',
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Host": "foodscores.state.al.us",
                    "Origin": "http://foodscores.state.al.us",
                    "Referer": response.url,
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36",            }
            yield scrapy.FormRequest.from_response(response,callback=self.parse_next1,dont_filter=True,formdata=formdata,headers=header,dont_click=True,errback=self.errors,meta={"max_retry_times":3})


    def save_csv(self,response,data_dic):
        il = ItemLoader(item=AlFoodInspectionsSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags,lambda data:re.sub(r'\s+', ' ',data) if data else '',replace_escape_chars)         
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'AL_Food_Inspections')
        il.add_value('url', 'http://www.alabamapublichealth.gov/foodscores/index.html')       
        
        for k in data_dic:
            il.add_value(k,(data_dic[k]))
        return il

    def dba_format(self,name):
        if name:
            if "[" in name:
                name_replace=name
                if 'DBA' in name_replace:
                    split_name_replace=name_replace.split('[')
                    if len(split_name_replace)>1:
                        if 'DBA' in (split_name_replace)[1]:
                            b= ((split_name_replace)[1].replace('DBA','').replace(']','').strip())
                            return [split_name_replace[0],b]
                        else:
                            return self._getDBA1(name_replace)   
                        
                    else:
                        return self._getDBA1(' '.join(split_name_replace))
                else:
                    return self._getDBA1(name_replace)
            else:
                if "(" in name:
                    name_replace=name
                    if 'DBA' in name_replace:
                        split_name_replace=name_replace.split('(')
                        if len(split_name_replace)>1:
                            if 'DBA' in (split_name_replace)[1]:
                                b= ((split_name_replace)[1].replace('DBA','').replace(')','').strip())
                                return (split_name_replace[0],b)
                            else:
                                return self._getDBA1(name_replace)
                        else:
                            return  self._getDBA1(' '.join(split_name_replace)) 
                    else:
                        return self._getDBA1(name_replace)
                else:
                    return  self._getDBA1(name)
        else:
            return self._getDBA1(name)


    def errors(self,response):
        print("\n\n\n")
        print('error in this page:',self.county_data)
        if self.county_list:
            self.county_data=self.county.pop(0)
            self.county_pop=self.county_list.pop(0)
            print("\n\n\n")
            print(self.county_data)
            print("\n\n\n")
            formdata={
                    "ctl00$ScriptManager1": "ctl00$UpdatePanel1|ctl00$ContentPlaceHolder1$BtnSearch",
                    "ctl00$ContentPlaceHolder1$TxtEstdNm": "",
                    "ctl00$ContentPlaceHolder1$DrpEstdType": "All",
                    "ctl00$ContentPlaceHolder1$txtCity": "",
                    "ctl00$ContentPlaceHolder1$DrpCnty": str(self.county_pop),
                    "__LASTFOCUS": "",
                    "__VIEWSTATE": self.response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                    "__VIEWSTATEGENERATOR": self.response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "__EVENTVALIDATION": self.response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                    "ctl00$ContentPlaceHolder1$BtnSearch.x": "60",
            }
            header={
                    "Accept": '*/*',
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Host": "foodscores.state.al.us",
                    "Origin": "http://foodscores.state.al.us",
                    "Referer": self.response.url,
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36",            }
            yield scrapy.FormRequest.from_response(self.response,callback=self.parse_next1,dont_filter=True,formdata=formdata,headers=header,dont_click=True,errback=self.errors,meta={"max_retry_times":3})


    def _getDBA1(self, person_name):
        if person_name:
            person_name=re.sub('doingbusiness|doingbusiness','',re.sub(r" Dba | DBA |D/B/A|d/b/a| DBA|D B A | D B A| D B A | d b a| d b a |d b a |DBA | dba|dba |\(dba\)|dba:|\(DBA\)|D/b/a|/DBA|D/b/A|/dba|/dba/|dba/|/DBA/|DBA/|-DBA|DBA.|/DBA",' dba ',person_name))
            if re.search(' dba ', person_name, flags=re.IGNORECASE):
                name = person_name.split('dba')[0]
                dba_name = person_name.split('dba')[1]
                return name, dba_name
        return (person_name, '')











    #     # # self.state['items_count'] = self.state.get('items_count', 0) + 1
        # il = ItemLoader(item=AlFoodInspectionsSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        # il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        # il.add_value('sourceName', 'AL_Food_Inspections')
        # il.add_value('url', 'http://www.alabamapublichealth.gov/foodscores/index.html')
        # il.add_xpath('dba_name', '')
        # il.add_xpath('county', '')
        # il.add_xpath('location_address_string', '')
        # il.add_xpath('company_name', '')
        # il.add_xpath('smoke free', '')
        # il.add_xpath('inspection_Score', '')
        # il.add_xpath('inspection_type', '')
        # il.add_xpath('inspection_date', '')
        # return il.load_item()