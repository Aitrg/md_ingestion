# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-16 07:49:48
TICKET NUMBER -AI_1481
@author: Velsystems
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1481.items import AlGeologyLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.shell import inspect_response
import scrapy
from inline_requests import inline_requests
import re
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils

class AlGeologyLicensesSpider(CommonSpider): 
    name = '1481_al_geology_licenses'
    allowed_domains = ['alabama.gov']
    start_urls = ['http://www.algeobd.alabama.gov/search.aspx?sm=d_a']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1481_Licenses_Geology_AL_CurationReady'),
        'JIRA_ID':'AI_1481',
        'DOWNLOAD_DELAY':.2,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        #'JOBDIR' : CustomSettings.getJobDirectory('al_geology_licenses'),
        'TOP_HEADER':{                        'company_name': 'Company',
                         'company_phone': 'Phone',
                         'dba_name': '',
                         'location_address_string': 'Address',
                         'permit_lic_desc': '',
                         'permit_lic_eff_date': 'Effective Date',
                         'permit_lic_exp_date': 'Expiration Date',
                         'permit_lic_no': 'License Number',
                         'permit_type': ''},
        'FIELDS_TO_EXPORT':['company_name', 'dba_name', 'company_phone', 'location_address_string', 'permit_lic_no', 'permit_lic_eff_date', 'permit_lic_exp_date', 'permit_lic_desc', 'permit_type', 'url', 'sourceName', 'ingestion_timestamp'],
        'NULL_HEADERS':[]
        }

    def parse(self,response):


        yield scrapy.Request(url='http://www.algeobd.alabama.gov/search.aspx?sm=d_a',callback=self.parse_next,dont_filter=True,meta={'page':1})


    
    
    @inline_requests
    def parse_next(self, response):
        #inspect_response(response,self)
        check_None=lambda data: data if data else ""
        table1=response.xpath("//div[@id='ContentPlaceHolder1_Panel1']/fieldset//tr")[1:-2]
        #print("--------------->",table1)
        for i in table1:
            License=i.xpath(".//td[1]/text()").extract_first()
            if License.strip():
                first_name=check_None(i.xpath(".//td[2]/text()").extract_first())
                last_name=check_None(i.xpath(".//td[3]/text()").extract_first())
                Person_Name=first_name+' '+last_name
                #print("--------------->",Person_Name)
                link=i.xpath(".//td[7]/a/@href").extract_first()
                link_detail=yield scrapy.Request(url="http://www.algeobd.alabama.gov/"+link,dont_filter=True)
                company_name=check_None(link_detail.xpath("//td//b[contains(text(),'Company:')]/ancestor::td/following-sibling::td/text()").extract_first())
              
                company_phone=check_None(link_detail.xpath("//td//b[contains(text(),'Phone:')]/ancestor::td/following-sibling::td/text()").extract_first())
                Address=check_None(link_detail.xpath("//td//b[contains(text(),'Address:')]/ancestor::td/following-sibling::td/text()").extract_first())
                License_Number=check_None(link_detail.xpath("//td//b[contains(text(),'License Number:')]/ancestor::td/following-sibling::td/text()").extract_first())
                Effective_Date=check_None(link_detail.xpath("//td//b[contains(text(),'Effective Date:')]/ancestor::td/following-sibling::td/text()").extract_first())
                Expiration_Date=check_None(link_detail.xpath("//td//b[contains(text(),'Expiration Date:')]/ancestor::td/following-sibling::td/text()").extract_first())



                if "Retired" in company_name:
                    Company_name=Person_Name
                else:
                    Company_name=company_name



                 #self.state['items_count'] = self.state.get('items_count', 0) + 1
                il = ItemLoader(item=AlGeologyLicensesSpiderItem(),response=response)
                il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
                il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                il.add_value('sourceName', 'AL_Geology_Licenses')
                il.add_value('url', 'http://www.algeobd.alabama.gov/search.aspx?sm=d_a')
                il.add_value('company_name', Company_name)
                il.add_value('dba_name', '')
                il.add_value('company_phone', company_phone)
                il.add_value('location_address_string', Address if Address else "AL")
                il.add_value('permit_lic_no', License_Number)
                il.add_value('permit_lic_eff_date', Effective_Date)
                il.add_value('permit_lic_exp_date', Expiration_Date)
                il.add_value('permit_lic_desc', 'Geology License for '+Company_name if company_name else 'Geology License')
                il.add_value('permit_type', 'geology_license')
                yield il.load_item()
            
           






            # print("\n\n\n\n")
            # print("--------------->",Company_name)
            # print("--------------->",company_phone)
            # print("--------------->",Address)
            # print("--------------->",License_Number)
            # print("--------------->",Effective_Date)
            # print("--------------->",Expiration_Date)
            # print("\n\n\n\n")
            #inspect_response(link_detail,self)
        page=response.xpath("//td/span[text()='{}']/ancestor::td/following-sibling::td/a/@href".format(str(response.meta['page']))).extract_first()
        current_page=response.meta['page']+1
        print('----->',current_page,'\n\n\n')
        if page:

            form_args_pagn = JavaScriptUtils.getValuesFromdoPost(page)

            form_data={
            '__EVENTTARGET': form_args_pagn['__EVENTTARGET'],
            '__EVENTARGUMENT': form_args_pagn['__EVENTARGUMENT'],
            '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
            '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
            '__EVENTVALIDATION':response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
            'ctl00$ContentPlaceHolder1$NumberTextBox': '',
            'ctl00$ContentPlaceHolder1$NameTextBox': '',
            'ctl00$ContentPlaceHolder1$CityTextBox': ''
                      }
            # current_page+=1

            yield scrapy.FormRequest(url='http://www.algeobd.alabama.gov/search.aspx?sm=d_a',formdata=form_data,callback=self.parse_next,dont_filter=True,meta={'page':current_page})
























       