# -*- coding: utf-8 -*-
'''
Created on 2019-Mar-21 05:10:07
TICKET NUMBER -AI_973
@author: Prazi
'''
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_973.items import WaWhatcomBellinghamBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.http import FormRequest, Request
from inline_requests import inline_requests
from scrapy.selector.unified import Selector
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils
from Data_scuff.utils.searchCriteria import SearchCriteria
import scrapy
import re
import requests
import json
import operator
import itertools
import datetime
class WaWhatcomBellinghamBuildingPermitsSpider(CommonSpider):
    name = '973_wa_whatcom_bellingham_building_permits_2'
    allowed_domains = ['cob.org']
    start_urls = ['https://www.cob.org/epermits/Search/permit.aspx']
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('2_AI-973_Permits_Buildings_WA_Whatcom_Bellingham_CurationReady'),
        'JIRA_ID':'AI_973',
        'DOWNLOAD_DELAY':5,
        'COOKIES_ENABLED':True,
        'TRACKING_OPTIONAL_PARAMS':['record_number'],
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('WaWhatcomBellinghamBuildingPermitsSpider'),
        'TOP_HEADER':{'apn/pin': 'APN/PIN','approved date': 'Approved Date','contractor_address_string': '','contractor_dba': '','dba_name': '','finaled date': 'Finaled Date','inspection_date': 'Completed','inspection_pass_fail': 'Result','inspection_subtype': 'Type','inspection_type': '','location_address_string': 'Address','mixed_contractor_name': 'Contractor','mixed_name': '','mixed_subtype': '','parcel #': 'Parcel #','permit_applied_date': 'Applied Date','Status': 'Status','permit_lic_eff_date': 'Issued Date','permit_lic_exp_date': 'Expiration Date','permit_lic_fee': 'Fees','permit_lic_no': 'Permit #','permit_subtype': 'Permit Type','permit_type': '','person_address_string': '','property type': 'Property Type','subtype': 'Subtype','permit_lic_desc':'Project Description'},
        'FIELDS_TO_EXPORT':['permit_lic_no','permit_subtype','subtype','property type','permit_lic_desc','Status','permit_applied_date','approved date','permit_lic_eff_date','finaled date','permit_lic_exp_date','location_address_string','apn/pin','parcel #','permit_lic_fee','mixed_name','dba_name','mixed_subtype','person_address_string','mixed_contractor_name','contractor_dba','contractor_address_string','inspection_subtype','inspection_date','inspection_pass_fail','inspection_type','permit_type','url','sourceName','ingestion_timestamp'],
        'NULL_HEADERS':['subtype', 'property type', 'Status','approved date', 'finaled date', 'apn/pin', 'parcel #']
        }
    search_element = []
    check_first = True
    end_date=''
    def parse(self, response):
        headers={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Length': '11564',
            'Content-Type': 'application/x-www-form-urlencoded',
            'DNT': '1',
            'Host': 'www.cob.org',
            'Origin': 'https://www.cob.org',
            'Referer': 'https://www.cob.org/epermits/login.aspx?lt=either&rd=~/Search/permit.aspx',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'
        }
        form_data={
            '__LASTFOCUS':'',
            'RadScriptManager1_TSM': ';;System.Web.Extensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35:en-US:1453655a-6b8d-49b1-94c2-f77a352f5241:ea597d4b:b25378d2;Telerik.Web.UI, Version=2013.2.717.40, Culture=neutral, PublicKeyToken=121fae78165ba3d4:en-US:0507d587-20ad-4e22-b866-76bd3eaee2df:16e4e7cd:ed16cbdc:f7645509:24ee1bba:92fe8ea0:f46195d3:fa31b949:874f8ea2:19620875:490a9d4e:bd8f85e4:b7778d6c',
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
            '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
            '__EVENTVALIDATION':response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
            'ctl00_ucLogin_rwmLogin_ClientState': '',
            'ctl00$ucLogin$hfDashboardRedirect': 'https://www.cob.org/epermits/dashboard.aspx',
            'ctl00$ucLogin$hfCartRedirect': 'https://www.cob.org/epermits/ShoppingCart.aspx',
            'ctl00$ucLogin$hfViewEditProfile': 'static value',
            'ctl00$ucLogin$hfHome': 'https://www.cob.org/epermits/default.aspx',
            'ctl00$ucLogin$hfSetupAnAccountForPublic': 'https://www.cob.org/epermits/publicUserAccount.aspx?action=npa',
            'ctl00$ucLogin$hfSetupAnAccountForContractor': 'https://www.cob.org/epermits/RegistrationConfirmation.aspx',
            'ctl00$ucLogin$hfContractorCSLBVerification': 'DISABLED',
            'ctl00$ucLogin$ddlSelLogin': 'Contractor',
            'ctl00$ucLogin$txtLoginId': 'Username',
            'ctl00_ucLogin_txtLoginId_ClientState': '{"enabled":true,"emptyMessage":"Username","validationText":"","valueAsString":"","lastSetTextBoxValue":"Username"}',
            'ctl00$ucLogin$RadTextBox2': 'Password',
            'ctl00_ucLogin_RadTextBox2_ClientState': '{"enabled":true,"emptyMessage":"Password","validationText":"","valueAsString":"","lastSetTextBoxValue":"Password"}',
            'ctl00$ucLogin$txtPassword': '',
            'ctl00_ucLogin_txtPassword_ClientState': '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","lastSetTextBoxValue":""}',
            'ctl00$hfGoogleKey': 'UA-5831706-1',
            'ctl00$cplMain$txtPublicUserName': 'dummy',
            'ctl00$cplMain$txtPublicPassword': 'dummy1234',
            'ctl00$cplMain$btnPublicLogin': 'Login',
            'ctl00$cplMain$txtStLicNo': '',
            'ctl00$cplMain$txtContractorPassword': '',
            'ctl00$cplMain$txtEnterKeySubmit': ''
        }
        yield FormRequest(url='https://www.cob.org/epermits/login.aspx?lt=either&rd=%7e%2fSearch%2fpermit.aspx',formdata=form_data,headers=headers,callback= self.search, dont_filter=True)
    def search(self,response):
        if self.check_first:
            self.check_first = False
            self.search_element = SearchCriteria.numberRange(int(self.start),int(self.end), 1)
        if len(self.search_element) > 0:
            param = self.search_element.pop(0)
            param=param.zfill(5)
            form_data={
                'ctl00$RadScriptManager1': 'ctl00$RadScriptManager1|ctl00$cplMain$btnSearch',
                'RadScriptManager1_TSM': ';;System.Web.Extensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35:en-US:1453655a-6b8d-49b1-94c2-f77a352f5241:ea597d4b:b25378d2;Telerik.Web.UI, Version=2013.2.717.40, Culture=neutral, PublicKeyToken=121fae78165ba3d4:en-US:0507d587-20ad-4e22-b866-76bd3eaee2df:16e4e7cd:ed16cbdc:f7645509:24ee1bba:92fe8ea0:f46195d3:fa31b949:874f8ea2:19620875:490a9d4e:bd8f85e4:b7778d6c:58366029:e330518b:1e771326:8e6f0d33:6a6d718d;',
                'ctl00$ucLogin$hfDashboardRedirect': 'https://www.cob.org/epermits/dashboard.aspx',
                'ctl00$ucLogin$hfCartRedirect': 'https://www.cob.org/epermits/ShoppingCart.aspx',
                'ctl00$ucLogin$hfViewEditProfile': 'static value',
                'ctl00$ucLogin$hfHome': 'https://www.cob.org/epermits/default.aspx',
                'ctl00$ucLogin$hfSetupAnAccountForPublic': 'https://www.cob.org/epermits/publicUserAccount.aspx?action=npa',
                'ctl00$ucLogin$hfSetupAnAccountForContractor': 'https://www.cob.org/epermits/RegistrationConfirmation.aspx',
                'ctl00$ucLogin$hfContractorCSLBVerification': 'DISABLED',
                'ctl00$ucLogin$ddlSelLogin': 'Contractor',
                'ctl00$ucLogin$txtLoginId': 'Username',
                'ctl00_ucLogin_txtLoginId_ClientState': '{"enabled":true,"emptyMessage":"Username","validationText":"","valueAsString":"","lastSetTextBoxValue":"Username"}',
                'ctl00$ucLogin$RadTextBox2': 'Password',
                'ctl00_ucLogin_RadTextBox2_ClientState': '{"enabled":true,"emptyMessage":"Password","validationText":"","valueAsString":"","lastSetTextBoxValue":"Password"}',
                'ctl00_ucLogin_txtPassword_ClientState': '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","lastSetTextBoxValue":""}',
                'ctl00$hfGoogleKey': 'UA-5831706-1',
                'ctl00$cplMain$hfActivityMode':'', 
                'ctl00$cplMain$ddSearchBy': 'Permit_Main.PERMIT_NO',
                'ctl00$cplMain$ddSearchOper': 'CONTAINS',
                'ctl00$cplMain$txtSearchString': str(param),
                'ctl00_cplMain_rgSearchRslts_ClientState': '{"selectedIndexes":["0"],"selectedCellsIndexes":[],"unselectableItemsIndexes":[],"reorderedColumns":[],"expandedItems":[],"expandedGroupItems":[],"expandedFilterItems":[],"deletedItems":[],"hidedColumns":[],"showedColumns":[],"groupColsState":{},"hierarchyState":{},"popUpLocations":{},"draggedItemsIndexes":[]}',
                'ctl00_cplMain_tcSearchDetails_ClientState': '{"selectedIndexes":["0"],"logEntries":[],"scrollState":{}}',
                '__EVENTTARGET': 'ctl00$cplMain$btnSearch',
                '__VIEWSTATE':response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(), 
                '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                '__ASYNCPOST': 'true',
                'RadAJAXControlID': 'ctl00_RadAjaxManager1'
            }
            currentPage = 1
            yield FormRequest(url=self.start_urls[0], headers={'Referer': self.start_urls},formdata=form_data,callback= self.parse_list, dont_filter=True,meta={'currentPage':currentPage,'param':param})
    def parse_list(self,response):
        currentPage = response.meta['currentPage']
        param=response.meta['param']
        responseValues = response.text.split('|')
        viewstate = ""
        viewgenerator = ""
        for i in range(len(responseValues)):
            if responseValues[i] == "__VIEWSTATE":
                viewstate = responseValues[i+1]
            if responseValues[i] == "__VIEWSTATEGENERATOR":
                viewgenerator = responseValues[i+1]
            if responseValues[i] == "__VIEWSTATEGENERATOR":
                viewgenerator = responseValues[i+1]
        table=response.xpath('//table[@id="ctl00_cplMain_rgSearchRslts_ctl00"]//tr')[7:]
        for ind,a in enumerate(table):
            record_number=a.xpath('td[1]/text()').extract_first()
            address=a.xpath('td[2]/span/text()').extract_first()
            parcel_number=a.xpath('td[4]/span/text()').extract_first()
            form_data_2={
                'ctl00$RadScriptManager1': 'ctl00$ctl00$cplMain$rgSearchRsltsPanel|ctl00$cplMain$rgSearchRslts',
                'RadScriptManager1_TSM': ";;System.Web.Extensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35:en-US:1453655a-6b8d-49b1-94c2-f77a352f5241:ea597d4b:b25378d2;Telerik.Web.UI, Version=2013.2.717.40, Culture=neutral, PublicKeyToken=121fae78165ba3d4:en-US:0507d587-20ad-4e22-b866-76bd3eaee2df:16e4e7cd:ed16cbdc:f7645509:24ee1bba:92fe8ea0:f46195d3:fa31b949:874f8ea2:19620875:490a9d4e:bd8f85e4:b7778d6c:58366029:e330518b:1e771326:8e6f0d33:6a6d718d;",
                'ctl00$ucLogin$hfDashboardRedirect': 'https://www.cob.org/epermits/dashboard.aspx',
                'ctl00$ucLogin$hfCartRedirect': 'https://www.cob.org/epermits/ShoppingCart.aspx',
                'ctl00$ucLogin$hfViewEditProfile': 'static value',
                'ctl00$ucLogin$hfHome': 'https://www.cob.org/epermits/default.aspx',
                'ctl00$ucLogin$hfSetupAnAccountForPublic': 'https://www.cob.org/epermits/publicUserAccount.aspx?action=npa',
                'ctl00$ucLogin$hfSetupAnAccountForContractor': 'https://www.cob.org/epermits/RegistrationConfirmation.aspx',
                'ctl00$ucLogin$hfContractorCSLBVerification': 'DISABLED',
                'ctl00$ucLogin$ddlSelLogin': 'Contractor',
                'ctl00$ucLogin$txtLoginId': 'Username',
                'ctl00_ucLogin_txtLoginId_ClientState': '{"enabled":true,"emptyMessage":"Username","validationText":"","valueAsString":"","lastSetTextBoxValue":"Username"}',
                'ctl00$ucLogin$RadTextBox2': 'Password',
                'ctl00_ucLogin_RadTextBox2_ClientState': '{"enabled":true,"emptyMessage":"Password","validationText":"","valueAsString":"","lastSetTextBoxValue":"Password"}',
                'ctl00_ucLogin_txtPassword_ClientState': '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","lastSetTextBoxValue":""}',
                'ctl00$hfGoogleKey': 'UA-5831706-1',
                'ctl00$cplMain$ddSearchBy': 'Permit_Main.PERMIT_NO',
                'ctl00$cplMain$ddSearchOper': 'CONTAINS',
                'ctl00$cplMain$txtSearchString': str(param),
                'ctl00_cplMain_rgSearchRslts_ClientState': '{"selectedIndexes":["0"],"selectedCellsIndexes":[],"unselectableItemsIndexes":[],"reorderedColumns":[],"expandedItems":[],"expandedGroupItems":[],"expandedFilterItems":[],"deletedItems":[],"hidedColumns":[],"showedColumns":[],"groupColsState":{},"hierarchyState":{},"scrolledPosition":"0,0","popUpLocations":{},"draggedItemsIndexes":[]}',
                'ctl00_cplMain_tcSearchDetails_ClientState': '{"selectedIndexes":["2"],"logEntries":[],"scrollState":{}}',
                '__EVENTTARGET': 'ctl00$cplMain$rgSearchRslts',
                '__EVENTARGUMENT': 'RowClick;'+str(ind),
                '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': viewgenerator,
                '__ASYNCPOST': 'true',
                'RadAJAXControlID': 'ctl00_RadAjaxManager1'
            }
            yield FormRequest(url=self.start_urls[0],formdata=form_data_2,callback= self.get_data, dont_filter=True,meta={'record_number':record_number,'address':address,'parcel_number':parcel_number,'optional':{'record_number':record_number}})
        total_pages=response.xpath("//tr[5]/td/span[@class='font12 italic']/text()").extract_first()
        if total_pages:
            pages=str(total_pages).split('of')[1]
            page=pages.strip()
            headers={
                'Accept': '*/*',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Host': 'www.cob.org',
                'Origin': 'https://www.cob.org',
                'Referer': 'https://www.cob.org/epermits/Search/permit.aspx',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36'}
            if int(currentPage) < int(page)+1:
                form_data_3={
                    'ctl00$RadScriptManager1': 'ctl00$ctl00$cplMain$rgSearchRsltsPanel|ctl00$cplMain$rgSearchRslts',
                    'RadScriptManager1_TSM': ';;System.Web.Extensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35:en-US:1453655a-6b8d-49b1-94c2-f77a352f5241:ea597d4b:b25378d2;Telerik.Web.UI, Version=2013.2.717.40, Culture=neutral, PublicKeyToken=121fae78165ba3d4:en-US:0507d587-20ad-4e22-b866-76bd3eaee2df:16e4e7cd:ed16cbdc:f7645509:24ee1bba:92fe8ea0:f46195d3:fa31b949:874f8ea2:19620875:490a9d4e:bd8f85e4:b7778d6c:58366029:e330518b:1e771326:8e6f0d33:6a6d718d;',
                    'ctl00$ucLogin$hfDashboardRedirect': 'https://www.cob.org/epermits/dashboard.aspx',
                    'ctl00$ucLogin$hfCartRedirect': 'https://www.cob.org/epermits/ShoppingCart.aspx',
                    'ctl00$ucLogin$hfViewEditProfile': 'static value',
                    'ctl00$ucLogin$hfHome': 'https://www.cob.org/epermits/default.aspx',
                    'ctl00$ucLogin$hfSetupAnAccountForPublic': 'https://www.cob.org/epermits/publicUserAccount.aspx?action=npa',
                    'ctl00$ucLogin$hfSetupAnAccountForContractor': 'https://www.cob.org/epermits/RegistrationConfirmation.aspx',
                    'ctl00$ucLogin$hfContractorCSLBVerification': 'DISABLED',
                    'ctl00$ucLogin$ddlSelLogin': 'Contractor',
                    'ctl00$ucLogin$txtLoginId': 'Username',
                    'ctl00_ucLogin_txtLoginId_ClientState': '{"enabled":true,"emptyMessage":"Username","validationText":"","valueAsString":"","lastSetTextBoxValue":"Username"}',
                    'ctl00$ucLogin$RadTextBox2': 'Password',
                    'ctl00_ucLogin_RadTextBox2_ClientState': '{"enabled":true,"emptyMessage":"Password","validationText":"","valueAsString":"","lastSetTextBoxValue":"Password"}',
                    'ctl00_ucLogin_txtPassword_ClientState': '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","lastSetTextBoxValue":""}',
                    'ctl00$hfGoogleKey': 'UA-5831706-1',
                    'ctl00$cplMain$activeTab': '1',
                    'ctl00$cplMain$ddSearchBy': 'Permit_Main.PERMIT_NO',
                    'ctl00$cplMain$ddSearchOper': 'CONTAINS',
                    'ctl00$cplMain$txtSearchString': str(param),
                    'ctl00_cplMain_tcSearchDetails_ClientState': '{"selectedIndexes":["1"],"logEntries":[],"scrollState":{}}',
                    '__EVENTTARGET': 'ctl00$cplMain$rgSearchRslts',
                    '__EVENTARGUMENT': 'FireCommand:ctl00$cplMain$rgSearchRslts$ctl00;Page;next',
                    '__LASTFOCUS': '',
                    '__VIEWSTATE': viewstate,
                    '__VIEWSTATEGENERATOR': viewgenerator,
                    '__ASYNCPOST': 'true',
                    'RadAJAXControlID': 'ctl00_RadAjaxManager1'
                }
                yield FormRequest(url=self.start_urls[0],formdata=form_data_3,headers=headers,callback= self.parse_list, dont_filter=True,meta={'currentPage': int(currentPage)+1,'param':param})
        if len(self.search_element) > 0:
            yield scrapy.Request(url=self.start_urls[0], callback=self.search, dont_filter=True)
    @inline_requests
    def get_data(self,response):
        meta={}
        meta["mixed_name"]=meta["mixed_subtype"]=meta["person_address_string"]=meta["mixed_contractor_name"]=meta["contractor_address_string"]=meta["inspection_subtype"]=meta["inspection_type"]=meta["inspection_pass_fail"]=meta["completed_date"]=meta['address']=meta["apn_pin"]=meta["parent_project"]=meta["property_type"]=meta["permit_lic_exp_date"]=meta["finaled_date"]=meta["approved_date"]=meta["permit_lic_eff_date"]=meta["permit_applied_date"]=meta["permit_lic_status"]=meta["permit_lic_desc"]=meta['permit_subtype']=meta['permit_lic_type']=meta['parcel_number']=meta['record_number']=''
        address=response.meta['address']
        meta['parcel_number']=response.meta['parcel_number']
        meta['record_number']=response.meta['record_number']
        meta['permit_lic_type']=response.xpath("//tr/td[(contains(span,'Type:'))]/following-sibling::td/span/text()").extract_first()
        meta['permit_subtype']=response.xpath("//tr/td[(contains(span,'Subtype:'))]/following-sibling::td/span/text()").extract_first()
        if response.xpath("//tr/td[(contains(span,'Project Description:'))]/following-sibling::td/span/text()").extract_first()=='':
            meta["permit_lic_desc"]=meta["permit_subtype"]
        else:
            meta["permit_lic_desc"]=response.xpath("//tr/td[(contains(span,'Project Description:'))]/following-sibling::td/span/text()").extract_first()
        meta["permit_lic_status"]=response.xpath("//tr/td[(contains(span,'Status:'))]/following-sibling::td/span/text()").extract_first()
        meta["permit_applied_date"]=response.xpath("//tr/td[(contains(span,'Applied Date:'))]/following-sibling::td/span/text()").extract_first()
        meta["permit_lic_eff_date"]=response.xpath("//tr/td[(contains(span,'Issued Date:'))]/following-sibling::td/span/text()").extract_first()
        meta["approved_date"]=response.xpath("//tr/td[(contains(span,'Approved Date:'))]/following-sibling::td/span/text()").extract_first()
        meta["finaled_date"]=response.xpath("//tr/td[(contains(span,'Finaled Date:'))]/following-sibling::td/span/text()").extract_first()
        meta["permit_lic_exp_date"]=response.xpath("//tr/td[(contains(span,'Expiration Date:'))]/following-sibling::td/span/text()").extract_first()
        meta["property_type"]=response.xpath("//tr/td[(contains(span,'Property Type'))]/following-sibling::td/span/text()").extract_first()
        meta["parent_project"]=response.xpath('//*[@id="cplMain_ctl02_ctl02_tableParentProjects"]//tr[2]/td[2]/a/text()').extract_first()
        meta["apn_pin"]=response.xpath("//tr/td[(contains(span,'APN/PIN:'))]/following-sibling::td/a/text()").extract_first()
        City_State_Zip=response.xpath("//tr/td[(contains(span,'City/State/Zip:'))]/following-sibling::td/span/text()").extract_first()
        if address:
            if address and City_State_Zip:
                meta['address']=address+', '+City_State_Zip
            elif address:
                meta['address']=address+', WA'
        else:
            meta['address']='WA'
        meta['permit_lic_fee']=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl05_rgFeeDetails')]/td[@class='ellipsis'][2]/span/text()").extract_first()
        meta["contact_len"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl04_rgContactInfo')]/td[@class='ellipsis'][1]/text()").extract()
        yield self.save_to_csv(response,**meta)
        if meta["contact_len"]:
            meta["contact_len"]=meta["contact_len"]
            for i in range(1,int(len(meta["contact_len"]))+1):
                meta["mixed_sub"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl04_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][1]/text()").extract_first()
                if str(meta["mixed_sub"]).strip()=='CONTRACTOR':
                    meta["mixed_name"]=meta["mixed_subtype"]=meta["person_address_string"]=meta["inspection_subtype"]=meta["inspection_type"]=meta["inspection_pass_fail"]=meta["completed_date"]=''
                    meta["mixed_contractor_name"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl04_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][2]/text()").extract_first()
                    meta["contractor_address_string"]=str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl04_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][3]/text()").extract_first()).strip()+', '+str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl04_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][4]/text()").extract_first()).strip()
                    yield self.save_to_csv(response,**meta)
                else:
                    meta["mixed_contractor_name"]=meta["contractor_address_string"]=meta["inspection_subtype"]=meta["inspection_type"]=meta["inspection_pass_fail"]=meta["completed_date"]=''
                    meta["mixed_name"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl04_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][2]/text()").extract_first()
                    meta["mixed_subtype"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl04_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][1]/text()").extract_first()
                    person_add1=str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl04_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][3]/text()").extract_first()).strip()
                    person_add2=str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl04_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][4]/text()").extract_first()).strip()
                    if person_add1 and person_add1:
                        meta["person_address_string"]=person_add1+', '+person_add2
                    elif person_add1:
                        meta["person_address_string"]=person_add1+', WA'
                    else:
                        meta["person_address_string"]='WA'
                    yield self.save_to_csv(response,**meta)
        else:
            meta["contact_len"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgContactInfo')]/td[@class='ellipsis'][1]/text()").extract()
            if meta["contact_len"]:
                for i in range(1,int(len(meta["contact_len"]))+1):
                    meta["mixed_sub"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][1]/text()").extract_first()
                    if str(meta["mixed_sub"]).strip()=='CONTRACTOR':
                        meta["mixed_name"]=meta["mixed_subtype"]=meta["person_address_string"]=meta["inspection_subtype"]=meta["inspection_type"]=meta["inspection_pass_fail"]=meta["completed_date"]=''
                        meta["mixed_contractor_name"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][2]/text()").extract_first()
                        meta["contractor_address_string"]=str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][3]/text()").extract_first()).strip()+', '+str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][4]/text()").extract_first()).strip()
                        yield self.save_to_csv(response,**meta)
                    else:
                        meta["mixed_contractor_name"]=meta["contractor_address_string"]=meta["inspection_subtype"]=meta["inspection_type"]=meta["inspection_pass_fail"]=meta["completed_date"]=''
                        meta["mixed_name"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][2]/text()").extract_first()
                        meta["mixed_subtype"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][1]/text()").extract_first()
                        person_add1=str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][3]/text()").extract_first()).strip()
                        person_add2=str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][4]/text()").extract_first()).strip()
                        if person_add1 and person_add1:
                            meta["person_address_string"]=person_add1+', '+person_add2
                        elif person_add1:
                            meta["person_address_string"]=person_add1+', WA'
                        else:
                            meta["person_address_string"]='WA'
                        yield self.save_to_csv(response,**meta)
            else:
                meta["contact_len"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgContactInfo')]/td[@class='ellipsis'][1]/text()").extract()
                for i in range(1,int(len(meta["contact_len"]))+1):
                    meta["mixed_sub"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][1]/text()").extract_first()
                    if str(meta["mixed_sub"]).strip()=='CONTRACTOR':
                        meta["mixed_name"]=meta["mixed_subtype"]=meta["person_address_string"]=meta["inspection_subtype"]=meta["inspection_type"]=meta["inspection_pass_fail"]=meta["completed_date"]=''
                        meta["mixed_contractor_name"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][2]/text()").extract_first()
                        meta["contractor_address_string"]=str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][3]/text()").extract_first()).strip()+', '+str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][4]/text()").extract_first()).strip()
                        yield self.save_to_csv(response,**meta)
                    else:
                        meta["mixed_contractor_name"]=meta["contractor_address_string"]=meta["inspection_subtype"]=meta["inspection_type"]=meta["inspection_pass_fail"]=meta["completed_date"]=''
                        meta["mixed_name"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][2]/text()").extract_first()
                        meta["mixed_subtype"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][1]/text()").extract_first()
                        person_add1=str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][3]/text()").extract_first()).strip()
                        person_add2=str(response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgContactInfo')]["+str(i)+"]/td[@class='ellipsis'][4]/text()").extract_first()).strip()
                        if person_add1 and person_add1:
                            meta["person_address_string"]=person_add1+', '+person_add2
                        elif person_add1:
                            meta["person_address_string"]=person_add1+', WA'
                        else:
                            meta["person_address_string"]='WA'
                        yield self.save_to_csv(response,**meta)
        insp_len=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgInspectionInfo_ctl00')]/td[@class='ellipsis'][1]/text()").extract()
        if insp_len:
            for j in range(1,int(len(insp_len))+1):
                meta["mixed_name"]=meta["mixed_subtype"]=meta["person_address_string"]=meta["mixed_contractor_name"]=meta["contractor_address_string"]=''
                meta["inspection_subtype"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgInspectionInfo_ctl00')]["+str(j)+"]/td[@class='ellipsis'][1]/text()").extract_first()
                meta["inspection_type"]=''
                if meta["inspection_subtype"]=='':
                    meta["inspection_type"]=''
                else:
                    meta["inspection_type"]='building_inspection'
                meta["inspection_pass_fail"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgInspectionInfo_ctl00')]["+str(j)+"]/td[@class='ellipsis'][2]/text()").extract_first()
                meta["completed_date"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl06_rgInspectionInfo_ctl00')]["+str(j)+"]/td[@class='ellipsis'][4]/text()").extract_first()
                yield self.save_to_csv(response,**meta)
        else:
            insp_len=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgInspectionInfo_ctl00')]/td[@class='ellipsis'][1]/text()").extract()
            for j in range(1,int(len(insp_len))+1):
                meta["mixed_name"]=meta["mixed_subtype"]=meta["person_address_string"]=meta["mixed_contractor_name"]=meta["contractor_address_string"]=''
                meta["inspection_subtype"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgInspectionInfo_ctl00')]["+str(j)+"]/td[@class='ellipsis'][1]/text()").extract_first()
                meta["inspection_type"]=''
                if meta["inspection_subtype"]=='':
                    meta["inspection_type"]=''
                else:
                    meta["inspection_type"]='building_inspection'
                meta["inspection_pass_fail"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgInspectionInfo_ctl00')]["+str(j)+"]/td[@class='ellipsis'][2]/text()").extract_first()
                meta["completed_date"]=response.xpath("//tr[contains(@id,'ctl00_cplMain_ctl08_rgInspectionInfo_ctl00')]["+str(j)+"]/td[@class='ellipsis'][4]/text()").extract_first()
                yield self.save_to_csv(response,**meta)
    def save_to_csv(self,response,**meta):
        il = ItemLoader(item=WaWhatcomBellinghamBuildingPermitsSpiderItem())
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'WA_Whatcom_Bellingham_Building_Permits')
        il.add_value('url', 'https://www.cob.org/epermits/Search/permit.aspx')
        il.add_value('permit_lic_no', meta['record_number'])
        il.add_value('permit_subtype', meta['permit_lic_type'])
        il.add_value('subtype', meta['permit_subtype'])
        il.add_value('property type', meta["property_type"])
        if meta["permit_lic_desc"]:
            meta["permit_lic_desc"]=meta["permit_lic_desc"]
        else:
            meta["permit_lic_desc"]='Building Permit'
        il.add_value('permit_lic_desc', meta["permit_lic_desc"])
        il.add_value('Status', meta["permit_lic_status"])
        il.add_value('permit_applied_date',meta["permit_applied_date"])
        il.add_value('approved date', meta["approved_date"])
        il.add_value('permit_lic_eff_date', meta["permit_lic_eff_date"])
        il.add_value('finaled date', meta["finaled_date"])
        il.add_value('permit_lic_exp_date', meta["permit_lic_exp_date"])
        il.add_value('location_address_string',meta['address'])
        il.add_value('apn/pin',meta["apn_pin"])
        il.add_value('parcel #',meta['parcel_number'])
        il.add_value('permit_lic_fee',meta['permit_lic_fee'])
        il.add_value('mixed_name',self._getDBA(meta['mixed_name'])[0])
        il.add_value('dba_name',self._getDBA(meta['mixed_name'])[1])
        il.add_value('mixed_subtype',meta["mixed_subtype"])
        il.add_value('person_address_string',meta["person_address_string"])
        il.add_value('mixed_contractor_name',self._getDBA(meta['mixed_contractor_name'])[0])
        il.add_value('contractor_dba',self._getDBA(meta['mixed_contractor_name'])[1])
        il.add_value('contractor_address_string',meta["contractor_address_string"])
        il.add_value('inspection_subtype',meta["inspection_subtype"])
        il.add_value('inspection_date',meta["completed_date"])
        il.add_value('inspection_pass_fail',meta["inspection_pass_fail"])
        il.add_value('inspection_type',meta["inspection_type"])
        il.add_value('permit_type', 'building_permit')
        return il.load_item()