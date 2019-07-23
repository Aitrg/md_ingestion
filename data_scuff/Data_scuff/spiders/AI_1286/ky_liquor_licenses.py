'''
scrapy crawl 1286_ky_liquor_licenses -a start=2 -a end=3
Created on 2019-Jun-28 03:57:52
TICKET NUMBER -AI_1286
@author: Muhil
'''
from bs4 import BeautifulSoup
import requests
from scrapy.http import HtmlResponse
import re
import json
from scrapy.shell import inspect_response
import scrapy
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils
from inline_requests import inline_requests
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import os
from Data_scuff.spiders.AI_1286.items import KyLiquorLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils

class KyLiquorLicensesSpider(CommonSpider):
    name = '1286_ky_liquor_licenses'
    # allowed_domains = ['ky.gov']
    start_urls = ['https://www.aitrg.com']
    pro_urls = []
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1286_Licenses_Liquor_KY_CurationReady'),
        'JIRA_ID':'AI_1286',
        'DOWNLOAD_DELAY':.2,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'AJAXCRAWL_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('ky_liquor_licenses'),
        'TOP_HEADER':{                        'company_name': 'Licensee Name',
                         'company_phone': 'Premises Phone',
                         'company_subtype': 'Business Type',
                         'dba_name': 'DBA',
                         'effective date': 'Effective Date',
                         'is licensee owner': 'Is Licensee Owner',
                         'licensing county': 'Licensing County',
                         'location_address_string': 'Site Address + City + State + Zip',
                         'mail_address_string': 'Mailing Address + City + State + Zip',
                         'permit_lic_desc': '',
                         'permit_lic_eff_date': 'Issue Date',
                         'permit_lic_exp_date': 'Expire Date',
                         'permit_lic_no': 'License Number',
                         'permit_lic_status': 'Status',
                         'permit_subtype': 'License Type',
                         'permit_type': '',
                         'person_name': 'PARTNERS',
                         'real estate owner': 'Real Estate Owner',
                         'restrictions': 'Restrictions',
                         'site id': 'Site Id'},
        'FIELDS_TO_EXPORT':['site id',
                         'location_address_string',
                         'licensing county',
                         'company_phone',
                         'company_name',
                         'dba_name',
                         'company_subtype',
                         'mail_address_string',
                         'person_name',
                         'real estate owner',
                         'is licensee owner',
                         'permit_subtype',
                         'permit_lic_desc',
                         'permit_lic_no',
                         'permit_lic_status',
                         'permit_lic_eff_date',
                         'effective date',
                         'permit_lic_exp_date',
                         'restrictions',
                         'permit_type',
                         'url',
                         'sourceName',
                         'ingestion_timestamp'],
        'NULL_HEADERS':['site id', 'licensing county', 'real estate owner', 'is licensee owner', 'effective date', 'restrictions']
        }
    # @inline_requests
    def parse(self, response):
        module_dir =os.path.dirname(os.path.realpath(__file__))
        paths=[module_dir+'/AI-1286/file1.csv',module_dir+'/AI-1286/file2.csv',module_dir+'/AI-1286/file3.csv',module_dir+'/AI-1286/file4.csv',module_dir+'/AI-1286/file5.csv',module_dir+'/AI-1286/file6.csv',]
        # yield scrapy.Request(url='file://'+path,callback=self.parse_rows,dont_filter=True)
        import csv
        # path=r'/Users/imac/Downloads/Permits_Buildings_FL_Highlands_CurationReady_20181004_v12.csv'
        # for path in paths[int(self.start):int(self.end)]:
        # print('path====>',path)
        with open(module_dir+'/file1.csv', errors='ignore') as csvfile:
            readCSV = csv.reader(csvfile, delimiter='\t')
            count=0

            for row in readCSV:
                count+=1
                if count<=2:
                    continue
                if len(row)<5:
                    break
                print('==============>',len(row))
                # print(row)
    # def parse_row(self,response,row):
                il = ItemLoader(item=KyLiquorLicensesSpiderItem())
                il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags,lambda data:re.sub(r'\s+', ' ',data) if data else '',replace_escape_chars)
                il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                il.add_value('sourceName', 'KY_Liquor_Licenses')
                il.add_value('url', 'https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx')
                il.add_value('permit_type', 'liquor_license')
                # # il.add_value('unique_id','')
                # for k in data_dic:
                #     il.add_value(k,data_dic[k])
                # return il
                il.add_value('site id', row[0])
                il.add_value('location_address_string', row[1])
                il.add_value('licensing county', row[2])
                il.add_value('company_phone', row[3])
                il.add_value('company_name', row[4])
                il.add_value('dba_name', row[5])
                il.add_value('company_subtype', row[6])
                il.add_value('mail_address_string', row[7])
                il.add_value('person_name', row[8])
                il.add_value('real estate owner', row[9])
                il.add_value('is licensee owner', row[10])
                il.add_value('permit_subtype', row[11])
                il.add_value('permit_lic_desc', row[12])
                il.add_value('permit_lic_no',row[13])
                il.add_value('permit_lic_status', row[14])
                il.add_value('permit_lic_eff_date', row[15])
                il.add_value('effective date', row[16])
                il.add_value('permit_lic_exp_date', row[17])
                
                il.add_value('restrictions', row[18])
                # il.add_value('permit_subtype', row['permit_subtype'])

                yield il.load_item()

                        
                      

    #     # inspect_response(response,self)
    #     # self.response=response
    #     self.s = requests.session()
    #     self.site_id=' '
    #     driver=BeautifulSoup(self.s.get('https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx').text,"html.parser")
    #     res = HtmlResponse('https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx', body=str.encode(driver.prettify()))
    #     req_url='https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx'        
    #     self.facility=res.xpath("//select[contains(@title,'Select Business Type')]/option/@value").extract()[1:]
    #     print("***********",self.facility)
    #     page=1
    #     module_dir = os.path.dirname(os.path.realpath(__file__))
    #     print('===========',module_dir[:module_dir.find('Data_scuff')])
    #     path=module_dir[:module_dir.find('Data_scuff')]+'storage/ky_liquor_licenses.csv'
    #     with open(path,'w') as f:
    #         f.write('|'.join(['Site Id','Site Address + City + State + Zip','Licensing County','Premises Phone','Licensee Name','DBA','Business Type','Mailing Address + City + State + Zip','PARTNERS','Real Estate Owner','Is Licensee Owner','License Type',' ','License Number','Status','Issue Date','Effective Date','Expire Date','Restrictions',' ',' ',' ',' '])+'\n')
    #         f.write('|'.join([' ',
    #                      'location_address_string',
    #                      ' ',
    #                      'company_phone',
    #                      'company_name',
    #                      'dba_name',
    #                      'company_subtype',
    #                      'mail_address_string',
    #                      'person_name',
    #                      ' ',
    #                      ' ',
    #                      'permit_subtype',
    #                      'permit_lic_desc',
    #                      'permit_lic_no',
    #                      'permit_lic_status',
    #                      'permit_lic_eff_date',
    #                      ' ',
    #                      'permit_lic_exp_date',
    #                      ' ',
    #                      'permit_type',
    #                      'url',
    #                      'sourceName',
    #                      'ingestion_timestamp'])+'\n')
    #         for search in self.facility[int(self.start):int(self.end)]:
    #             # print(response)
    #             # response=yield scrapy.Request(url='http://kentucky.gov/')
    #             self.form_data={
    #                   'ctl00$toolkitScriptMaster': 'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$UpdatePanel|ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$searchButton',
    #                     'ctl00_toolkitScriptMaster_HiddenField': ';;AjaxControlToolkit, Version=4.1.50401.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:5da0a7c3-4bf6-4d4c-b5d5-f0137ca3a302:de1feab2:f2c8e708:720a52bf:f9cec9bc:589eaa30:698129cf:7a92f56c',
    #                     '__LASTFOCUS':'', 
    #                     '__EVENTTARGET':'',
    #                     '__EVENTARGUMENT': '',
    #                     '__VIEWSTATE1': str(res.xpath('//*[@id="__VIEWSTATE1"]/@value').extract_first()),
    #                     '__VIEWSTATE': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollLeft': '0',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollTop': '300',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxSiteID':self.site_id,
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxAdrOfPrememsis': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxLicenseeOrDbaName': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxCity': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxLicenseNumber': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxZipCode': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlLicenseType': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlCountyCode':'' ,
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlBusinessType': '',
    #                     'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '1',
    #                     # '__ASYNCPOST': 'true',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$searchButton':'Search'
    #             }
    #             self.header={
    #                 'Origin': 'https://dppweb.ky.gov',
    #                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36', 
    #                  'Accept': '*/*' ,
    #                  'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' ,
    #                  'Referer': 'https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx'
    #                 }

    #             res_req = self.s.post(res.url, data=self.form_data,headers=self.header,verify=True)
    #             driver = BeautifulSoup(res_req.text, 'html5lib')
    #             response_one= HtmlResponse('https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx', body=str.encode(driver.prettify()))
    #             while True:
    #                 header={
    #                     'Origin': 'https://dppweb.ky.gov',
    #                      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36', 
    #                      'Accept': '*/*' ,
    #                      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' ,
    #                      'Referer': 'https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx'
    #                     }
    #                 # if page>1:
    #                 inspect_response(response_one,self)
    #                 data_url='https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx'
    #                 form_data={
    #                     'ctl00_toolkitScriptMaster_HiddenField': ';;AjaxControlToolkit, Version=4.1.50401.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:5da0a7c3-4bf6-4d4c-b5d5-f0137ca3a302:de1feab2:f2c8e708:720a52bf:f9cec9bc:589eaa30:698129cf:7a92f56c',
    #                     '__LASTFOCUS':'', 
    #                     '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$resultsGrid$ctl03$lnkbtnDetails',
    #                     '__EVENTARGUMENT': '',
    #                     '__VIEWSTATE1': str(response_one.xpath('//*[@id="__VIEWSTATE1"]/@value').extract_first()),
    #                     '__VIEWSTATE': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollLeft': '0',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollTop': '352',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxSiteID':self.site_id,  
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxAdrOfPrememsis': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxLicenseeOrDbaName': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxCity': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxLicenseNumber': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxZipCode': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlLicenseType': '',
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlCountyCode':'' ,
    #                     'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlBusinessType': search,
    #                     'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '1'
    #                     }
    #         #         for links in response_one.xpath("//a[contains(text(),'Details')]/@href").extract():
    #         # #             link=links
    #         #             form_args_pagn = JavaScriptUtils.getValuesFromdoPost(links)
    #         #             # form_dataa = {
    #         #             # '__EVENTTARGET': ,
    #         #             form_data['ctl00$toolkitScriptMaster']= 'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$resultsUpdatePanel|'+form_args_pagn['__EVENTTARGET']
    #         #             form_data['__EVENTTARGET']= form_args_pagn['__EVENTTARGET']
    #         #             print(json.dumps(form_data,indent=3))
    #         #             res_data = self.s.post(data_url, data=form_data,headers=header,verify=True)
    #         #             driver = BeautifulSoup(res_data.text, 'html5lib')
    #         #             res = HtmlResponse(data_url, body=str.encode(driver.prettify()))
    #         #             # inspect_response(res,self)
    #         #             data_dic={}
    #         #             datas=lambda res,data:res.xpath("//div[contains(text(),'{}')]/following-sibling::div/text()".format(data)).extract_first()
    #         #             site_id=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteIdText']/text()").extract_first()
    #         #             address=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteAddressLine1Text']/text()").extract_first()
    #         #             city=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteCityText']/text()").extract_first()
    #         #             state=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteStateText']/text()").extract_first()
    #         #             loc_zip=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteZipCodeText']/text()").extract_first()
    #         #             location_address=self.format__address_4(address,city,state,loc_zip)
    #         #             location_address_string=location_address if location_address else 'KY'
    #         #             licensing_county=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteLicensingCountyText']/text()").extract_first()
    #         #             company_phone=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSitePhoneNumberText']/text()").extract_first()
    #         #             company_name=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblLicenseeNameText']/text()").extract_first()
    #         #             dba_name=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblDbaNameText']/text()").extract_first()
    #         #             company_subtype=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblBusinessTypeText']/text()").extract_first()
    #         #             company_subtype=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblDbaNameText']/text()").extract_first()
    #         #             mail_address=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblMailingAddressText']/text()").extract_first()
    #         #             mail_city=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblMailingCityText']/text()").extract_first()
    #         #             mail_state=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblMailingStateText']/text()").extract_first()
    #         #             mail_zip=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblMailingZipText']/text()").extract_first()
    #         #             mail_address_string=self.format__address_4(mail_address,mail_city,mail_state,mail_zip)
    #         #             mail_address_string=mail_address_string
    #         #             # inspect_response(res,self)
    #         #             print('')
    #         #             person_name=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lvOfficers_ctrl0_Label2']/text()").   extract_first()
                   
    #         #             real_estate_owner=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblRealEstateOwnerText']/text()").   extract_first()
    #         #             is_licensee_owner=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblIsLicenseeOwnerText']/text()").   extract_first()
    #         #             company_subtype=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblBusinessTypeText']/text()").   extract_first()
    #         #             company_subtype=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblBusinessTypeText']/text()").   extract_first()
    #         #             company_subtype=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblBusinessTypeText']/text()").   extract_first()
    #         #             restrictions=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblRestrictions']//text()").extract_first()
    #         #             print('================>',search)
    #         #             if res.xpath("//table[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_gvLicenses']//tr"):
    #         #                 for tr in res.xpath("//table[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_gvLicenses']//tr"):

    #         #                     # il=self.save_csv(response,           il.add_value('permit_subtype', tr.xpath('.//td[1]//text()').extract_first())
    #         #                     # il.add_value('permit_lic_desc', tr.xpath('.//td[1]//text()').extract_first())
    #         #                     # il.add_value('permit_lic_no', tr.xpath('.//td[2]//text()').extract_first())
    #         #                     # il.add_value('permit_lic_status', tr.xpath('.//td[3]//text()').extract_first())
    #         #                     # il.add_value('
    #         #                     # il.add_value('effective date', tr.xpath('.//td[5]//text()').extract_first())
    #         #                     # il.add_value('permit_lic_exp_date', tr.xpath('.//td[6]//text()').extract_first())
    #         #                     if tr.xpath('.//td[4]//text()').extract_first():
    #         #                         permit_lic_eff_date=tr.xpath('.//td[4]/span/text()').extract_first()
    #         #                         permit_subtype=tr.xpath('.//td[1]//text()').extract_first()
    #         #                         permit_lic_desc=tr.xpath('.//td[1]//text()').extract_first()
    #         #                         permit_lic_no=tr.xpath('.//td[2]//text()').extract_first()
    #         #                         permit_lic_status=tr.xpath('.//td[3]//text()').extract_first()
    #         #                         effective_date=tr.xpath('.//td[5]/span/text()').extract_first()
    #         #                         permit_lic_exp_date=tr.xpath('.//td[6]/span/text()').extract_first()
    #         #                         lis=[site_id,location_address_string,licensing_county,company_phone,company_name,dba_name,company_subtype,mail_address_string,person_name,real_estate_owner,is_licensee_owner,permit_subtype,permit_lic_desc,permit_lic_no,permit_lic_status,permit_lic_eff_date,effective_date,permit_lic_exp_date,restrictions,'liquor_license','https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx','KY_Liquor_Licenses',Utils.getingestion_timestamp()]
    #         #                         f.write('|'.join(list([re.sub(r'\s+', ' ',data) if data else '' for data in lis]))+"\n")
    #         #                     # il_lis.append(il)
    #         #                     # yield il.load_item()
    #         #             else:
    #         #                 permit_subtype=' '
    #         #                 permit_lic_desc=' '
    #         #                 permit_lic_eff_date= ' '
    #         #                 permit_lic_no=' '
    #         #                 permit_lic_status=' '
    #         #                 effective_date=' '
    #         #                 permit_lic_exp_date=' '
    #         #                 lis=[site_id,location_address_string,licensing_county,company_phone,company_name,dba_name,company_subtype,mail_address_string,person_name,real_estate_owner,is_licensee_owner,permit_subtype,permit_lic_desc,permit_lic_no,permit_lic_status,permit_lic_eff_date,effective_date,permit_lic_exp_date,restrictions,'liquor_license','https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx','KY_Liquor_Licenses',Utils.getingestion_timestamp()]
    #         #                 f.write('|'.join(list([re.sub(r'\s+', ' ',data) if data else '' for data in lis]))+"\n")
    #                 if response_one.xpath("//td/span[text()={}]/following-sibling::a/@href".format(page)).extract_first():
    #                     print('=============>',response_one.xpath("//td/span[text()={}]/following-sibling::a/@href".format(page)).extract_first())
    #                     form_args_pagn = JavaScriptUtils.getValuesFromdoPost(response_one.xpath("//td/span[text()={}]/following-sibling::a/@href".format(page)).extract_first())
    #                     self.form_data['ctl00$toolkitScriptMaster']= 'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$resultsUpdatePanel|'+form_args_pagn['__EVENTTARGET']
    #                     self.form_data['ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$searchButton']=''
    #                     self.form_data['__VIEWSTATE1']=str(response_one.xpath('//*[@id="__VIEWSTATE1"]/@value').extract_first())
    #                     # self.form_data['ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollLeft']=str(response_one.xpath('//*[@id="ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollLeft"]/@value').extract_first()),
    #                     # self.form_data['ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollTop']= str(response_one.xpath('//*[@id="ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollTop"]/@value').extract_first())
    #                     self.form_data['__EVENTTARGET']=form_args_pagn['__EVENTTARGET']
                        
    #                     form_data={
    #                          'ctl00$toolkitScriptMaster': 'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$resultsUpdatePanel|ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$resultsGrid$ctl14$ctl01',
    #                         'ctl00_toolkitScriptMaster_HiddenField': ';;AjaxControlToolkit, Version=4.1.50401.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:5da0a7c3-4bf6-4d4c-b5d5-f0137ca3a302:de1feab2:f2c8e708:720a52bf:f9cec9bc:589eaa30:698129cf:7a92f56c;',
    #                         '__LASTFOCUS': '',
    #                         'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollLeft': '0',
    #                         'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollTop': '629',
    #                         'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxSiteID':' ',
    #                         'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxAdrOfPrememsis': '',
    #                         'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxLicenseeOrDbaName': '',
    #                         'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxCity': '',
    #                         'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxLicenseNumber': '',
    #                         'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxZipCode': '',
    #                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlLicenseType': '',
    #                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlCountyCode': '',
    #                         'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlBusinessType': 'Agent / Solicitor',
    #                         'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '1',
    #                        ' __EVENTTARGET': 'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$resultsGrid$ctl14$ctl01',
    #                         '__EVENTARGUMENT': '',
    #                         '__VIEWSTATE1': str(response_one.xpath('//*[@id="__VIEWSTATE1"]/@value').extract_first()),
    #                         '__VIEWSTATE':''
    #                     }
    #                     print(json.dumps(self.form_data,indent=3))
    #                     res_req = self.s.post(res.url, data=form_data,headers=self.header)
    #                     driver = BeautifulSoup(res_req.text, 'html5lib')
    #                     response_one= HtmlResponse('https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx', body=str.encode(driver.prettify()))
    #                     page+=1
    #                 else:
    #                     page=1
    #                     break

    #                     # f.write('|'.join(list(data_dic.values())))
    #                     # yield self.save_csv(response,data_dic).load_item()

    # # 