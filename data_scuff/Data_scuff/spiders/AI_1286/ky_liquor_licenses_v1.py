'''
scrapy crawl 1286_ky_liquor_licenses_v1 
Created on 2019-Jun-28 03:57:52
TICKET NUMBER -AI_1286
@author: Muhil
'''
from bs4 import BeautifulSoup
import requests
from scrapy.http import HtmlResponse
import re
import json
import pandas as pd
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
    name = '1286_ky_liquor_licenses_v1'
    allowed_domains = ['ky.gov']
    start_urls = ['https://d3external.atlassian.net/browse/AI-1286']
    
    pro_urls = []
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('Licenses_Liquor_KY_CurationReady'),
        'JIRA_ID':'AI_1286',
        'DOWNLOAD_DELAY':.2,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        # 'HTTPCACHE_ENABLED':False,
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
        # inspect_response(response,self)
        # self.response=response
        self.s = requests.session()
        self.site_id=' '
        driver=BeautifulSoup(self.s.get('https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx').text,"html.parser")
        res = HtmlResponse('https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx', body=str.encode(driver.prettify()))
        req_url='https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx'        
        self.facility=res.xpath("//select[contains(@title,'Select Business Type')]/option/@value").extract()[1:]
        print("***********",self.facility)
        page=1
        module_dir = os.path.dirname(os.path.realpath(__file__))
        # print('===========',module_dir[:module_dir.find('Data_scuff')])
        path=module_dir[:module_dir.find('Data_scuff')]+'storage/AI_1286/AI-1286_Licenses_Liquor_KY_CurationReady_{}.csv'.format(str(Utils.getingestion_timestamp()).split('_')[0]+'_'+self.start+"_"+self.end)
        with open(path,'w') as f:
            f.write('|'.join(['Site Id','Site Address + City + State + Zip','Licensing County','Premises Phone','Licensee Name','DBA','Business Type','Mailing Address + City + State + Zip','PARTNERS','Real Estate Owner','Is Licensee Owner','License Type',' ','License Number','Status','Issue Date','Effective Date','Expire Date','Restrictions',' ',' ',' ',' '])+'\n')
            f.write('|'.join([' ',
                         'location_address_string',
                         ' ',
                         'company_phone',
                         'company_name',
                         'dba_name',
                         'company_subtype',
                         'mail_address_string',
                         'person_name',
                         ' ',
                         ' ',
                         'permit_subtype',
                         'permit_lic_desc',
                         'permit_lic_no',
                         'permit_lic_status',
                         'permit_lic_eff_date',
                         ' ',
                         'permit_lic_exp_date',
                         ' ',
                         'permit_type',
                         'url',
                         'sourceName',
                         'ingestion_timestamp'])+'\n')
            module_dir = os.path.dirname(os.path.realpath(__file__))
            path=module_dir+'/input.csv'
            
            all_data = pd.DataFrame()
            data=pd.read_csv(path,engine='python')
            # data.
            ids=[]
            for idss in data['24736']:
                ids.append(idss)
            ids.insert(0,'24736')
            ids=list(set(ids))
            print('=============>',len(ids))
            # input()
            # ids=['26075']
            
            # for tt in temp_ids:
            #     if tt in ids:
            #         ids.remove(tt)                
            # # exit()
            # ids=[4402,6035,21301,954,22860,24671,13878,9775,5803,9838,4112,10398,26601,4135,13242,19326,19326,10658,14879,22786,21643,27438,28209,28208,28160,28268,27955,28046,28210,16496,15855,15531,23368,23370,26278,27986,28301,27024,27001,27026,27118,27002,27005,27027,27200,27337,27418,27419,25751,25752,16174,14678,16360,25670,25668,27511,17653,27475,14743,23376,25603,24702,25713,23403,23305,27247,25549,25844,28159,25463,22578,23966,28221,22662,27519,27966,26568,22123,27192,20416,25523,27030,26445,25945,25145,27884,26284,21296,27248,27956,22306,18630,27961,16570,28152,20532,4132,16921,24605,25896,20499,25692,25937,27379,27032,26813,26814,15059,16119,18215]
            # ids=['12']
            for site_id in ids[int(self.start):int(self.end)]:
                self.site_id=str(site_id)
                self.form_data={
                      'ctl00$toolkitScriptMaster': 'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$UpdatePanel|ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$searchButton',
                        'ctl00_toolkitScriptMaster_HiddenField': ';;AjaxControlToolkit, Version=4.1.50401.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:5da0a7c3-4bf6-4d4c-b5d5-f0137ca3a302:de1feab2:f2c8e708:720a52bf:f9cec9bc:589eaa30:698129cf:7a92f56c',
                        '__LASTFOCUS':'', 
                        '__EVENTTARGET':'',
                        '__EVENTARGUMENT': '',
                        '__VIEWSTATE1': str(res.xpath('//*[@id="__VIEWSTATE1"]/@value').extract_first()),
                        '__VIEWSTATE': '',
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollLeft': '0',
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollTop': '300',
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxSiteID':self.site_id,
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxAdrOfPrememsis': '',
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxLicenseeOrDbaName': '',
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxCity': '',
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxLicenseNumber': '',
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxZipCode': '',
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlLicenseType': '',
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlCountyCode':'' ,
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlBusinessType': '',
                        'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '1',
                        # '__ASYNCPOST': 'true',
                        'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$searchButton':'Search'
                }
                self.header={
                    'Origin': 'https://dppweb.ky.gov',
                     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36', 
                     'Accept': '*/*' ,
                     'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' ,
                     'Referer': 'https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx'
                    }

                res_req = self.s.post(res.url, data=self.form_data,headers=self.header,verify=True)
                driver = BeautifulSoup(res_req.text, 'html5lib')
                response_one= HtmlResponse('https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx', body=str.encode(driver.prettify()))
                header={
                    'Origin': 'https://dppweb.ky.gov',
                     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36', 
                     'Accept': '*/*' ,
                     'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' ,
                     'Referer': 'https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx'
                    }
                # if page>1:
                # inspect_response(response_one,self)
                data_url='https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx'
                form_data={
                    'ctl00_toolkitScriptMaster_HiddenField': ';;AjaxControlToolkit, Version=4.1.50401.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:5da0a7c3-4bf6-4d4c-b5d5-f0137ca3a302:de1feab2:f2c8e708:720a52bf:f9cec9bc:589eaa30:698129cf:7a92f56c',
                    '__LASTFOCUS':'', 
                    '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$resultsGrid$ctl03$lnkbtnDetails',
                    '__EVENTARGUMENT': '',
                    '__VIEWSTATE1': str(response_one.xpath('//*[@id="__VIEWSTATE1"]/@value').extract_first()),
                    '__VIEWSTATE': '',
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollLeft': '0',
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl03$scrollTop': '352',
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxSiteID':self.site_id,  
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxAdrOfPrememsis': '',
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxLicenseeOrDbaName': '',
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxCity': '',
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxLicenseNumber': '',
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$tbxZipCode': '',
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlLicenseType': '',
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlCountyCode':'' ,
                    'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$ABCLicenseSearch1$ddlBusinessType': '',
                    'hiddenInputToUpdateATBuffer_CommonToolkitScripts': '1'
                    }
                # inspect_response(response_one,self)
                # print('=============>',response_one.xpath("//table[@class='searchResultsGrid']//tr//td[5]/text()").extract())
                # input()
                for links,b_type in zip(response_one.xpath("//a[contains(text(),'Details')]/@href").extract(),response_one.xpath("//table[@class='searchResultsGrid']//tr//td[5]/text()").extract()):
        #             link=links
                    form_args_pagn = JavaScriptUtils.getValuesFromdoPost(links)
                    # form_dataa = {
                    # '__EVENTTARGET': ,
                    form_data['ctl00$toolkitScriptMaster']= 'ctl00$ContentPlaceHolder1$PortalPageControl1$ctl12$resultsUpdatePanel|'+form_args_pagn['__EVENTTARGET']
                    form_data['__EVENTTARGET']= form_args_pagn['__EVENTTARGET']
                    # print(json.dumps(form_data,indent=3))
                    res_data = self.s.post(data_url, data=form_data,headers=header,verify=True)
                    driver = BeautifulSoup(res_data.text, 'html5lib')
                    res = HtmlResponse(data_url, body=str.encode(driver.prettify()))
                    # inspect_response(res,self)
                    data_dic={}
                    # datas=lambda res,data:res.xpath("//div[contains(text(),'{}')]/following-sibling::div/text()".format(data)).extract_first()
                    site_id=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteIdText']/text()").extract_first()
                    address=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteAddressLine1Text']/text()").extract_first()
                    city=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteCityText']/text()").extract_first()
                    state=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteStateText']/text()").extract_first()
                    loc_zip=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteZipCodeText']/text()").extract_first()
                    location_address=self.format__address_4(address,city,state,loc_zip)
                    location_address_string=location_address if location_address else 'KY'
                    licensing_county=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSiteLicensingCountyText']/text()").extract_first()
                    company_phone=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblSitePhoneNumberText']/text()").extract_first()
                    com_dba=self._getDBA(res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblLicenseeNameText']/text()").extract_first())
                    company_name=com_dba[0] if com_dba[0] else com_dba[1]
                    dba_names=[]
                    dba_names.append(res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblDbaNameText']/text()").extract_first())
                    if com_dba[1]:
                        dba_names.append(com_dba[1])
                    company_subtype=b_type
                   # company_subtype=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblBusinessTypeText']/text()").extract_first()
                    mail_address=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblMailingAddressText']/text()").extract_first()
                    mail_city=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblMailingCityText']/text()").extract_first()
                    mail_state=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblMailingStateText']/text()").extract_first()
                    mail_zip=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblMailingZipText']/text()").extract_first()
                    mail_address_string=self.format__address_4(mail_address,mail_city,mail_state,mail_zip)
                    mail_address_string=mail_address_string
                    # inspect_response(res,self)
                    person_names=[name for name in res.xpath("//span[contains(@id,'ctl12_lvOfficers')]/text()").extract() if name]
               
                    real_estate_owner=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblRealEstateOwnerText']/text()").extract_first()
                    is_licensee_owner=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblIsLicenseeOwnerText']/text()").extract_first()
                    # company_subtype=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblBusinessTypeText']/text()").extract_first()
                    # company_subtype=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblBusinessTypeText']/text()").extract_first()
                    restrictions=res.xpath("//span[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_lblRestrictions']//text()").extract_first()
                    print('================>',site_id)
                    person_name=''
                    dba_name=''
                    if len(dba_names)>1:
                        for dba in dba_names:
                            dba_name=dba
                            permit_subtype=' '
                            permit_lic_desc='Liquor License for '+company_name
                            permit_lic_eff_date= ' '
                            permit_lic_no=' '
                            permit_lic_status=' '
                            effective_date=' '
                            permit_lic_exp_date=' '
                            lis=[site_id,location_address_string,licensing_county,company_phone,company_name,dba_name,company_subtype,mail_address_string,person_name,real_estate_owner,is_licensee_owner,permit_subtype,permit_lic_desc,permit_lic_no,permit_lic_status,permit_lic_eff_date,effective_date,permit_lic_exp_date,restrictions,'liquor_license','https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx','KY_Liquor_Licenses',Utils.getingestion_timestamp()]
                            # print('|'.join(list([re.sub(r'\s+', ' ',data) if data else '' for data in lis])))
                            f.write('|'.join(list([re.sub(r'\s+', ' ',data).strip() if data else '' for data in lis]))+"\n")
                    elif dba_names:
                        dba_name=dba_names.pop()
                    if len(person_names)>1:
                        for person_name in person_names:
                            # person_name=person_names.pop(0)
                            permit_subtype=' '
                            permit_lic_desc='Liquor License for '+company_name
                            permit_lic_eff_date= ' '
                            permit_lic_no=' '
                            permit_lic_status=' '
                            effective_date=' '
                            permit_lic_exp_date=' '
                            lis=[site_id,location_address_string,licensing_county,company_phone,company_name,dba_name,company_subtype,mail_address_string,person_name,real_estate_owner,is_licensee_owner,permit_subtype,permit_lic_desc,permit_lic_no,permit_lic_status,permit_lic_eff_date,effective_date,permit_lic_exp_date,restrictions,'liquor_license','https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx','KY_Liquor_Licenses',Utils.getingestion_timestamp()]
                            # print('|'.join(list([re.sub(r'\s+', ' ',data) if data else '' for data in lis])))
                            f.write('|'.join(list([re.sub(r'\s+', ' ',data).strip() if data else '' for data in lis]))+"\n")
                                # if len(person_names)==1:
                                #     person_name=person_names.pop(0)
                            person_name=''
                                #     break
                    elif person_names:
                        person_name=person_names.pop(0)
                   
                    
                    if res.xpath("//table[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_gvLicenses']//tr"):
                        for tr in res.xpath("//table[@id='ctl00_ContentPlaceHolder1_PortalPageControl1_ctl12_gvLicenses']//tr"):
                            if tr.xpath('.//td[4]//text()').extract_first():
                                permit_lic_eff_date=tr.xpath('.//td[4]/span/text()').extract_first()
                                permit_subtype=tr.xpath('.//td[1]//text()').extract_first()
                                permit_lic_desc=tr.xpath('.//td[1]//text()').extract_first()
                                permit_lic_no=tr.xpath('.//td[2]//text()').extract_first()
                                permit_lic_status=tr.xpath('.//td[3]//text()').extract_first()
                                effective_date=tr.xpath('.//td[5]/span/text()').extract_first()
                                permit_lic_exp_date=tr.xpath('.//td[6]/span/text()').extract_first()
                                lis=[site_id,location_address_string,licensing_county,company_phone,company_name,dba_name,company_subtype,mail_address_string,person_name,real_estate_owner,is_licensee_owner,permit_subtype,permit_lic_desc,permit_lic_no,permit_lic_status,permit_lic_eff_date,effective_date,permit_lic_exp_date,restrictions,'liquor_license','https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx','KY_Liquor_Licenses',Utils.getingestion_timestamp()]
                                # print('|'.join(list([re.sub(r'\s+', ' ',data) if data else '' for data in lis])))

                                f.write('|'.join(list([re.sub(r'\s+', ' ',data).strip() if data else '' for data in lis]))+"\n")
                    else:
                        permit_subtype=' '
                        permit_lic_desc=' '
                        permit_lic_eff_date= ' '
                        permit_lic_no=' '
                        permit_lic_status=' '
                        effective_date=' '
                        permit_lic_exp_date=' '
                        lis=[site_id,location_address_string,licensing_county,company_phone,company_name,dba_name,company_subtype,mail_address_string,person_name,real_estate_owner,is_licensee_owner,permit_subtype,permit_lic_desc,permit_lic_no,permit_lic_status,permit_lic_eff_date,effective_date,permit_lic_exp_date,restrictions,'liquor_license','https://dppweb.ky.gov/ABCSTAR27161/page/License_Lookup/portal.aspx','KY_Liquor_Licenses',Utils.getingestion_timestamp()]
                        print('|'.join(list([re.sub(r'\s+', ' ',data) if data else '' for data in lis])))
                        f.write('|'.join(list([re.sub(r'\s+', ' ',data).strip() if data else '' for data in lis]))+"\n")
                   