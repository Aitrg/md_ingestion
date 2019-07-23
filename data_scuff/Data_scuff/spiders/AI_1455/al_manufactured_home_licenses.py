# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-11 13:38:55
TICKET NUMBER -AI_1455
@author: Vel Systems
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1455.items import AlManufacturedHomeLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.searchCriteria import SearchCriteria
from inline_requests import inline_requests
from scrapy.shell import inspect_response
from Data_scuff.utils.utils import Utils
from dateutil.parser import parse
import scrapy
import re

class AlManufacturedHomeLicensesSpider(CommonSpider):
    name = 'al_manufactured_home_licenses'
    allowed_domains = ['alabama.gov']
    start_urls = ['http://www.amhc.alabama.gov/licensee.aspx']
    license_desc = []
    license_type = []
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1455_Licenses_Manufactured_Home'),
        'JIRA_ID':'AI_1455',
        'COOKIES_ENABLED':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('al_manufactured_home_licenses'),
        'TOP_HEADER':{'company_name': 'Name',
                         'company_phone': 'Phone',
                         'company_fax': 'Fax',
                         'county': 'County',
                         'dba_name': 'Dealer',
                         'dealer number': 'Dealer Number',
                         'location address': 'Location Address',
                         'location_address_string': 'Address',
                         'permit_lic_desc': '',
                         'permit_lic_no': 'License Number',
                         'permit_lic_status': 'Status',
                         'permit_lic_exp_date': 'Expires On/Date Closed',
                         'permit_subtype': 'Type',
                         'permit_type': ''},
        'FIELDS_TO_EXPORT':['permit_subtype',
                         'permit_lic_no',
                         'company_name',
                         'dba_name',
                         'dealer number',
                         'location_address_string',
                         'location address',
                         'company_phone',
                         'company_fax',
                         'county',
                         'permit_lic_status',
                         'permit_lic_exp_date',
                         'permit_lic_desc',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp'],
        'NULL_HEADERS':['dealer number', 'location address', 'county']
        }

    def dateFormat(self, date):
        if len(str(date)) > 6:
            dt = parse(str(date))
            date = dt.strftime('%m/%d/%Y')
        else:
            date = str(date)
        return date

    def parse(self, response):
        for i in range(1, len(response.xpath('//*[@id="ContentPlaceHolder1_ddlSearchType"]/option')) + int(1)):
            option_value = response.xpath('//*[@id="ContentPlaceHolder1_ddlSearchType"]/option['+str(i)+']/@value').extract_first()
            desc_value = response.xpath('//*[@id="ContentPlaceHolder1_ddlSearchType"]/option['+str(i)+']/text()').extract_first()
            self.license_type.append(option_value)
            self.license_desc.append(desc_value)
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse_on_crawl, dont_filter=True)

    def parse_on_crawl(self, response):
        self.lic_type = ''
        self.lic_desc = ''
        if len(self.license_type) > 0:
            self.lic_type = self.license_type.pop(0)
            self.lic_desc = self.license_desc.pop(0)
            print('_____________________________ Search Criteria : ', str(self.lic_desc))            
            form_data = {
                '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                'ctl00$ContentPlaceHolder1$ddlSearchType': self.lic_type,
                'ctl00$ContentPlaceHolder1$rblActive': '1,2,3,4',
                'ctl00$ContentPlaceHolder1$btnSubmit': 'Search'
            }
            yield scrapy.FormRequest(url=self.start_urls[0], method='POST', formdata=form_data, callback=self.parse_on_fetch, dont_filter=True)

    @inline_requests
    def parse_on_fetch(self, response):
        table_data = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr')
        for i in range(2, len(table_data)):
            _href = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/a[contains(@id, "ContentPlaceHolder1_gvResults_hlPrint_")]/@href').extract_first()
            _url = 'http://www.amhc.alabama.gov/'+ _href            
            url_response = yield scrapy.Request(url=_url, dont_filter=True)
            
            lic_no = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/b[contains(text(), "License Number:")]/following-sibling::text()').extract_first()
            if lic_no is not None and lic_no != '':
                permit_lic_no = re.sub(r'\s+', ' ', lic_no.strip())
            else:
                permit_lic_no = ''
    
            name = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/span[contains(@id, "ContentPlaceHolder1_gvResults_lblName_")]/text()').extract_first()
            if name is not None and name != '':
                cmpy_name = re.sub(r'\s+', ' ', self._getDBA(name)[0].strip())
                dba = re.sub(r'\s+', ' ', self._getDBA(name)[1].strip())
            else:
                cmpy_name = ''
                dba = ''

            deal_name = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/b[contains(text(), "Dealer:")]/following-sibling::text()').extract_first()
            if deal_name is not None and deal_name != '':
                dealer_name = re.sub(r'\s+', ' ', self._getDBA(deal_name)[0].strip())
                dealer_dba = re.sub(r'\s+', ' ', self._getDBA(deal_name)[1].strip())
            else:
                dealer_name = ''
                dealer_dba = ''

            d_name = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/b[contains(text(), "DBA:")]/following-sibling::text()').extract_first()
            if d_name is not None and d_name != '':
                db_name = re.sub(r'\s+', ' ', d_name.strip()).replace('dba ', '').replace('dba', '').replace('DBA ', '').replace('DBA', '').replace('Dba ', '').replace('Dba ', '')
            else:
                db_name = ''

            if dealer_name is not None and dealer_name != '':
                dba_name = re.sub(r'\s+', ' ', dealer_name.strip())
            elif dealer_dba is not None and dealer_dba != '':
                dba_name = re.sub(r'\s+', ' ', dealer_dba.strip())
            elif db_name is not None and db_name != '':
                dba_name = re.sub(r'\s+', ' ', db_name.strip())
            elif dba is not None and dba != '':
                dba_name = re.sub(r'\s+', ' ', dba.strip())
            else:
                dba_name = ''

            if self.lic_type == '2' or self.lic_type == '7':
                if cmpy_name is not None or cmpy_name != '':
                    name_split = cmpy_name.split(',')
                    if len(name_split) == 3:
                        f_name = re.sub(r'\s+', ' ', name_split[2].strip()).replace('-', '')
                        l_name = re.sub(r'\s+', ' ', name_split[0].strip()).replace('-', '')
                        suffix = re.sub(r'\s+', ' ', name_split[1].strip()).replace('-', '')
                        name_con = f_name + ' ' + l_name + ' ' + suffix
                        company_name = re.sub(r'\s+', ' ', name_con.strip())
                    elif len(name_split) == 2:
                        f_name = re.sub(r'\s+', ' ', name_split[1].strip()).replace('-', '')
                        l_name = re.sub(r'\s+', ' ', name_split[0].strip()).replace('-', '')
                        name_con = f_name + ' ' + l_name
                        company_name = re.sub(r'\s+', ' ', name_con.strip())
                    else:
                        company_name = re.sub(r'\s+', ' ', name_split[0].strip())
            else:
                if cmpy_name is not None or cmpy_name != '':
                    company_name = re.sub(r'\s+', ' ', cmpy_name.strip())
                else:
                    company_name = ''

            deal_num = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/b[contains(text(), "Dealer Number:")]/following-sibling::text()').extract_first()
            if deal_num is not None and deal_num != '':
                dealer_number = re.sub(r'\s+', ' ', deal_num.strip())
            else:
                dealer_number = ''

            ## Address
            addr_1 = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/b[contains(text(), "Address:")]/following-sibling::text()').extract_first()
            if addr_1 is not None and addr_1 != '':
                addr_line_1 = re.sub(r'\s+', ' ', addr_1.strip()).replace('NA', '').replace('Mail Renew Notice to: See file', '').replace('Missing File', '')
            else:
                addr_line_1 = ''
            citi = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/span[contains(@id, "ContentPlaceHolder1_gvResults_lblCity_")]/text()').extract_first()
            if citi is not None and citi != '':
                city = re.sub(r'\s+', ' ', citi.strip()).replace('Unknown', '')
            else:
                city = ''
            stat = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/span[contains(@id, "ContentPlaceHolder1_gvResults_lblState_")]/text()').extract_first()
            if stat is not None and stat != '':
                state = re.sub(r'\s+', ' ', stat.strip())
            else:
                state = 'AL'
            zip = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/span[contains(@id, "ContentPlaceHolder1_gvResults_Label1_")]/text()').extract_first()
            if zip is not None and zip != '' and zip != '0':
                zip_code = re.sub(r'\s+', ' ', zip.strip())
            else:
                zip_code = ''
            county = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/b[contains(text(), "County:")]/following-sibling::text()').extract_first()
            if county is not None and county != '':
                addr_county = re.sub(r'\s+', ' ', county.strip()).replace('Unknown', '')
            else:
                addr_county = ''    

            full_addr = (addr_line_1 if addr_line_1 else '') + (', ' if addr_line_1 and city else '') + (city if city else '') + (', ' if city and state else '') + (state if state else '') + (' ' if state and zip_code else '') + (zip_code if zip_code else '')
            if full_addr is not None and full_addr != '':
                addr_string = re.sub(r'\s+', ' ', full_addr.strip()).replace(',,', ',').replace('-', '')
            else:
                addr_string = 'AL'

            loc_addr = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/b[contains(text(), "Location Address:")]/following-sibling::text()').extract_first()
            if loc_addr is not None and loc_addr.strip() != '':
                loc_addr_str = (loc_addr + ', AL' if loc_addr else '') 
                loc_addr_string = re.sub(r'\s+', ' ', loc_addr_str.strip()).replace(',,', ',')
            else:
                loc_addr_string = ''

            phone = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/b[contains(text(), "Phone:")]/following-sibling::text()').extract_first()
            if phone is not None and phone != '':
                cmpy_phone = re.sub("[^0-9-)()]", " ", phone.strip())
            else:
                cmpy_phone = ''

            status = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr['+str(i)+']/td/div/span[contains(@id, "ContentPlaceHolder1_gvResults_lblStatus_")]/text()').extract_first()
            if status is not None and status != '':
                lic_status = re.sub(r'\s+', ' ', status.strip())
            else:
                lic_status = ''
            
            date = url_response.xpath('//*[contains(@id, "gvInstaller") or contains(@id,"gvManufacturer") or contains(@id,"gvModManufacturer") or contains(@id, "gvModularRetailer") or contains(@id, "gvRetailer") or contains(@id, "gvSalesPerson") or contains(@id, "gvThirdParty")]/tr[2]/td/span[contains(@id, "gvInstaller_lblInstallerDateExpires_0") or contains(@id, "gvManufacturer_lblManufacturerLicenseDate_0") or contains(@id, "gvModManufacturer_lblModManufacturerDateClosed_0") or contains(@id, "gvModularRetailer_lblModRetailerLicenseDateExpiration_0") or contains(@id, "gvRetailer_lblRetailerDateClosed_0") or contains(@id, "gvSalesPerson_lblSalesPersonDateClosed_0") or contains(@id, "gvThirdParty_Label6_0")]/text()').extract_first()
            if date is not None and date != '':                    
                exp_date = self.dateFormat(date)
            else:
                exp_date = ''

            if self.lic_type == '5' or self.lic_type == '6':
                fax = url_response.xpath('//*[contains(@id, "gvModularRetailer") or contains(@id, "gvRetailer")]/tr[2]/td/span[contains(@id, "gvModularRetailer_lblModRetailerFax_0") or contains(@id, "gvRetailer_lblRetailerFax_0")]/text()').extract_first()
                if fax is not None and fax != '':
                    cmpy_fax = re.sub("[^0-9-)()]", " ", fax.strip())
                else:
                    cmpy_fax = ''
            elif self.lic_type == '8':
                fax = url_response.xpath('//*[contains(@id, "gvThirdParty")]/tr[2]/td[9]/span[contains(@id, "gvThirdParty_lblEducation_")]/text()').extract_first()
                if fax is not None and fax != '':
                    cmpy_fax = re.sub("[^0-9-)()]", " ", fax.strip())
                else:
                    cmpy_fax = ''
            else:
                cmpy_fax = ''    

            ## Output Write
            print('_____________________ Permit Type : ', str(self.lic_desc), '_____________________ Permit Lic Number : ', permit_lic_no)                
            il = ItemLoader(item=AlManufacturedHomeLicensesSpiderItem(),response=response)
            il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
            il.add_value('sourceName', 'AL_Manufactured_Home_Licenses')
            il.add_value('url', 'http://www.amhc.alabama.gov/licensee.aspx')
            il.add_value('permit_subtype', str(self.lic_desc))
            il.add_value('permit_lic_no', permit_lic_no)
            il.add_value('company_name', company_name)
            il.add_value('dba_name', dba_name)
            il.add_value('dealer number', dealer_number)
            il.add_value('location_address_string', addr_string)
            il.add_value('location address', loc_addr_string)
            il.add_value('company_phone', cmpy_phone)
            il.add_value('company_fax', cmpy_fax)
            il.add_value('county', addr_county)
            il.add_value('permit_lic_status', lic_status)
            il.add_value('permit_lic_exp_date', exp_date)
            il.add_value('permit_lic_desc', str(self.lic_desc))
            il.add_value('permit_type', 'manufactured_home_license')
            yield il.load_item()

        ## Paging
        val = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr[7]/td/table/tr/td/span/text()').extract_first()
        nextPage = response.xpath('//*[@id="ContentPlaceHolder1_gvResults"]/tr[7]/td/table/tr/td[contains(span, "'+ str(val) + '")]/following-sibling::td/a/@href').extract_first()
        if nextPage:
            print('______________________________ CURRENT PAGE : ', int(val) + 1)
            event_argument = nextPage.replace("javascript:__doPostBack('ctl00$ContentPlaceHolder1$gvResults','", '').replace("')","")
            form_data={'__EVENTTARGET': 'ctl00$ContentPlaceHolder1$gvResults',
                '__EVENTARGUMENT': event_argument,
                '__VIEWSTATE': response.xpath("//*[@name='__VIEWSTATE']/@value").extract_first(),
                '__VIEWSTATEGENERATOR': response.xpath("//*[@name='__VIEWSTATEGENERATOR']/@value").extract_first(),
                '__EVENTVALIDATION': response.xpath("//*[@name='__EVENTVALIDATION']/@value").extract_first(),
                'ctl00$ContentPlaceHolder1$ddlSearchType': self.lic_type,
                'ctl00$ContentPlaceHolder1$rblActive': '1,2,3,4'
            }
            yield scrapy.FormRequest(url=self.start_urls[0], formdata = form_data, callback = self.parse_on_fetch, dont_filter=True)
        else:
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse_on_crawl, dont_filter=True)