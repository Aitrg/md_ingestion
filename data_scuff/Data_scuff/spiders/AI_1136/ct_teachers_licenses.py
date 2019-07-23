# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-10 12:07:28
TICKET NUMBER -AI_1136
@author: velsystems
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1136.items import CtTeachersLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.searchCriteria import SearchCriteria
from inline_requests import inline_requests
from scrapy.shell import inspect_response
from Data_scuff.utils.utils import Utils
from dateutil.parser import parse
import scrapy
import re

class CtTeachersLicensesSpider(CommonSpider):
    name = 'ct_teachers_licenses'
    allowed_domains = ['ct.gov']
    start_urls = ['http://sdeportal.ct.gov/CECSFOI/FOILookup.aspx']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1136_Licenses_Teachers_CT_CurationReady'),
        'JIRA_ID':'AI_1136',
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': 0.5,
        'JOBDIR' : CustomSettings.getJobDirectory('ct_teachers_licenses'),
        'TOP_HEADER':{'dis trict': 'District',
                         'endorsements held': 'Endorsements Held',
                         'endorsements held.1': 'Endorsements Held.1',
                         'location_address_string': '',
                         'permit_lic_desc': '',
                         'permit_lic_eff_date': 'Valid From',
                         'permit_lic_exp_date': 'Valid To',
                         'permit_lic_status': 'Status',
                         'permit_subtype': 'Certificate Type',
                         'permit_type': '',
                         'person_name': 'Name'},
        'FIELDS_TO_EXPORT':['person_name',
                         'dis trict',
                         'endorsements held',
                         'endorsements held.1',
                         'permit_subtype',
                         'permit_lic_status',
                         'location_address_string',
                         'permit_lic_eff_date',
                         'permit_lic_exp_date',
                         'permit_lic_desc',
                         'permit_type',
                         'url',
                         'sourceName',
                         'ingestion_timestamp'],
        'NULL_HEADERS':['dis trict', 'endorsements held', 'endorsements held.1']
        }
    
    page_no = 1
    last_name = ''
    searchkeys = []
    searchkeys1 = []
    page_set_list = []
    
    def __init__(self, start=None, end=None,startnum=None,endnum=None,is_server=None, proxyserver=None, *a, **kw):
        super(CtTeachersLicensesSpider, self).__init__(start,end, proxyserver=None,*a, **kw)
        if start and startnum:
            self.searchkeys = SearchCriteria.numberRange(startnum,endnum,1) + SearchCriteria.strRange(start,end)
        elif start:
            self.searchkeys = SearchCriteria.strRange(start,end)
        elif startnum:
            self.searchkeys = SearchCriteria.numberRange(startnum,endnum,1)
    
    def parse(self, response):
        if len(self.searchkeys) > 0:
            self.page_no = 1
            self.last_name = self.searchkeys.pop(0)
            self.searchkeys1 = SearchCriteria.strRange('aa','zz')
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse_search, dont_filter=True)

    def parse_search(self, response):
        if len(self.searchkeys1) > 0:
            first_name = self.searchkeys1.pop(0)
            form_data = {
                'ctl00$ScriptManager1': 'ctl00$ScriptManager1|ctl00$ContentPlaceHolder1$btnGo',
                'ctl00_ScriptManager1_HiddenField': response.xpath('//*[@id="ctl00_ScriptManager1_HiddenField"]/@value').extract_first(),
                '__EVENTTARGET': '',
                '__EVENTARGUMENT': '',
                '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                '__SCROLLPOSITIONX': '0',
                '__SCROLLPOSITIONY': '0',
                '__VIEWSTATEENCRYPTED': '',
                '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                'ctl00$ContentPlaceHolder1$txtLastName': str(self.last_name),
                'ctl00$ContentPlaceHolder1$tbwetxtLastName_ClientState': '',
                'ctl00$ContentPlaceHolder1$txtFirstName': str(first_name),
                'ctl00$ContentPlaceHolder1$tbwetxtFirstName_ClientState': '',
                'ctl00$ContentPlaceHolder1$drpDistrict': '0',
                'ctl00$ContentPlaceHolder1$txtEmailRqd': 'test@gmail.com',
                'ctl00$ContentPlaceHolder1$tbwetxtEmailRqd_ClientState': '',
                'ctl00$ContentPlaceHolder1$gdv_NameAndDistrict$ctl18$ddlPageGroups': '1',
                'ctl00$ContentPlaceHolder1$txtFocus': '',
                'ctl00$ContentPlaceHolder1$btnGo': 'Search'
            }
            yield scrapy.FormRequest(url=self.start_urls[0], method="POST", formdata=form_data, callback=self.scrape_details, meta={'first_name': str(first_name)}, dont_filter=True)
        else:
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)

    @inline_requests
    def scrape_details(self, response):
        data_val = response.meta
        first_name = data_val['first_name']
        page_set = response.xpath('//*[contains(@id, "_ddlPageGroups")]/option')
        for p in range(1, len(page_set) + 1):
            val = response.xpath('//*[contains(@id, "_ddlPageGroups")]/option['+str(p)+']/@value').extract_first()
            self.page_set_list.append(val)

        print('----------------------------------------------------------------')
        print('---------------------------------------------------------------- Pageno: ', self.page_no, ' LastName: ', self.last_name, ' FirstName: ', first_name)

        data_rows = response.xpath('//*[@id="ctl00_ContentPlaceHolder1_gdv_NameAndDistrict"]/tr[@class != "GridPagerClass"]')
        for i in range(2, len(data_rows) + 1):
            href = response.xpath('//*[@id="ctl00_ContentPlaceHolder1_gdv_NameAndDistrict"]/tr['+str(i)+']/td[5]/a/@href').extract_first()
            event_arg = href.replace("javascript:__doPostBack('ctl00$ContentPlaceHolder1$gdv_NameAndDistrict','", '').replace("')", "").strip()
            select_frm_data = {
                'ctl00$ScriptManager1': 'ctl00$ContentPlaceHolder1$upnlFindEdResult|ctl00$ContentPlaceHolder1$gdv_NameAndDistrict',
                'ctl00_ScriptManager1_HiddenField': response.xpath('//*[@id="ctl00_ScriptManager1_HiddenField"]/@value').extract_first(),
                '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$gdv_NameAndDistrict',
                '__EVENTARGUMENT': str(event_arg),
                '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                '__SCROLLPOSITIONX': '0',
                '__SCROLLPOSITIONY': '0',
                '__VIEWSTATEENCRYPTED': '',
                '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                'ctl00$ContentPlaceHolder1$txtLastName': str(self.last_name),
                'ctl00$ContentPlaceHolder1$tbwetxtLastName_ClientState': '',
                'ctl00$ContentPlaceHolder1$txtFirstName': str(first_name),
                'ctl00$ContentPlaceHolder1$tbwetxtFirstName_ClientState': '',
                'ctl00$ContentPlaceHolder1$drpDistrict': '0',
                'ctl00$ContentPlaceHolder1$txtEmailRqd': 'test@gmail.com',
                'ctl00$ContentPlaceHolder1$tbwetxtEmailRqd_ClientState': '',
                'ctl00$ContentPlaceHolder1$txtFocus': '',
                '__LASTFOCUS': '' 
            }
            detail_response = yield scrapy.FormRequest(url=self.start_urls[0], method="POST", formdata=select_frm_data, dont_filter=True)

            detail_rows = detail_response.xpath('//*[@id="ctl00_ContentPlaceHolder1_gv_FOISearchList"]/tr')
            for j in range(2, len(detail_rows) + 1):
                name = detail_response.xpath('//*[@id="ctl00_ContentPlaceHolder1_gv_FOISearchList"]/tr['+str(j)+']/td[1]/text()').extract_first()
                if name is not None:
                    if ',' in name:
                        data_val['person_name'] = name.split(',')[1] + ' ' + name.split(',')[0]
                    else:
                        data_val['person_name'] = name
                    data_val['permit_license_desc'] = 'Teacher License for ' + data_val['person_name']
                else:
                    data_val['person_name'] = ''
                    data_val['permit_license_desc'] = 'Teacher License'
                district = detail_response.xpath('//*[@id="ctl00_ContentPlaceHolder1_gv_FOISearchList"]/tr['+str(j)+']/td[2]/text()').extract_first()
                if district is not None and district != '--':
                    data_val['district'] = district.strip()
                else:
                    data_val['district'] = ''
                data_val['certificate_type'] = detail_response.xpath('//*[@id="ctl00_ContentPlaceHolder1_gv_FOISearchList"]/tr['+str(j)+']/td[4]/text()').extract_first()
                data_val['status'] = detail_response.xpath('//*[@id="ctl00_ContentPlaceHolder1_gv_FOISearchList"]/tr['+str(j)+']/td[5]/text()').extract_first()
                valid_from = detail_response.xpath('//*[@id="ctl00_ContentPlaceHolder1_gv_FOISearchList"]/tr['+str(j)+']/td[6]/text()').extract_first()
                data_val['valid_from'] = self.format_date(valid_from)
                valid_to = detail_response.xpath('//*[@id="ctl00_ContentPlaceHolder1_gv_FOISearchList"]/tr['+str(j)+']/td[7]/text()').extract_first()
                data_val['valid_to'] = self.format_date(valid_to)
                endorsements_held_list = detail_response.xpath('//*[@id="ctl00_ContentPlaceHolder1_gv_FOISearchList"]/tr['+str(j)+']/td[3]/text()').extract()
                if len(endorsements_held_list) > 0:
                    for k in range(0, len(endorsements_held_list)):
                        endorsements = endorsements_held_list[k].split('-')
                        if len(endorsements) == 2:
                            data_val['endorsements_desc'] = endorsements[1]
                            data_val['endorsements_number'] = endorsements[0]
                        elif len(endorsements) > 2:
                            data_val['endorsements_number'] = endorsements[0]
                            data_val['endorsements_desc'] = endorsements_held_list[k].replace(endorsements[0]+'-', '').strip()
                        yield self.save_to_csv(response, **data_val)
                else:
                    data_val['endorsements_desc'] = ''
                    data_val['endorsements_number'] = ''
                    yield self.save_to_csv(response, **data_val)
        
        next_page = response.xpath('//*[contains(@id, "_imgNextPage")]').extract_first()
        if next_page:
            ddlPageGroups = '1'
            self.page_no = self.page_no + 1
            if self.page_no < 7:
                if len(self.page_set_list) > 0:
                    ddlPageGroups = self.page_set_list[0]
            elif self.page_no > 6 and self.page_no < 11:
                if len(self.page_set_list) > 1:
                    ddlPageGroups = self.page_set_list[1]
            elif self.page_no > 10 and self.page_no < 16:
                if len(self.page_set_list) > 2:
                    ddlPageGroups = self.page_set_list[2]

            if len(self.page_set_list) > 0:
                next_paging_form_data = {
                    'ctl00$ScriptManager1': 'ctl00$ContentPlaceHolder1$upnlFindEdResult|ctl00$ContentPlaceHolder1$gdv_NameAndDistrict$ctl18$imgNextPage',
                    'ctl00_ScriptManager1_HiddenField': response.xpath('//*[@id="ctl00_ScriptManager1_HiddenField"]/@value').extract_first(),
                    '__EVENTTARGET': '',
                    '__EVENTARGUMENT': '',
                    '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                    '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                    '__SCROLLPOSITIONX': response.xpath('//*[@id="__SCROLLPOSITIONX"]/@value').extract_first(),
                    '__SCROLLPOSITIONY': response.xpath('//*[@id="__SCROLLPOSITIONY"]/@value').extract_first(),
                    '__VIEWSTATEENCRYPTED': '',
                    '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                    'ctl00$ContentPlaceHolder1$txtLastName': str(self.last_name),
                    'ctl00$ContentPlaceHolder1$tbwetxtLastName_ClientState': '',
                    'ctl00$ContentPlaceHolder1$txtFirstName': str(first_name),
                    'ctl00$ContentPlaceHolder1$tbwetxtFirstName_ClientState': '',
                    'ctl00$ContentPlaceHolder1$drpDistrict': '0',
                    'ctl00$ContentPlaceHolder1$txtEmailRqd': 'test@gmail.com',
                    'ctl00$ContentPlaceHolder1$tbwetxtEmailRqd_ClientState': '',
                    'ctl00$ContentPlaceHolder1$gdv_NameAndDistrict$ctl18$ddlPageGroups': str(ddlPageGroups),
                    'ctl00$ContentPlaceHolder1$txtFocus': '',
                    '__LASTFOCUS': '',
                    'ctl00$ContentPlaceHolder1$gdv_NameAndDistrict$ctl18$imgNextPage.x': '2',
                    'ctl00$ContentPlaceHolder1$gdv_NameAndDistrict$ctl18$imgNextPage.y': '4'
                }
                yield scrapy.FormRequest(url=self.start_urls[0], method='POST', formdata=next_paging_form_data, callback=self.scrape_details, meta={'first_name': str(first_name)}, dont_filter=True)
            else:
                cur_page = response.xpath('//*[contains(@id, "_pagerInnerTable")]/tr/td[@class="pageCurrentNumber"]/a/text()').extract_first()
                if cur_page is not None:
                    next_page = response.xpath('//*[contains(@id, "_pagerInnerTable")]/tr/td[@class="pageCurrentNumber"]/a[contains(text(), "'+ str(cur_page) + '")]/ancestor::td[1]/following-sibling::td[1]/a[@title]/@href').extract_first()
                    if next_page:
                        event_argument = next_page.replace("javascript:__doPostBack('", "").replace("','')", "")
                        paging_form_data = {
                            'ctl00$ScriptManager1': 'ctl00$ContentPlaceHolder1$upnlFindEdResult|' + str(event_argument),
                            'ctl00_ScriptManager1_HiddenField': response.xpath('//*[@id="ctl00_ScriptManager1_HiddenField"]/@value').extract_first(),
                            '__EVENTTARGET': str(event_argument),
                            '__EVENTARGUMENT': '',
                            '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                            '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                            '__SCROLLPOSITIONX': response.xpath('//*[@id="__SCROLLPOSITIONX"]/@value').extract_first(),
                            '__SCROLLPOSITIONY': response.xpath('//*[@id="__SCROLLPOSITIONY"]/@value').extract_first(),
                            '__VIEWSTATEENCRYPTED': '',
                            '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                            'ctl00$ContentPlaceHolder1$txtLastName': str(self.last_name),
                            'ctl00$ContentPlaceHolder1$tbwetxtLastName_ClientState': '',
                            'ctl00$ContentPlaceHolder1$txtFirstName': str(first_name),
                            'ctl00$ContentPlaceHolder1$tbwetxtFirstName_ClientState': '',
                            'ctl00$ContentPlaceHolder1$drpDistrict': '0',
                            'ctl00$ContentPlaceHolder1$txtEmailRqd': 'test@gmail.com',
                            'ctl00$ContentPlaceHolder1$tbwetxtEmailRqd_ClientState': '',
                            'ctl00$ContentPlaceHolder1$txtFocus': ''
                        }
                        yield scrapy.FormRequest(url=self.start_urls[0], method='POST', formdata=paging_form_data, callback=self.scrape_details, meta={'first_name': str(first_name)}, dont_filter=True)
        else:
            self.page_no = 1
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse_search, dont_filter=True)
    
    def save_to_csv(self, response ,**meta):
        il = ItemLoader(item=CtTeachersLicensesSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('sourceName', 'CT_Teachers_Licenses')
        il.add_value('url', 'http://sdeportal.ct.gov/CECSFOI/FOILookup.aspx')
        il.add_value('person_name', meta['person_name'])
        il.add_value('district ', meta['district'])
        il.add_value('endorsements held', meta['endorsements_number'])
        il.add_value('endorsements held.1', meta['endorsements_desc'])
        il.add_value('permit_subtype', meta['certificate_type'])
        il.add_value('permit_lic_status', meta['status'])
        il.add_value('location_address_string', 'CT')
        il.add_value('permit_lic_eff_date', meta['valid_from'])
        il.add_value('permit_lic_exp_date', meta['valid_to'])
        il.add_value('permit_lic_desc', meta['permit_license_desc'])
        il.add_value('permit_type', 'teacher_license')
        return il.load_item()