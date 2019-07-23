# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-17 10:55:42
TICKET NUMBER -AI_1401
@author: megha
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from bs4 import BeautifulSoup as soup
import requests
from Data_scuff.spiders.AI_1401.items import IlSwimmingFacilityLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
from scrapy.shell  import inspect_response
import re
class IlSwimmingFacilityLicensesSpider(CommonSpider):
    name = 'il_swimming_facility_licenses'
    allowed_domains = ['illinois.gov']
    start_urls = ['http://ehlicv5pub.illinois.gov/Clients/ILDOHENV/PUBLIC/Swimming_Verifications.aspx']
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1401_Licenses_Swimming_Facility_IL_CurationReady'),
        'JIRA_ID':'AI_1401',
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        'DOWNLOAD_DELAY':0.5,
        'TOP_HEADER':{   'aquatic features type': 'Aquatic Features Type',
                         'company_name': 'Facility Name',
                         'county': 'County',
                         'dba_name': '',
                         'location': 'Location',
                         'location_address_string': 'Facility Address',
                         'permit_lic_desc': '',
                         'permit_lic_status': 'License Status',
                         'permit_type': ''},
        'FIELDS_TO_EXPORT':['company_name','dba_name','location_address_string','county','aquatic features type','location','permit_lic_status','permit_lic_desc','permit_type','sourceName','url','ingestion_timestamp'],
        'NULL_HEADERS':['location', 'county', 'aquatic features type']
        }
    aqua_1_lis=[]
    aqua_2_lis=[]
    check=True
    key_val=''
    searchkeys=[]
    def parse(self,response):
        options=response.xpath('//*[@id="qCounty"]/option')[1:]
        for i in options:
            option=i.xpath('@value').extract_first()
            self.searchkeys.append(option)
        if self.check==True:
            self.key_val = self.searchkeys.pop(0)
            form_data={
                '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                '__VIEWSTATEGENERATOR': 'A31F86B4',
                '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                'ObjectTypeID': '2177',
                'ObjectID': '40',
                'qBusinessName': '',
                'qStreet': '',
                'qCity': '',
                'qCounty': str(self.key_val),
                'btnSearch': 'Search'}
            yield scrapy.FormRequest(url=self.start_urls[0], method='POST', callback=self.second_page_crawl, formdata=form_data, dont_filter=True,meta={'option':option})
    def second_page_crawl(self,response):
        c=str(response.text).replace('<br>','**').replace('</br>','**')
        page_content_b = soup(c,"lxml")
        table = page_content_b.find('table', id="dtgList")
        soup_row = table.findAll('tr')
        
        check=0
        meta=response.meta
        las_add_lis_1=''
        las_add_lis=[]
        common_path=response.xpath('//*[@id="dtgList"]//tr')[1:-2]
        for ind_val, iter_val in enumerate(common_path):
            facility_name=iter_val.xpath('td[1]/text()').extract_first()
            facility_name_1=self._getDBA(facility_name)[0]
            dba_name=self._getDBA(facility_name)[1]
            desc='Swimming Facility License for ' + facility_name_1
            mail_address_string=iter_val.xpath('td[2]/text()').extract()
            mail_add_lis=[]
            county=''
            check=3
            for mail_ind, mail_add_value in enumerate(mail_address_string):
                if mail_ind>=0:
                    if check==3:
                        mail_add_lis.append(mail_add_value)
                        mail_add_lis_1=', '.join(mail_add_lis).replace('  ',' ')
                        address= mail_add_lis_1[:mail_add_lis_1.rindex(", ")]if  mail_add_lis_1[:mail_add_lis_1.rindex(" ")].endswith(',') else mail_add_lis_1[:mail_add_lis_1.rindex(" ")]
                        county=mail_add_lis_1[mail_add_lis_1.rindex(" ")+1:]
                        adres=re.sub(r'(\d+)$', r'KY \1', address)
                    else:
                        county=mail_add_value
                else:
                    adres=''
                    county=''
            aqua_1_lis=[]
            aqua_2_lis=[]
            aq_location=''
            aq_feature=''
            aq_feat=''
            aq_loct=''
            aqua=''

            cols = soup_row[ind_val+1].findAll('td')
            if cols[2]:
                ere=str(cols[2]).split('**')
                for kre in ere:
                    clean_val=self.data_clean(kre)
                    if clean_val and len(clean_val)>1:
                        aq_feature=clean_val.split('-')[0]
                        aqua_1_lis.append(aq_feature)
                        aqua_1_lis=list(dict.fromkeys(aqua_1_lis))
                        aq_feat='; '.join(aqua_1_lis)
                        if aq_feat and len(aq_feat)>2:
                            if aq_feat[0] ==';':
                                aq_feat=aq_feat.replace(aq_feat[0],'')
                        aq_location=clean_val.split('-')[1]
                        aqua_2_lis.append(aq_location)
                        aqua_2_lis=list(dict.fromkeys(aqua_2_lis))
                        aq_loct='; '.join(aqua_2_lis)
                        if aq_loct and len(aq_loct)>2:
                            if aq_loct[0] ==';':
                                aq_loct=aq_loct.replace(aq_loct[0],'')
            status=iter_val.xpath('td[4]/text()').extract_first()
            data_pass={'location':aq_loct,'permit_lic_desc':desc,'company_name':facility_name_1,'county':county,'dba_name':dba_name,'location_address_string':adres,'permit_type':'pool_license','permit_lic_status':status,'aquatic_features_type':aq_feat}
            yield self.save_to_csv(response,**data_pass) 
        next_page_link=response.xpath("//td[@colspan='4']/span/following::a/@href").extract_first()
        next_page_=response.xpath("//td[@colspan='4']/span/following::a/text()").extract_first()
        if next_page_link and 'doPostBack(' in next_page_link:
            next_page=next_page_link.split("('")[1].split("',")[0]
            form_data_page={'__EVENTTARGET': next_page,
                '__EVENTARGUMENT': response.xpath('//*[@id="__EVENTARGUMENT"]/@value').extract_first(),
                '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                '__VIEWSTATEGENERATOR': 'A31F86B4',
                '__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                'ObjectTypeID': '2177',
                'ObjectID': '40',
                'qCounty': '5451'}
            yield scrapy.FormRequest(url="http://ehlicv5pub.illinois.gov/Clients/ILDOHENV/PUBLIC/Swimming_Verifications.aspx", callback = self.second_page_crawl, dont_filter=True, formdata=form_data_page,meta=meta)
        else:
            yield scrapy.Request(url=self.start_urls[0],callback=self.parse,dont_filter=True)
    def save_to_csv(self, response, **datum):
        il = ItemLoader(item=IlSwimmingFacilityLicensesSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('url', 'http://ehlicv5pub.illinois.gov/Clients/ILDOHENV/PUBLIC/Swimming_Verifications.aspx')
        il.add_value('sourceName', 'IL_Swimming_Facility_Licenses')
        il.add_value('location', datum['location'])
        il.add_value('permit_lic_desc', datum['permit_lic_desc'])
        il.add_value('company_name', datum['company_name'])
        il.add_value('county', datum['county'])
        il.add_value('dba_name', datum['dba_name'])
        il.add_value('location_address_string', datum['location_address_string'])
        il.add_value('permit_type', datum['permit_type'])
        il.add_value('permit_lic_status', datum['permit_lic_status'])
        il.add_value('aquatic features type', datum['aquatic_features_type'])
        return il.load_item()
    def data_clean(self, value):
        if value:
            try:
                clean_tags = re.compile('<.*?>')
                desc_list = re.sub(r'\s+', ' ', re.sub(clean_tags, '', value))
                desc_list_rep = desc_list.replace('&amp;', '&').replace('Home Phone:','').replace('Mobile Phone:','').replace('Fax:','').replace('Primary Phone:','')
                return desc_list_rep.strip()
            except:
                return ''
        else:
            return ''