# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-17 11:35:46
TICKET NUMBER -AI_1480
@author: ait_python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1480.items import AlForesterLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.shell import inspect_response
from scrapy import Request
import scrapy,tabula
import pandas as pd

class AlForesterLicensesSpider(CommonSpider):
    name = '1480_al_forester_licenses'
    allowed_domains = ['alabama.gov']
    start_urls = ['http://asbrf.alabama.gov/vs2k5/rosterofforesters.aspx?list=all']
    main_url = 'http://asbrf.alabama.gov/vs2k5/'
    pdf_url = 'http://asbrf.alabama.gov/pdfs/2019/ExpLic_1-3-2019.pdf'

    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1480_Licenses_Forester_AL_CurationReady'),
        'JIRA_ID':'AI_1480',
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        'JOBDIR' : CustomSettings.getJobDirectory('al_forester_licenses'),
        'TOP_HEADER':{                        
                         'company_email': 'E-mail',
                         'company_name': 'Company',
                         'company_phone': 'Phone',
                         'county': 'County',
                         'dba_name': '',
                         'location_address_string': 'Address',
                         'permit_lic_desc': '',
                         'permit_lic_no': 'Lic. Number',
                         'permit_type': '',
                         'person_name': 'Name',
                         'person_subtype': 'Title'
                         },
        'FIELDS_TO_EXPORT':[
            'person_name',
            'person_subtype', 
            'company_name', 
            'dba_name', 
            'location_address_string', 
            'county', 
            'permit_lic_no', 
            'company_phone',
            'company_email', 
            'permit_lic_desc', 
            'permit_type', 
            'url', 
            'sourceName',
            'ingestion_timestamp', 
            ],
        'NULL_HEADERS':['county']
        }

    def parse(self, response):
        # inspect_response(response,self)
        det_link = response.xpath('//*[@id="Label1"]/table//tr/td[4]/a/@href').extract()
        for x in det_link:
            req_link = self.main_url+x
            yield scrapy.Request(url = req_link,callback = self.parse_det,dont_filter = True)
        # print("____________________________________________________________Finishing of HTMl")
        # yield scrapy.Request(url = self.pdf_url,callback= self.pdf_scrap,dont_filter=True)
        # next(self.pdf_scrap)
        det_dic = {
            'user_name':'',
            'person_subtype':'',
            'comny_name':'',
            'dba_name':'',
            'person_addrs':'',
            'person_country':'',
            'person_lic_num':'',
            'person_phone_num':'',
            'person_mail_id':'',
            'permit_lic_desc':''
        }
        df = tabula.read_pdf('http://asbrf.alabama.gov/pdfs/2019/ExpLic_1-3-2019.pdf',
                        pages = 'all',
                        stream = True,
                        column = (208.463,360.698,499.163),
                        encoding= 'ISO-8859-1',
                        area = (193.928,69.998,678.173,506.813),
                        guess = False,
                        pandas_options= {'header':None}
                        )
        for _, row in df.iterrows():
            b=row.tolist()
            det_dic['user_name'] = b[0]
            det_dic['person_addrs'] = b[2]
            if not det_dic['person_addrs'] or det_dic['person_addrs'] is None or det_dic['person_addrs'] == ',':
                det_dic['person_addrs'] = "AL"
            else:
                det_dic['person_addrs'] = b[2]
            det_dic['person_lic_num'] = b[1]
            det_dic['comny_name'] = b[0]
            det_dic['permit_lic_desc'] = "Forester License for "+str(b[0])
            det_dic['dba_name'] = self._getDBA(det_dic['user_name'])[1]
            yield self.save_to_csv(response,**det_dic).load_item()
    
    def parse_det(self, response):
        # inspect_response(response,self)
        det_dic = {}
        user_name = response.xpath('//*[@id="Label1"]/table//tr[1]/td[2]/big/b/text()').extract()
        det_dic['user_name'] = self.val_return(user_name)
        person_subtype = response.xpath('//*[@id="Label1"]/table//tr[2]/td[2]/text()').extract()
        det_dic['person_subtype'] = self.val_return(person_subtype)
        comny_name = response.xpath('//*[@id="Label1"]/table//tr[3]/td[2]/text()').extract()
        det_dic['comny_name'] = self.val_return(comny_name)
        person_addrs = response.xpath('//*[@id="Label1"]/table//tr[4]/td[2]/text()').extract()
        person_addr_1 = self.val_return(person_addrs).replace('\xa0'," ")
        if person_addr_1=='' or person_addr_1 ==None or person_addr_1 == " " or person_addr_1.strip() ==",":
            det_dic['person_addrs'] = "AL"
        else:
            det_dic['person_addrs'] = person_addr_1
        person_country = response.xpath('//*[@id="Label1"]/table//tr[5]/td[2]/text()').extract()
        det_dic['person_country'] = self.val_return(person_country)
        person_lic_num = response.xpath('//*[@id="Label1"]/table//tr[6]/td[2]/text()').extract()
        det_dic['person_lic_num'] = self.val_return(person_lic_num)
        person_phone_num = response.xpath('//*[@id="Label1"]/table//tr[7]/td[2]/text()').extract()
        det_dic['person_phone_num'] = self.val_return(person_phone_num).replace('\xa0','')
        person_mail_id = response.xpath('//*[@id="Label1"]/table//tr[8]/td[2]/a/text()').extract()
        det_dic['person_mail_id'] = self.val_return(person_mail_id)
        det_dic['permit_lic_desc'] = "Forester License for "+det_dic['comny_name']
        det_dic['dba_name'] = self._getDBA(det_dic['user_name'])[1]
        yield self.save_to_csv(response,**det_dic).load_item()

    def val_return(self,val):
        a = ''.join(val)
        if a:
            return a
        else:
            return ''
 
    def save_to_csv(self,response,**det_dic):
        il = ItemLoader(item=AlForesterLicensesSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'AL_Forester_Licenses')
        il.add_value('url', 'http://asbrf.alabama.gov/vs2k5/rosterofforesters.aspx')
        il.add_value('permit_type', 'forester_license')
       # il.add_value('location_address_string', "AL")
        il.add_value('location_address_string', str(det_dic['person_addrs']))
        il.add_value('county', str(det_dic['person_country']))
        il.add_value('company_email', det_dic['person_mail_id'])
        il.add_value('person_subtype', det_dic['person_subtype'])
        il.add_value('permit_lic_no', det_dic['person_lic_num'])
        il.add_value('person_name', det_dic['user_name'])
        il.add_value('permit_lic_desc', det_dic['permit_lic_desc'])
        il.add_value('dba_name', det_dic['dba_name'])
        il.add_value('company_name', det_dic['comny_name'])
        il.add_value('company_phone', det_dic['person_phone_num'])
        return il