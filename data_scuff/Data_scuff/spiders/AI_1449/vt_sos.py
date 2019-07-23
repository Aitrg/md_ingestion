'''
scrapy crawl 1449_vt_sos -a start=A -a end=B
Created on 2019-Jul-11 07:53:08
TICKET NUMBER -AI_1449
@author: Muhil
'''

import re
from scrapy.shell import inspect_response
from Data_scuff.utils.searchCriteria import SearchCriteria
import scrapy
from inline_requests import inline_requests
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_1449.items import VtSosSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils

class VtSosSpider(CommonSpider):
    name = '1449_vt_sos'
    allowed_domains = ['state.vt.us']
    start_urls = ['https://www.vtsosonline.com/online/BusinessInquire']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1449_SOS_VT_CurationReady'),
        'JIRA_ID':'AI_1449',
         'DOWNLOAD_DELAY':.4,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        'DOWNLOADER_MIDDLEWARES' : CustomSettings.appenDownloadMiddlewarevalues({
        'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
        'Data_scuff.middleware.common.TooManyRequestsRetryMiddleware': 543,
        }),
        # 'JOBDIR' : CustomSettings.getJobDirectory('vt_sos'),
        'TOP_HEADER':{                        'citizenship / domestic jurisdiction': 'Citizenship / Domestic Jurisdiction',
                         'company_name': 'Business Name',
                         'company_subtype': 'Business Type',
                         'creation_date': 'Date of Incorporation / Registration Date',
                         'dba_name': 'Trade Name',
                         'entity_id': 'Business ID',
                         'expiration date': 'Expiration Date',
                         'file #': 'File #',
                         'fiscal year month': 'Fiscal Year Month',
                         'inactive_date': 'Withdrawal Date/Dissolved Date',
                         'last report filed': 'Last Report Filed',
                         'location_address_string': 'Principal Office Business Address',
                         'mail_address_string': 'Principal Office Mailing Address',
                         'mixed_name': 'Name',
                         'mixed_subtype': 'Agent Type',
                         'naics': 'NAICS Code',
                         'naics sub code': 'NAICS sub code',
                         'next renewal period begins': 'Next Renewal Period Begins',
                         'non_profit_indicator': '',
                         'ops_desc': 'Business Description',
                         'permit_type': '',
                         'person_address_string': 'Physical Address',
                         'status': 'Business Status'},
        'FIELDS_TO_EXPORT':[                        'company_name',
                         'entity_id',
                         'company_subtype',
                         'non_profit_indicator',
                         'file #',
                         'status',
                         'expiration date',
                         'inactive_date',
                         'dba_name',
                         'creation_date',
                         'ops_desc',
                         'naics',
                         'naics sub code',
                         'location_address_string',
                         'citizenship / domestic jurisdiction',
                         'last report filed',
                         'fiscal year month',
                         'next renewal period begins',
                         'mail_address_string',
                         'mixed_name',
                         'mixed_subtype',
                         'person_address_string',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp'],
        'NULL_HEADERS':['file #', 'expiration date', 'naics sub code', 'citizenship / domestic jurisdiction', 'last report filed', 'fiscal year month', 'next renewal period begins']
        }

    def parse(self, response):
        # inspect_response(response,self)
        req_url='https://www.vtsosonline.com/online/BusinessInquire/BusinessSearch'
        # yield scrapy.Request(url=self.req_url,callback=self.parse_data, dont_filter=True)
        self.searchkeys=[]
        if self.start.isalpha() and self.end.isalpha():
            self.searchkeys = SearchCriteria.strRange(self.start,self.end)
        # self.searchkeys=['A TIME TO HEAL','THE VALSPAR CORPORATION',' Z PRESS, INC']
        search=self.searchkeys.pop(0)
        # search='Z PRESS, INC'
        self.header={
                'accept': '*/*',
                # 'accept-encoding': 'gzip, deflate, br',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'origin': 'https://www.vtsosonline.com',
                'referer': 'https://www.vtsosonline.com/online/BusinessInquire/BusinessSearch',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
            }
        self.__requestverificationtoken=response.xpath("//form/input[@name='__RequestVerificationToken']/@value").extract_first(),
        self.form_data={
            '__RequestVerificationToken':response.xpath("//form/input[@name='__RequestVerificationToken']/@value").extract_first(),
            'rbBusinessNameSearch': 'StartsWith',
            'rbBasicSearch': 'BusinessName',
            'txtBusinessName': search,
            'chkWithSimilarSoundingBusinessNames': 'false',
            'txtBusinessID': '',
            'txtFilingNumber': '',
            'txtOldBusinessName': '',
            'ddlBusinessType': '',
            'ddlNAICSCode': '',
            'domesticCorp': '',
            'ddlBusinessStatus': '',
            'txtCity': '',
            'txtZipCode': '',
            'txtAgentName': '',
            'chkWithSimilarSoundingAgentNames': 'false',
            'txtPrincipalName': '',
            'chkWithSimilarSoundingPrincipalNames': 'false',
            'btnSearch': 'Search',
            'hdnMessage': ''
        }
        yield scrapy.FormRequest(url=req_url,formdata=self.form_data,headers=self.header,callback=self.parse_data, dont_filter=True,meta={'search':search})
        self.links=[]
    @inline_requests
    def parse_data(self,response):
        print('---------------------->',response.meta['search'])
        # self.__requestverificationtoken=response.xpath("//form/input[@name='__RequestVerificationToken']/@value").extract_first()
        # inspect_response(response,self)
        for url in response.xpath("//td/a[contains(@href,'bus')]/@href").extract():
            self.links.append('https://www.vtsosonline.com'+url)
        page=response.xpath("//a[contains(text(),'Next')]/@href").extract_first()
        # print(self.links)
        
        # page=''
        if page:
            search=response.meta['search']
            page_num=page.replace('javascript:$xhtml.paging(','').replace(')','')
            print("page==========>",page_num)
           
            header={
                  'origin': 'https://www.vtsosonline.com' ,
                  'accept-encoding': 'gzip, deflate, br' ,
                  'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8' ,
                  'x-requested-with': 'XMLHttpRequest' ,
                  'pragma': 'no-cache' ,
                  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36' ,
                  'content-type': 'application/x-www-form-urlencoded; charset=UTF-8' ,
                  'accept': '*/*' ,
                  'cache-control': 'no-cache' ,
                  'authority': 'www.vtsosonline.com' ,
                  'referer': 'https://www.vtsosonline.com/online/BusinessInquire/BusinessSearch' ,
                  '__requestverificationtoken':self.__requestverificationtoken
            }
            
            form_data={
                'undefined': '',
                'sortby': 'BusinessID',
                'stype': 'a',
                'pidx': str(page_num)
            }
            # print(form_data)
            req_url='https://www.vtsosonline.com/online/BusinessInquire/BusinessSearchList'
            yield scrapy.FormRequest(url=req_url,formdata=form_data,headers=header,callback=self.parse_data, dont_filter=True,method='POST',meta={'search':search})
        else:
            if self.searchkeys:
                search=self.searchkeys.pop(0)
                data=yield scrapy.Request(url='https://www.vtsosonline.com/online/BusinessInquire/?isStartUpAction=False', dont_filter=True)
                self.__requestverificationtoken=data.xpath("//form/input[@name='__RequestVerificationToken']/@value").extract_first()
                self.form_data['__RequestVerificationToken']=self.__requestverificationtoken
                self.form_data['txtBusinessName']=search
                yield scrapy.FormRequest(url='https://www.vtsosonline.com/online/BusinessInquire/BusinessSearch',formdata=self.form_data,headers=self.header,callback=self.parse_data, dont_filter=True,meta={'search':search},method='POST')
            else:
                if self.links:
                    link=self.links.pop(0)
                    yield scrapy.Request(url=link,callback=self.main_data, dont_filter=True)
        
    @inline_requests
    def main_data(self,response):
        data_dic={}
        dba_name=[]
        company_name=self._getDBA(response.xpath("//td[contains(text(),'Business Name')]/following-sibling::td[1]/label/text()").extract_first())
        if company_name[1]:
            dba_name.append(company_name[1])
        if response.xpath("//*[contains(text(),'Trade Name Owned By')]/following::table[1]//tr/td[1]"):
            for dname in response.xpath("//*[contains(text(),'Trade Name Owned By')]/following::table[1]//tr/td[1]").extract()[1:]:
                dba_name.append(dname)
        data_dic['company_name']=company_name[0] if company_name[0] else company_name[1]
        # if not data_dic['company_name']:
        #     inspect_response(response,self)
        data_dic['entity_id']=response.xpath("//td[contains(text(),'Business ID')]/following-sibling::td[1]//text()").extract_first()
        data_dic['entity_id']=data_dic['entity_id'].zfill(7) if data_dic['entity_id'] and data_dic['entity_id'].isdigit() else  data_dic['entity_id']
        data_dic['company_subtype']=response.xpath("//td[contains(text(),'Business Type')]/following-sibling::td[1]//text()").extract_first()
        data_dic['non_profit_indicator']= str('Yes' if 'Non-profit' in data_dic['company_subtype'] else '') if data_dic['company_subtype'] else ''
        data_dic['file #']=response.xpath("//td[contains(text(),'File #')]/following-sibling::td[1]/label/text()").extract_first()
        data_dic['status']=response.xpath("//td[contains(text(),'Business Status')]/following-sibling::td[1]//text()").extract_first()
        data_dic['expiration date']=response.xpath("//td[contains(text(),'Expiration Date')]/following-sibling::td[1]//text()").extract_first()
        data_dic['next renewal period begins']=response.xpath("//td[contains(text(),'Next Renewal Period Begins')]/following-sibling::td[1]//text()").extract_first()

        data_dic['inactive_date']=response.xpath("//td[contains(text(),'Withdrawal Date')]/following-sibling::td[1]//text()").extract_first() if response.xpath("//td[contains(text(),'Withdrawal Date')]/following-sibling::td[1]//text()").extract_first() else response.xpath("//td[contains(text(),'Dissolved Date')]/following-sibling::td[1]//text()").extract_first()
        data_dic['creation_date']=response.xpath("//td[contains(text(),'Date of Incorporation / Registration Date')]/following-sibling::td[1]//text()").extract_first()

        address1=response.xpath("//td[contains(text(),'Principal Office Business Address')]/following-sibling::td[1]//text()").extract_first()
        address2=response.xpath("//td[contains(text(),'Designated Office Business Address')]/following-sibling::td[1]//text()").extract_first()
        address=address1 if len(address1.replace('NONE','') if address1 else '')>5 else address2
        data_dic['location_address_string']=self.address(address.replace('USA','').strip().strip(',')) if address and 'NONE' not in address else 'VT'
        data_dic['citizenship / domestic jurisdiction']=response.xpath("//td[contains(text(),'Citizenship / Domestic Jurisdiction')]/following-sibling::td[1]//text()").extract_first() if response.xpath("//td[contains(text(),'Citizenship / Domestic Jurisdiction')]/following-sibling::td[1]//text()").extract_first() else response.xpath("//td[contains(text(),'Citizenship')]/following-sibling::td[1]//text()").extract_first()
        last_repot=response.xpath("//td[contains(text(),'Last Report Filed')]/following-sibling::td[1]//text()").extract_first() if response.xpath("//td[contains(text(),'Last Report Filed')]/following-sibling::td[1]//text()").extract_first()  else ''
        data_dic['last report filed']=last_repot if '/' in last_repot else ''
        data_dic['fiscal year month']=response.xpath("//td[contains(text(),'Fiscal Year Month')]/following-sibling::td[1]//text()").extract_first()
        principal_address1=response.xpath("//td[contains(text(),'Principal Office Mailing Address')]/following-sibling::td[1]//text()").extract_first() 
        principal_address2=response.xpath("//td[contains(text(),'Designated Office Mailing Address')]/following-sibling::td[1]//text()").extract_first() 
        principal_address=principal_address1 if len(principal_address1.replace('NONE','') if principal_address1 else '')>5 else principal_address2
        data_dic['ops_desc'] = response.xpath("//td[contains(text(),'Business Description')]/following-sibling::td[1]//text()").extract_first()
        data_dic['mail_address_string']=self.address(principal_address.replace('USA','').strip().strip(',')) if principal_address and 'NONE' not in principal_address else ''
        data_dic['naics']=response.xpath("//td[contains(text(),'NAICS Code')]/following-sibling::td[1]//text()").extract_first()
        data_dic['naics sub code']=response.xpath("//td[contains(text(),'NAICS sub code')]/following-sibling::td[1]//text()").extract_first()

        mix_data=response.xpath("//th[contains(text(),'Registered Agent Information')]/ancestor::tr/following-sibling::tr")
        mixed_name=self._getDBA(mix_data.xpath(".//td[contains(text(),'Name')]/following-sibling::td//text()").extract_first())
        
        if mixed_name[1]:
            dba_name.append(mixed_name[1])
        dba_name=[dname for dname in dba_name if dname]
        # if len(dba_name)==1:
        #     data_dic['dba_name']=dba_name if dba_name else ''
        # else :
        # print('==================>',dba_name)
        flag=True
        if len(dba_name)>1:
            for dname in dba_name:
                data_dic['dba_name']=dname
                if data_dic['company_name'] or data_dic['entity_id']:
                    yield self.save_csv(response,data_dic).load_item()    
                flag=False
                data_dic['dba_name']=''
        elif dba_name:
            data_dic['dba_name']=dba_name.pop(0)
        data_dic['mixed_name']=mixed_name[0]
        mix_address=mix_data.xpath(".//td[contains(text(),'Physical Address')]/following-sibling::td//text()").extract_first()
        data_dic['person_address_string']=self.address(mix_address.replace('USA','').strip().strip(',')) if mix_address else ''
        data_dic['mixed_subtype']=mix_data.xpath(".//td[contains(text(),'Agent Type')]/following-sibling::td//text()").extract_first()
        if data_dic['mixed_name'] or data_dic.get('dba_name') or flag:
            if data_dic['company_name'] or data_dic['entity_id']:
                yield self.save_csv(response,data_dic).load_item()


        if response.xpath("//a[contains(@href,'Principals')]/@href").extract_first():
            print('https://www.vtsosonline.com'+response.xpath("//a[contains(@href,'Principals')]/@href").extract_first())
            mix_datas=yield scrapy.Request(url='https://www.vtsosonline.com'+response.xpath("//a[contains(@href,'Principals')]/@href").extract_first(),dont_filter=True)
            # inspect_response(mix_datas,self)
            if mix_datas.xpath("//span[contains(text(),'Total Number of Principals')]/following::table[1]//tr"):
                for tr in mix_datas.xpath("//span[contains(text(),'Total Number of Principals')]/following::table[1]//tr")[1:]:
                    mix_name_type,mixed_name,mixed_subtype,mix_address='','','',''
                    mix_name_type=tr.xpath('.//td[1]/text()').extract_first()
                    mixed_name =mix_name_type.split('/')[0] if '/' in mix_name_type else mix_name_type
                    mixed_subtype=mix_name_type.split('/')[1] if '/' in mix_name_type else ''
                    mix_address=tr.xpath(".//td[2]/text()").extract_first()
                    # print('==========================',mix_name_type,mix_address)
                    data_dic['person_address_string']=self.address(mix_address.replace('USA','').strip().strip(','))
                    data_dic['mixed_name']=self._getDBA(mixed_name)[0]
                    data_dic['dba_name']=self._getDBA(mixed_name)[1]
                    data_dic['mixed_subtype']=mixed_subtype
                    if data_dic['company_name'] or data_dic['entity_id']:
                        yield self.save_csv(response,data_dic).load_item()
        if self.links:
            link=self.links.pop(0)
            yield scrapy.Request(url=link,callback=self.main_data, dont_filter=True)
        # data_dic['company_subtype']=response.xpath("//td[contains(text(),'Business Type')]/following-sibling::td[1]//text()").extract_first()
        # data_dic['company_subtype']=response.xpath("//td[contains(text(),'Business Type')]/following-sibling::td[1]//text()").extract_first()
    def save_csv(self,response,data_dic):
        il = ItemLoader(item=VtSosSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda data:re.sub(r'\s+|NONE',' ',data) if data else '',lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'VT_SOS')
        il.add_value('url', 'https://www.sec.state.vt.us/corporationsbusiness-services/searches-databases/business-search.aspx')
        il.add_value('permit_type', 'business_license')
        # if 'Not Available' in data_dic['ops_desc']:
        if data_dic['ops_desc']:
            data_dic['ops_desc']='' if 'Not Available' in data_dic['ops_desc'] else data_dic['ops_desc']
        for k in data_dic:
            il.add_value(k,data_dic[k])
        return il
    def address(self,address):
        if re.search(r', \d+$|, \d+ - \d+$',address):
            st_code=re.search(r', \d+$|, \d+ - \d+$',address).group()
            # print(st_code.replace(', ',' '))
            address=re.sub(r', \d+$|, \d+ - \d+$',st_code.replace(', ',' '),address)
        return address
        # pass
        # il.add_xpath('company_name', '')
        # il.add_xpath('entity_id', '')
        # il.add_xpath('company_subtype', '')
        # il.add_xpath('non_profit_indicator', '')
        # il.add_xpath('file #', '')
        # il.add_xpath('status', '')
        # il.add_xpath('expiration date', '')
        # il.add_xpath('inactive_date', '')
        # il.add_xpath('dba_name', '')
        # il.add_xpath('creation_date', '')
        # il.add_xpath('ops_desc', '')
        # il.add_xpath('naics', '')
        # il.add_xpath('naics sub code', '')
        # il.add_xpath('last report filed', '')
        # il.add_xpath('fiscal year month', '')
        # il.add_xpath('', '')
        # il.add_xpath('mail_address_string', '')
        # il.add_xpath('mixed_name', '')
        # il.add_xpath('mixed_subtype', '')
        # il.add_xpath('person_address_string', '')
        # return il.load_item()