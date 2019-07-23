'''
scrapy crawl 1417_hi_sos -a start=AAA -a end=ZZZ
scrapy crawl 1417_hi_sos -a start=0 -a end=9


Created on 2019-Jun-26 04:36:27
TICKET NUMBER -AI_1417
@author: muhil
'''
from Data_scuff.utils.searchCriteria import SearchCriteria
import os
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import scrapy
from scrapy.shell import inspect_response
from inline_requests import inline_requests
from Data_scuff.spiders.AI_1417.items import HiSosSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import re
class HiSosSpider(CommonSpider):
    name = '1417_hi_sos'
    allowed_domains = ['ehawaii.gov']
    start_urls = ['https://hbe.ehawaii.gov/documents/search.html']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1417_SOS_HI_CurationReady'),
        'JIRA_ID':'AI_1417',
         'DOWNLOAD_DELAY':.2,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('hi_sos'),
        'TOP_HEADER':{                        'category': 'Category',
                         'company_name': 'MASTER NAME',
                         'company_subtype': 'Business Type',
                         'creation_date': 'Creation Date',
                         'dba_name': 'Trade Name',
                         'entity_id': 'File No',
                         'inactive_date': 'Expiration Date',
                         'location_address_string': 'Mailing Address',
                         'mixed_name': 'Registered Agent/Officers name',
                         'mixed_subtype': 'Registant/Agent/Officers/MEM',
                         'non_profit_indicator': '',
                         'ops_desc': 'Purpose',
                         'permit_type': '',
                         'person_address_string': 'Agent/Principal Information Address',
                         'place incorporated': 'Place Incorporated',
                         'record type': 'Record Type',
                         'status': 'Status',
                         'term/partner term': 'TERM/PARTNER TERM'},
        'FIELDS_TO_EXPORT':[
                         'company_name',
                        'entity_id',
                         'record type',
                         'company_subtype',
                         'non_profit_indicator',
                         'dba_name',
                        'location_address_string',
                         'status',
                         'creation_date',
                         'ops_desc',
                         'category',
                         'place incorporated',
                        'inactive_date',
                        'term/partner term',
                         'mixed_subtype',
                         'mixed_name',
                         'person_address_string',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp'
                         ],
        'NULL_HEADERS':['place incorporated', 'term/partner term', 'category', 'record type']
        }

    def parse(self, response):
        entityType=['COOPERATIVE','CORPORATION','LLC','PARTNERSHIP','TRADENAME']
        
        req_url='https://hbe.ehawaii.gov/documents/search.html'
        searchkeys=[]
        if self.start.isalpha() and self.end.isalpha():
            searchkeys = SearchCriteria.strRange(self.start,self.end)
        else:
            searchkeys = SearchCriteria.numberRange(self.start,self.end,1)
        self.search_lis=[]
        for key in searchkeys:
            for type in entityType:
                dic={}
                dic['query']=key
                dic['entityType']=type
                self.search_lis.append(dic.copy())
                dic.clear()
        # self.search_lis=[{'query':'PIRANHA GROUP LLC','entityType':'ALL'}]
        search=self.search_lis.pop(0)
        formdata={
            'beginsWith': 'true',
            'query': search['query'],
            'recordType': 'ALL',
            'status': 'ALL',
            'entityType': search['entityType'],
            'page': '0'
        }
        yield scrapy.FormRequest(url=req_url,formdata=formdata, callback=self.parse_data, dont_filter=True,meta={'search':search},errback=self.err_parse_data)
        self.links=[]
    @inline_requests
    def parse_data(self,response):
        
        print('===========',response.meta.get('search'),'=============',response.xpath("//div[@id='search_results_table_info']/text()").extract_first())
        if response.xpath("//div[@id='search_results_table_info']/text()").extract_first():
            for link,typ in zip(response.xpath('//tr//td/a/@href').extract(),response.xpath('//td[2]/text()').extract()):
                self.links.append('https://hbe.ehawaii.gov'+link+"###"+typ)
        if self.links:
            url=self.links.pop(0)
            yield scrapy.Request(url=url.split('###')[0],dont_filter=True,callback=self.main_data,meta={'type':url.split('###')[1]})        
        else:
            if self.search_lis:
                search=self.search_lis.pop(0)
                formdata={
                    'beginsWith': 'true',
                    'query': search['query'],
                    'recordType': 'ALL',
                    'status': 'ALL',
                    'entityType': search['entityType'],
                    'page': '0'
                }
                req_url='https://hbe.ehawaii.gov/documents/search.html'
                yield scrapy.FormRequest(url=req_url,formdata=formdata, callback=self.parse_data, dont_filter=True,meta={'search':search},errback=self.err_main_data)
        # else:
    def err_parse_data(self,response):
        if self.search_lis:
            search=self.search_lis.pop(0)
            formdata={
                'beginsWith': 'true',
                'query': search['query'],
                'recordType': 'ALL',
                'status': 'ALL',
                'entityType': search['entityType'],
                'page': '0'
            }
            req_url='https://hbe.ehawaii.gov/documents/search.html'
            yield scrapy.FormRequest(url=req_url,formdata=formdata, callback=self.parse_data, dont_filter=True,meta={'search':search})
    def main_data(self,response):
        print("remain==============>",len(self.links))
        office_dic={
            'AC' : 'Assistant Comptroller/Controller',
            'AS' : 'Assistant Secretary',
            'AT'  :'Assistant Treasurer',
            'C'   :'Chairman',
            'CEO': 'CEO',
            'CFO' :'CFO',
            'CO' : 'Comptroller/Controller',
            'D'  : 'Director',
            'EV':'Executive Vice President',
            'G':'General Partner',
            'L' :  'Limited Partner',
            'MGR' :'Manager',
            'P'  : 'President',
            'S'  : 'Secretary',
            'SRV' :'Senior Vice-President',
            'T'   :'Treasurer',
            'R'   :'Trustee',
            'VC'  :'Vice-Chairman',
            'V'   :'Vice-President',
            '1V'  :'1st Vice President',
            '9V'  :'9th Vice President',
            'MEM' : 'Member'
        }
        data_dic={}
        dba_data=[]
        
        com_dba=self._getDBA(response.xpath("//dt[contains(text(),'MASTER NAME')]/following-sibling::dd[1]/text()").extract_first() if response.xpath("//dt[contains(text(),'MASTER NAME')]/following-sibling::dd[1]") else response.xpath("//dt[contains(text(),'Trade Name')]/following-sibling::dd[1]/text()").extract_first())

        data_dic['company_name']=com_dba[0]
        dba_data.append(com_dba[1])
        data_dic['entity_id']=response.xpath("//dt[contains(text(),'FILE NUMBER')]/following-sibling::dd[1]/text()").extract_first() if response.xpath("//dt[contains(text(),'FILE NUMBER')]/following-sibling::dd[1]") else response.xpath("//dt[contains(text(),'File Number')]/following-sibling::dd[1]/text()").extract_first()
        data_dic['record type']=response.meta['type']
        data_dic['company_subtype']=response.xpath("//dt[contains(text(),'BUSINESS TYPE')]/following-sibling::dd[1]/text()").extract_first() if response.xpath("//dt[contains(text(),'BUSINESS TYPE')]/following-sibling::dd[1]/text()").extract_first() else ''  
        dba_data.append(response.xpath("//dt[contains(text(),'Trade Name')]/following-sibling::dd[1]/text()").extract_first() if response.xpath("//dt[contains(text(),'Trade Name')]/following-sibling::dd[1]/text()").extract_first() else response.xpath("//h2[contains(text(),'Trade Names')]/following::table[1]//tr/td[1]//text()").extract_first())
        # data_dic['dba_name']=
        location_address=response.xpath("//dt[contains(text(),'MAILING ADDRESS')]/following-sibling::dd[1]/text()").extract() if response.xpath("//dt[contains(text(),'MAILING ADDRESS')]/following-sibling::dd[1]/text()").extract() else  response.xpath("//dt[contains(text(),'Mailing Address')]/following-sibling::dd[1]/text()").extract()
        location_address_string=''
        if location_address:
            location_address_string=str(' '.join(location_address)).replace('UNITED STATES','').replace('JAPAN','')
        data_dic['location_address_string']=location_address_string if len(re.sub(r'\s+',' ',location_address_string))>4 else 'HI'
        data_dic['company_subtype']=re.sub(r'\s+',' ',data_dic['company_subtype'])
        data_dic['non_profit_indicator']='Yes' if ('non profit' in  data_dic['company_subtype'] or 'not for profit' in  data_dic['company_subtype'] or 'Nonprofit' in data_dic['company_subtype']) else ' '
        print('================>',data_dic['non_profit_indicator']+"##")
        data_dic['status']=response.xpath("//dt[contains(text(),'STATUS')]/following-sibling::dd[1]/text()").extract_first() if response.xpath("//dt[contains(text(),'STATUS')]/following-sibling::dd[1]") else response.xpath("//dt[contains(text(),'Status')]/following-sibling::dd[1]/text()").extract_first()

        creation_date=response.xpath("//dt[contains(text(),'REGISTRATION DATE')]/following-sibling::dd[1]/text()").extract_first() if response.xpath("//dt[contains(text(),'REGISTRATION DATE')]/following-sibling::dd[1]") else response.xpath("//dt[contains(text(),'Registration Date')]/following-sibling::dd[1]/text()").extract_first()
        data_dic['creation_date']=self.format_date(creation_date)
        inactive_date=response.xpath("//dt[contains(text(),'Expiration Date')]/following-sibling::dd[1]/text()").extract_first()
        data_dic['inactive_date']=self.format_date(inactive_date) if inactive_date else ''
        data_dic['ops_desc']=response.xpath("//dt[contains(text(),'PURPOSE')]/following-sibling::dd[1]/text()").extract_first() if response.xpath("//dt[contains(text(),'PURPOSE')]/following-sibling::dd[1]") else response.xpath("//dt[contains(text(),'Purpose')]/following-sibling::dd[1]/text()").extract_first()
        data_dic['category']=response.xpath("//dt[contains(text(),'Category')]/following-sibling::dd[1]/text()").extract_first()
        data_dic['place incorporated']=response.xpath("//dt[contains(text(),'PLACE INCORPORATED')]/following-sibling::dd[1]/text()").extract_first()
        data_dic['term/partner term']=response.xpath("//dt[contains(text(),'TERM')]/following-sibling::dd[1]/text()").extract_first() if response.xpath("//dt[contains(text(),'TERM')]/following-sibling::dd[1]/text()").extract_first() else response.xpath("//dt[contains(text(),'PARTNER TERM')]/following-sibling::dd[1]/text()").extract_first()
        c=0
        dba_data = [dba for dba in dba_data if dba]
        if 'Internal server error' not in response.xpath('//h1/text()').extract_first():
            if dba_data and len(dba_data)>1:
                while len(dba_data)>1:
                    dba=dba_data.pop(0)
                    c+=1
                    data_dic['dba_name']=dba
                    yield self.save_csv(response,data_dic).load_item()

            if response.xpath("//dt[contains(text(),'AGENT NAME')]/following-sibling::dd[1]/text()").extract_first():
                c+=1
                mixed_dba=self._getDBA(self.name_format(response.xpath("//dt[contains(text(),'AGENT NAME')]/following-sibling::dd[1]/text()").extract_first()))
                if mixed_dba[1] and dba_data:
                    dba=dba_data.pop(0)
                    c+=1
                    data_dic['dba_name']=dba
                    yield self.save_csv(response,data_dic).load_item()

                data_dic['dba_name']=mixed_dba[1] if mixed_dba[1] else str(dba_data[0] if dba_data else '')
                il=self.save_csv(response,data_dic)
                il.add_value('mixed_name',mixed_dba[0])
                # il.add_value('')
                il.add_value('mixed_subtype','Agent')
                mix_address=response.xpath("//dt[contains(text(),'AGENT ADDRESS')]/following-sibling::dd[1]/text()").extract() if response.xpath("//dt[contains(text(),'AGENT ADDRESS')]/following-sibling::dd[1]/text()").extract() else response.xpath("//dt[contains(text(),'Agent Address')]/following-sibling::dd[1]/text()").extract()
                mix_address_string=''
                if mix_address:
                    mix_address_string=str(' '.join(mix_address)).replace('UNITED STATES','').replace('JAPAN','')
                # address=_first() else 'HI'
                il.add_value('person_address_string',mix_address_string if mix_address_string else 'HI')
                yield il.load_item()
            if response.xpath("//table[@id='officersTable']//tr"):
                for tr in response.xpath("//table[@id='officersTable']//tr"):
                    if tr.xpath(".//td[1]/text()").extract_first():
                        c+=1
                        mixed_dba=self._getDBA(self.name_format(tr.xpath(".//td[1]/text()").extract_first()))
                        if mixed_dba[1] and dba_data:
                            dba=dba_data.pop(0)
                            c+=1
                            data_dic['dba_name']=dba
                            yield self.save_csv(response,data_dic).load_item()
                        data_dic['dba_name']=mixed_dba[1] if mixed_dba[1] else str(dba_data[0] if dba_data else '')
                        il=self.save_csv(response,data_dic)
                        il.add_value('mixed_name',self.name_format(tr.xpath(".//td[1]/text()").extract_first()))
                        if tr.xpath(".//td[2]/text()").extract_first():
                            mixed_subtype=str(tr.xpath(".//td[2]/text()").extract_first().replace('*','')).split("/")  if '/' in  tr.xpath(".//td[2]/text()").extract_first() else tr.xpath(".//td[2]/text()").extract_first()
                            print('====================',mixed_subtype)
                            if isinstance(mixed_subtype, str):
                                il.add_value('mixed_subtype',office_dic.get(mixed_subtype,'') if office_dic.get(mixed_subtype,'') else mixed_subtype)
                            elif isinstance(mixed_subtype, list):
                                typs=[]
                                for tt in mixed_subtype:
                                    if tt:
                                        typs.append(office_dic.get(tt,tt))
                                il.add_value('mixed_subtype','/'.join(typs))
                        il.add_value('person_address_string','HI')
                        yield il.load_item()
            if c==0:
                yield self.save_csv(response,data_dic).load_item()
        else:
            module_dir = os.path.dirname(os.path.realpath(__file__))
            path=module_dir+'/readme.txt'
            with open(path,'a') as f:
                f.write(response.url+"\n")
        if self.links:
                url=self.links.pop(0)
                yield scrapy.Request(url=url.split('###')[0],dont_filter=True,callback=self.main_data,meta={'type':url.split('###')[1]})
        else:
            if self.search_lis:
                search=self.search_lis.pop(0)
                formdata={
                    'beginsWith': 'true',
                    'query': search['query'],
                    'recordType': 'ALL',
                    'status': 'ALL',
                    'entityType': search['entityType'],
                    'page': '0'
                }
                req_url='https://hbe.ehawaii.gov/documents/search.html'
                yield scrapy.FormRequest(url=req_url,formdata=formdata, callback=self.parse_data, dont_filter=True,meta={'search':search})
    def err_main_data(self,response):
        if self.links:
                url=self.links.pop(0)
                yield scrapy.Request(url=url.split('###')[0],dont_filter=True,callback=self.main_data,meta={'type':url.split('###')[1]})
        else:
            if self.search_lis:
                search=self.search_lis.pop(0)
                formdata={
                    'beginsWith': 'true',
                    'query': search['query'],
                    'recordType': 'ALL',
                    'status': 'ALL',
                    'entityType': search['entityType'],
                    'page': '0'
                }
                req_url='https://hbe.ehawaii.gov/documents/search.html'
                yield scrapy.FormRequest(url=req_url,formdata=formdata, callback=self.parse_data, dont_filter=True,meta={'search':search})
    def name_format(self,name):
        if name:
            if ',' in name:
                name_sp=name.split(',')
                name=name_sp[1]+" "+name_sp[0]
        return name
    def save_csv(self,response,data_dic):
        il = ItemLoader(item=HiSosSpiderItem(),response=response)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'HI_SOS')
        il.add_value('url', 'https://hbe.ehawaii.gov/documents/search.html')
        il.add_value('permit_type', 'business_license')
        for k in data_dic:
            il.add_value(k,data_dic[k])
        return il
    def save_url(self,link):
        module_dir = os.path.dirname(os.path.realpath(__file__))
        path=module_dir+'/link.txt'
        with open(path,'a') as f:
            f.write(link+"\n")