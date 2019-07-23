# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-24 10:43:25
TICKET NUMBER -AI_1429
@author: ait
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1429.items import MeSosSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
from inline_requests import inline_requests
from scrapy.shell import inspect_response
import datetime
import re
import os
import datetime


class MeSosSpider(CommonSpider):
    name = '1429_me_sos'
    allowed_domains = ['informe.org']
    start_urls = ['https://icrs.informe.org/nei-sos-icrs/ICRS?MainPage=x']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1429_SOS_ME_CurationReady'),
        'JIRA_ID':'AI_1429',
        'COOKIES_ENABLED':True,
        'DOWNLOAD_DELAY':5,
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('me_sos'),
        'TOP_HEADER':{                        'company_name': 'Entity/Legal Name',
                         'company_subtype': 'Filing Type/Mark Type',
                         'creation_date': 'Filing Date',
                         'dba_name': 'Other Names',
                         'entity_id': 'Charter Number/Mark Number',
                         'inactive_date': 'Expiration Date',
                         'jurisdiction': 'Jurisdiction',
                         'location_address_string': 'Address in Maine',
                         'mail_address_string': 'Other Mailing Address',
                         'mixed_name': 'Registered Agent/Contact/Owner name',
                         'mixed_subtype': 'Agent/ Contact/Owner Title',
                         'name type': 'Name Type',
                         'non_profit_indicator': '',
                         'permit_type': 'permit_type',
                         'person_address_string': 'Agent/Contact/Owner Address',
                         'status': 'Status'},
        'FIELDS_TO_EXPORT':[                        
                        'company_name',
                        'entity_id',
                        'name type',
                        'dba_name',
                        'company_subtype',
                        'non_profit_indicator',
                        'location_address_string',
                         'mail_address_string',
                         'status',
                          'creation_date',
                         'jurisdiction',          
                         'inactive_date',
                         'mixed_subtype',
                         'mixed_name',
                         'person_address_string',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp',
                           
                        ],
        'NULL_HEADERS':['name type', 'jurisdiction']
        }

    def parse(self, response):
        date=[str(i) for i in range(2019,1924-1,-1)]
        # number=[(str(i).zfill(4)) for i in range(9999,1-1,-1)]
        number=[(str(i).zfill(4)) for i in range(int(self.start),int(self.end),1)]
        zz=[' B', 'CP',  ' D',  'DC', 'DP',  ' I',  ' L', 'LP', 'LN', ' M', ' N' , 'ND', 'RC', 'RD', 'RI', 'RN', 'RP','RR',  ' A', 'CR', ' F', 'FC', 'FP', 'LF', 'LR', 'NF', 'NR', 'PR', ' R']
        self.data=[]
        for i in date:
            for j in number:
                for k in zz:
                    self.data.append(i+j+k)

        print("__________________________length_____________________")

       # self.data=['20131017 D','20191017 D','20191117 D','20191007 D','20191017 D','19750763 D','19910195 D','20180017 M','20131035DC','19070015 D','20064103DC','20000025LF']
        # self.data=['20180017 M']
        print(len(self.data))
        print((self.data))
        print(len(self.data))
        if self.data:
            self.datum=self.data.pop(0)
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            print("search element: ",self.datum)
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


            self.formdata1={
                    "WAISqueryString": "",
                    "number": self.datum,
                    "search": "Click Here to Search",
                    "search": "search"
            }
            self.header={
                    "Host": "icrs.informe.org",
                    "Origin": "https://icrs.informe.org",
                    "Referer": "https://icrs.informe.org/nei-sos-icrs/ICRS",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36"

            }
            yield scrapy.FormRequest.from_response(response,url='https://icrs.informe.org/nei-sos-icrs/ICRS',headers=self.header,formdata=self.formdata1,callback=self.parse_next,dont_filter=True)
    @inline_requests
    def parse_next(self,response):
        
        self.remove_tag=lambda data:re.sub(r'\s+', ' ',data).replace('[\n\t\r"]','').strip() if data else ''
        dic={}
        table=response.xpath("//table[1]//tr[4]/td/font/b/text()").extract()
        if table:pass
        else:
            now = datetime.datetime.now()
            print(now)
            with open(os.path.dirname(os.path.realpath(__file__))+'/nodata.txt','a') as f:
                f.write(self.datum)
                f.write('\n')

        if len(table) >2:
            dic['company_name']=response.xpath("//tr//b[contains(text(),'Legal Name')]/following::tr[1]/td[1]/text()").extract_first()
            dic['entity_id']=response.xpath("//tr//b[contains(text(),'Legal Name')]/following::tr[1]/td[2]/text()").extract_first()
            dic['company_subtype']=response.xpath("//tr//b[contains(text(),'Legal Name')]/following::tr[1]/td[3]/text()").extract_first()
            dic['status']=response.xpath("//tr//b[contains(text(),'Legal Name')]/following::tr[1]/td[4]/text()").extract_first()
            if dic['company_name']:pass
            else:
                dic['company_name']=response.xpath("//tr//b[contains(text(),'Registered Name')]/following::tr[1]/td[1]/text()").extract_first()
                dic['entity_id']=response.xpath("//tr//b[contains(text(),'Registered Name')]/following::tr[1]/td[2]/text()").extract_first()
                dic['company_subtype']=response.xpath("//tr//b[contains(text(),'Registered Name')]/following::tr[1]/td[3]/text()").extract_first()
                dic['status']=response.xpath("//tr//b[contains(text(),'Registered Name')]/following::tr[1]/td[4]/text()").extract_first()
            # print()
            if dic['company_name']:pass
            else:
                dic['company_name']=response.xpath("//tr//b[contains(text(),'Reserved Name')]/following::tr[1]/td[1]/text()").extract_first()
                dic['entity_id']=response.xpath("//tr//b[contains(text(),'Reserved Name')]/following::tr[1]/td[2]/text()").extract_first()
                dic['company_subtype']=response.xpath("//tr//b[contains(text(),'Reserved Name')]/following::tr[1]/td[3]/text()").extract_first()
                dic['status']=response.xpath("//tr//b[contains(text(),'Reserved Name')]/following::tr[1]/td[4]/text()").extract_first()
            # print()
            dic['creation_date']=response.xpath("//tr//b[contains(text(),'Filing Date')]/following::td/text()").extract()[0].replace('N/A','')
            dic['inactive_date']=response.xpath("//tr//b[contains(text(),'Filing Date')]/following::td/text()").extract()[1].replace("N/A",'')
            dic['jurisdiction']=response.xpath("//tr//b[contains(text(),'Filing Date')]/following::td/text()").extract()[2]
            # if 
            if 'NONPROFIT' in dic['company_subtype']:
                dic['non_profit_indicator']='Yes'
            else:
                dic['non_profit_indicator']=''
            dic['mixed_subtype']=''
            if response.xpath("//tr//b[contains(text(),'No Clerk/Registered Agent on file -- Contact Name ')]/following::tr[1]/td[1]/text()").extract_first():
                name='No Clerk/Registered Agent on file -- Contact Name '

            elif response.xpath("//tr//b[contains(text(),'Clerk/Registered Agent ')]/following::tr[1]/td[1]/text()").extract_first():
                name='Clerk/Registered Agent '
                mixed_subtype='Agent'
            elif response.xpath("//tr//b[contains(text(),'Owner Name')]/following::tr[1]/td[1]/text()").extract_first():
                name='Owner Name'
                mixed_subtype='Owner'
            elif response.xpath("//tr//b[contains(text(),'Contact Name')]/following::tr[1]/td[1]/text()").extract_first():
                name='Contact Name'
                mixed_subtype='Contact'

            data=response.xpath("//tr//b[contains(text(),'"+name+"')]/following::tr[1]/td[1]/text()").extract()
            i=0
            lis_spce=lambda data:[dat for dat in filter(lambda strs:strs.replace('[\n\t\r]','').strip(),data)]
            res_data=lis_spce(data)
            c=0
            for j in res_data:
                c+=1
                if j and re.search(r'\d+|EAST|STATION|NORTH|WEST|NORTH|SOUTH|ROAD|STREET|St.|Rd.| ME ',j):
                    break
                else:dic['address']='WA'
            dic['location_address_string']=re.sub(r'\s+', ' ',(','.join(res_data[c-1:])))
            dic['mail_address_string']=dic['location_address_string'].replace(' , ',', ').strip()
            dic['location_address_string']=dic['location_address_string'] if dic['location_address_string'] else 'WA'
            name=(res_data[:c-1])
            if name:
                if "PRESIDENT" in name[0]:
                    nam=name[0].split("PRESIDENT")
                    dic['mixed_name']=nam[0].replace(",",'')
                    dic['mixed_subtype']='PRESIDENT'
                else:
                    dic['mixed_subtype']=mixed_subtype
                    dic['mixed_name']=name[0]
                dic['person_address_string']=dic['location_address_string'].replace(' , ',', ').strip()
            else:
                dic['person_address_string']=''
                dic['mixed_name']=''
                dic['mixed_subtype']=''

            self.formdata2={
                    "WAISqueryString": dic['company_name'],
                    "number": '',
                    "search": "Click Here to Search",
                    "search": "search"
            }
            main_page=yield scrapy.FormRequest.from_response(response,url='https://icrs.informe.org/nei-sos-icrs/ICRS',headers=self.header,formdata=self.formdata2,method="POST",dont_filter=True)
            if dic['company_name']:
                if "'" in dic['company_name']:
                    search=dic['company_name'].split("'")[0]
                    dic['name type']=main_page.xpath("//td/font[contains(text(),'"+re.sub('\s+','',search)+"')]/ancestor::td/following-sibling::td[1]/font/text()").extract_first()
                else:
                    search=dic['company_name']
                    dic['name type']=main_page.xpath("//td/font[text()='"+search+"']/ancestor::td/following-sibling::td[1]/font/text()").extract_first()
            else:
                dic['name type']=''
            other_names=response.xpath("//tr//b[contains(text(),'Other Names')]/following::tr")[1:]
            dic['dba_name']=''
            if response.xpath("//tr//b[contains(text(),'Other Names')]/following::tr[2]//td[2]/text()").extract_first():
                for row in other_names:
                    if row.xpath(".//td[2]/text()").extract_first():
                        dic['dba_name']=row.xpath(".//td[1]/text()").extract_first()
                        yield self.save_csv(response,dic).load_item()
                    else:
                        dic['dba_name']=''                        
            else:yield self.save_csv(response,dic).load_item()

        elif len(table) == 2:
            dic['company_name']=response.xpath("//tr//b[contains(text(),'Mark Text')]/following::tr[1]/td[1]/text()").extract_first()
            dic['status']=response.xpath("//tr//b[contains(text(),'Mark Text')]/following::tr[1]/td[1]/text()").extract_first()
            dic['entity_id']=response.xpath("//tr//b[contains(text(),'Mark Number')]/following::tr[1]/td[1]/text()").extract_first()
            dic['creation_date']=response.xpath("//tr//b[contains(text(),'Mark Number')]/following::tr[1]/td[2]/text()").extract_first().replace("N/A",'')
            dic['inactive_date']=response.xpath("//tr//b[contains(text(),'Mark Number')]/following::tr[1]/td[3]/text()").extract_first().replace("N/A",'')
            dic['company_subtype']=response.xpath("//tr//b[contains(text(),'Mark Number')]/following::tr[1]/td[4]/text()").extract_first()
            dic['jurisdiction']=response.xpath("//tr//b[contains(text(),'Mark Number')]/following::tr[1]/td[6]/text()").extract_first()
            
            if dic['company_name']:pass
            else:
                dic['company_name']=response.xpath("//tr//b[contains(text(),'Registered Name')]/following::tr[1]/td[1]/text()").extract_first()
                dic['entity_id']=response.xpath("//tr//b[contains(text(),'Registered Name')]/following::tr[1]/td[2]/text()").extract_first()
                dic['company_subtype']=response.xpath("//tr//b[contains(text(),'Registered Name')]/following::tr[1]/td[3]/text()").extract_first()
                dic['status']=response.xpath("//tr//b[contains(text(),'Registered Name')]/following::tr[1]/td[4]/text()").extract_first()
            if dic['company_name']:pass
            else:
                dic['company_name']=response.xpath("//tr//b[contains(text(),'Reserved Name')]/following::tr[1]/td[1]/text()").extract_first()
                dic['entity_id']=response.xpath("//tr//b[contains(text(),'Reserved Name')]/following::tr[1]/td[2]/text()").extract_first()
                dic['company_subtype']=response.xpath("//tr//b[contains(text(),'Reserved Name')]/following::tr[1]/td[3]/text()").extract_first()
                dic['status']=response.xpath("//tr//b[contains(text(),'Reserved Name')]/following::tr[1]/td[4]/text()").extract_first()
            # print()
            if 'NONPROFIT' in dic['company_subtype']:
                dic['non_profit_indicator']='Yes'
            else:
                dic['non_profit_indicator']=''
            dic['mixed_subtype']=''
            if response.xpath("//tr//b[contains(text(),'No Clerk/Registered Agent on file -- Contact Name ')]/following::tr[1]/td[1]/text()").extract_first():
                name='No Clerk/Registered Agent on file -- Contact Name '
            elif response.xpath("//tr//b[contains(text(),'Clerk/Registered Agent ')]/following::tr[1]/td[1]/text()").extract_first():
                name='Clerk/Registered Agent '
                mixed_subtype='Agent'
            elif response.xpath("//tr//b[contains(text(),'Owner Name')]/following::tr[1]/td[1]/text()").extract_first():
                name='Owner Name'
                mixed_subtype="Owner"
            elif response.xpath("//tr//b[contains(text(),'Contact Name')]/following::tr[1]/td[1]/text()").extract_first():
                name='Contact Name'
                mixed_subtype="Contact"

            data=response.xpath("//tr//b[contains(text(),'"+ name+"')]/following::tr[1]/td[1]/text()").extract()
            i=0
            lis_spce=lambda data:[dat for dat in filter(lambda strs:strs.replace('[\n\t\r]','').strip(),data)]
            res_data=lis_spce(data)
            c=0
            for j in res_data:
                c+=1
                if j and re.search(r'\d+|East|Station|North|West|North|South|Road|Street|St.|Rd.',j):
                    break
                else:
                    dic['address']='WA'
            dic['location_address_string']=re.sub(r'\s+', ' ',(','.join(res_data[c-1:])))
            dic['location_address_string']=dic['location_address_string'] if dic['location_address_string'] else 'WA'
            names=(res_data[:c-1])
            print('\n\n',names,'\n')
                
            self.formdata3={
                    "WAISqueryString": dic['company_name'],
                    "number": '',
                    "search": "Click Here to Search",
                    "search": "search"
            }
            main_page=yield scrapy.FormRequest.from_response(response,url='https://icrs.informe.org/nei-sos-icrs/ICRS',headers=self.header,formdata=self.formdata3,method="POST",dont_filter=True)
            if dic['company_name']:
                if "'" in dic['company_name']:
                    search=dic['company_name'].split("'")[0]
                    dic['name type']=main_page.xpath("//td/font[contains(text(),'"+search+"')]/ancestor::td/following-sibling::td[1]/font/text()").extract_first()     
                else:
                    search=dic['company_name']
                    dic['name type']=main_page.xpath("//td/font[text()='"+search+"']/ancestor::td/following-sibling::td[1]/font/text()").extract_first()                
            else:
                dic['name type']=''
            if names:
                dic['mixed_subtype']=''
                for i in names:
                    print("namessssss: ",names)
                    if "MEMBER" in i:
                        print('memberrrrrr: '+i)
                        dic['mixed_name']=i.replace("MEMBER",'').replace(",",'')
                        dic['mixed_subtype']='MEMBER'
                    else:
                        dic['mixed_name']=i
                        dic['mixed_subtype']=mixed_subtype
                    dic['person_address_string']=dic['location_address_string']
                    yield self.save_csv(response,dic).load_item()
            else:
                yield self.save_csv(response,dic).load_item()
        else:
            table=response.xpath("//table[1]//table[1]//tr")[5:-2]
            for row in table:
                link='https://icrs.informe.org'+row.xpath(".//td[4]/font/a/@href").extract_first()
                yield scrapy.Request(url=link,dont_filter=True,callback=self.parse_next)

        if self.data:
            self.datum=self.data.pop(0)
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            print("search element: ",self.datum)
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            self.formdata={
                    "WAISqueryString": "",
                    "number": self.datum,
                    "search": "Click Here to Search",
                    "search": "search"
            }
            yield scrapy.FormRequest.from_response(response,url='https://icrs.informe.org/nei-sos-icrs/ICRS',headers=self.header,formdata=self.formdata,callback=self.parse_next,dont_filter=True)

    def save_csv(self,response,data_dic):
        il = ItemLoader(item=MeSosSpiderItem(),link_page=response)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'ME_SOS')
        il.add_value('permit_type', 'business_license')
        il.add_value('url', 'https://icrs.informe.org/nei-sos-icrs/ICRS?MainPage=x')
        for k in data_dic:
            il.add_value(k,(self.remove_tag(data_dic[k])))
        return il




        # # self.state['items_count'] = self.state.get('items_count', 0) + 1
        # il = ItemLoader(item=MeSosSpiderItem(),link_page=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        # #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        # il.add_value('sourceName', 'me_sos')
        # il.add_value('url', 'https://icrs.informe.org/nei-sos-icrs/ICRS?MainPage=x')
        # il.add_xpath('jurisdiction', '')
        # il.add_xpath('company_subtype', '')
        # il.add_xpath('inactive_date', '')
        # il.add_xpath('company_name', '')
        # il.add_xpath('name type', '')
        # il.add_xpath('location_address_string', '')
        # il.add_xpath('mail_address_string', '')
        # il.add_xpath('person_address_string', '')
        # il.add_xpath('non_profit_indicator', '')
        # il.add_xpath('permit_type', '')
        # il.add_xpath('dba_name', '')
        # il.add_xpath('entity_id', '')
        # il.add_xpath('mixed_name', '')
        # il.add_xpath('mixed_subtype', '')
        # il.add_xpath('status', '')
        # il.add_xpath('creation_date', '')
        # return il.load_item()
