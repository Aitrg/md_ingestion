# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-20 06:42:28
TICKET NUMBER -AI_1398
@author: ait
'''

from Data_scuff.extensions.feeds import  ExcelFeedSpider
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1398.items import IlAgricultureLicensesSpiderItem
from Data_scuff.spiders.__common import CustomSettings
from Data_scuff.utils.utils import Utils
from Data_scuff.spiders.__common import DataFormatterMixin,LookupDatareaderMixin
import scrapy
from inline_requests import inline_requests
from scrapy.shell import inspect_response
import datetime
import re


class IlAgricultureLicensesSpider(ExcelFeedSpider,DataFormatterMixin,LookupDatareaderMixin):
    name = '1398_il_agriculture_licenses'
    allowed_domains = ['illinois.gov']
    start_urls = ['https://www.agr.state.il.us/sharepoint/azlicenselist.php']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1398_Licenses_Agriculture_IL_CurationReady'),
        'JIRA_ID':'AI_1398',
        'COOKIES_ENABLED':True,
        'DOWNLOAD_DELAY':.2,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('il_agriculture_licenses'),
        'TOP_HEADER':{                        'company_fax': 'Fax',
                         'company_name': 'Name',
                         'company_phone': 'Phone',
                         'county': 'County',
                         'dba_name': '',
                         'location_address_String': 'Address',
                         'permit_lic_desc': '',
                         'permit_lic_exp_date': 'In compliance until/Expires',
                         'permit_subtype': 'Type',
                         'permit_type': ''},
        'FIELDS_TO_EXPORT':['permit_subtype', 'company_name','dba_name','location_address_String','company_phone','company_fax', 'county',  'permit_lic_exp_date', 'permit_lic_desc' ,'permit_type', 'sourceName',  'url', 'ingestion_timestamp' ],
        'NULL_HEADERS':['county']
        }
    def parse(self, response):
        self.links=response.xpath("//html//@href").extract()
        self.permit=response.xpath("/html//a/text()").extract()
        self.permit_subtype=self.permit.pop(0)

        self.link=self.links.pop(0)
        if self.links:
            if "http" not in self.link:self.link="https://www.agr.state.il.us/sharepoint/"+self.link
            else:self.link=self.link
            yield scrapy.Request(url=self.link,callback=self.parse_data,errback=self.errors,dont_filter=True,meta={"max_retry_times":0})   
    @inline_requests
    def parse_data(self,response):
        self.remove_tag=lambda data:re.sub(r'\s+', ' ',data).replace('[\n\t\r"]','').strip() if data else ''
        dic={}
        dic2={}
        if response.xpath("//input[@type='SUBMIT']").extract():
            if self.permit_subtype == "AGRICULTURE COOPS":
                formdata={
                        "name":"",    
                        "sc" : ""
                }
                header={
                        "Host": "www.agr.state.il.us",
                        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Referer": "https://www.agr.state.il.us/sharepoint/agcoop.php"

                }
                submit_page=yield scrapy.FormRequest(url='https://www.agr.state.il.us/sharepoint/coopresults.php',method="POST",dont_filter=True,formdata=formdata,headers=header)
                rows=submit_page.xpath("//table[@width='100%']/tr")
                for row in rows:
                    dic['company']=row.xpath(".//td/font/b/text()").extract_first()
                    if self.remove_tag(dic['company']):
                        dic['company_name']=self._getDBA(dic['company'])[0]
                        dic['dba_name']=self._getDBA(dic['company'])[1]
                    else:
                        dic['company_name']=""
                        dic['dba_name']=""
                    dic['permit_lic_exp_date']=row.xpath(".//td/b/text()").extract_first()
                    dic['permit_subtype']=self.permit_subtype
                    dic['location_address_String']=', '.join(row.xpath(".//td/font/text()").extract())
                    dic['company_phone']=row.xpath('.//td[contains(text(),"Phone:")]/following-sibling::td/text()').extract_first()
                    dic['company_fax']=row.xpath('.//tr/td[contains(text(),"Fax:")]/following-sibling::td/text()').extract_first()
                    dic['county']=row.xpath(".//td/p/text()").extract_first().replace("State of incorporation:",'')
                    dic['permit_lic_desc']="Agriculture License for "+dic['company_name'] if dic['company_name'] else "Agriculture License"
                    yield self.save_csv(response,dic).load_item()
            else :
                csrf_token=response.xpath("//input[@name='csrf_token']/@value").extract_first()
                formdata={
                        "name":"",    
                        "sc" : "",
                        "csrf_token":csrf_token,
                        "nr":"10000"

                }
                header={
                        "Host": "www.agr.state.il.us",
                        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Referer": "https://www.agr.state.il.us/sharepoint/warehouselookup.php"
                        }
                submit_page1=yield scrapy.FormRequest(url='https://www.agr.state.il.us/sharepoint/grain.php',method="POST",dont_filter=True,formdata=formdata,headers=header)
                rows=submit_page1.xpath("//table[2]//tr")[:-1:]
                for row in rows:
                    dic['bold_Data']=row.xpath(".//td[1]/b/text()").extract_first().strip()
                    dic["next_Data"]=row.xpath(".//td[1]/text()").extract_first()
                    dic['bold_Data_split']=dic['bold_Data'].split(" ")
                    if dic['bold_Data_split'][0] in dic['next_Data']:
                        if re.match(r'\d+|East|Station|North|West|North|South|Road|Street|St.|Rd.',dic['next_Data']):
                            dic['location_address_String']=', '.join(row.xpath(".//td[1]/text()").extract())
                            company_nam=[dic['bold_Data']]
                        else:
                            dic['location_address_String']=', '.join(row.xpath(".//td[1]/text()").extract()[1:])
                            company_nam=[dic['bold_Data'],dic['next_Data']]
                    else:
                        if re.match(r'\d+|East|Station|North|West|North|South|Road|Street|St.|Rd.',dic['next_Data']):
                            dic['location_address_String']=', '.join(row.xpath(".//td[1]/text()").extract())
                            company_nam=[dic['bold_Data']]
                        else:
                            dic['location_address']=(row.xpath(".//td[1]/text()").extract())
                            r=dic['location_address'].pop(0)
                            dic['location_address'].insert(-1,r)
                            dic['location_address_String']=", ".join(dic['location_address']).lower()
                            company_nam=[dic['bold_Data']]
                    for i in company_nam:
                        dic['company']=i
                        if (dic['company']):
                            dic['company_name']=self._getDBA(dic['company'])[0]
                            dic['dba_name']=self._getDBA(dic['company'])[1]
                        else:
                            dic['company_name']=""
                            dic['dba_name']=""
                        dic['permit_subtype']=self.permit_subtype
                        
                        dic["pf"]=', '.join(row.xpath(".//td[2]/text()").extract())
                        dic['company_phone']=''.join(re.findall('Phone\W*\d*\s*\W*\d*\s*\W*\d*\s*',dic["pf"]))
                        dic['company_fax']=''.join(re.findall('Fax\W*\d*\s*\W*\d*\s*\W*\d*\s*',dic["pf"]))
                        dic['permit_lic_desc']="Agriculture License for "+dic['company_name'] if dic['company_name'] else "Agriculture License"
                        yield self.save_csv(response,dic).load_item()

        elif response.xpath("//table[@id='lcc']").extract():
                dic11={}
                dic2={}
                headings=response.xpath("//table[@id='lcc']//tr/th/text()").extract()
                rows=response.xpath("//table[@id='lcc']//tr")[1:]
                for row in rows:
                    for i,heading in enumerate(headings):
                        dic11[self.remove_tag(heading.lower())]=row.xpath(".//td["+str(i+1)+"]/text()").extract_first()

                    dic2['company']=dic11.get('name','')
                    if self.remove_tag(dic2['company']):
                        dic2['company_name']=self._getDBA(dic2['company'])[0]
                        dic2['dba_name']=self._getDBA(dic2['company'])[1]
                    else:
                        dic2['company_name']=""
                        dic2['dba_name']=""
                    dic2['permit_subtype']=self.permit_subtype
                    dic2['add1']=dic11.get('address','')
                    dic2['add2']=dic11.get('city','')
                    dic2['add3']=dic11.get('state','')
                    dic2['add4']=dic11.get('zip','').replace(".0",'')
                    dic2['county']=dic11.get('county','')
                    dic2['permit_lic_exp_date']=dic11.get('expires','')
                    dic2['company_phone']=dic11.get('phone')
                    if dic2['add1']:
                        dic2['location_address_String']=self.format__address_4(dic2['add1'],dic2['add2'],dic2['add3'],dic2['add4'])
                    else:
                        dic2['location_address_String']=self.format_address(dic2['add2'],dic2['add3'],dic2['add4'])
                    dic2['permit_lic_desc']="Agriculture License for "+dic2['company_name'] if dic2['company_name'] else "Agriculture License"
                    yield self.save_csv(response,dic2).load_item()
        elif response.xpath("//div[@id='table']").extract():

            excel_file=response.xpath("//table[@dir='ltr']//ul/li/a/@href").extract()[1]
            if excel_file:
                urls="https://www.fsis.usda.gov"+response.xpath("//table[@dir='ltr']//ul/li/a/@href").extract()[1]
                yield scrapy.Request(urls, callback= self.parse_excel, dont_filter=True,encoding='utf-8')
        if self.links:
            self.link=self.links.pop(0)
            self.permit_subtype=self.permit.pop(0)
            if "http" not in self.link:self.link="https://www.agr.state.il.us/sharepoint/"+self.link
            else:self.link=self.link
            yield scrapy.Request(url=self.link,callback=self.parse_data,dont_filter=True,errback=self.errors,meta={"max_retry_times":0})
    def parse_row(self, response, row):
        dic3={}
        dic3['permit_subtype']=self.permit_subtype
        dic3['company']=row['Company']
        if self.remove_tag(dic3['company']):
            dic3['company_name']=self._getDBA(dic3['company'])[0]
            dic3['dba_name1']=self._getDBA(dic3['company'])[1]
        else:
            dic3['company_name']=""
            dic3['dba_name1']=""
        dic3['dba']=row['DBAs'].replace(' dba ',';').replace(' DBA ',';').replace(' DBAs',';').replace(' DBA','').replace('DBA ',';')
        if ';' in dic3['dba']:
            dic3['dba_n']=dic3['dba'].split(';')
            if dic3['dba_name1']:
                dic3['dba_names']=dic3['dba_n'].append(dic3['dba_name1'])
            else:
                dic3['dba_names']=dic3['dba_n']
        else:
            if dic3['dba_name1']:
                dic3['dba_names']=[dic3['dba'],dic3['dba_name1']]
            else:
                 dic3['dba_names']=[dic3['dba']]
        dic3['permit_lic_desc']="Agriculture License for "+dic3['company_name'] if dic3['company_name'] else "Agriculture License"
        dic3['add1']=row['Street']
        dic3['add2']=row['City']
        dic3['add3']=row['State']
        dic3['add4']=row['Zip'].replace(".0",'')
        dic3['location_address_String']=self.format__address_4(dic3['add1'],dic3['add2'],dic3['add3'],dic3['add4'])
        dic3['company_phone']=row['Phone']
        if dic3['dba_names']:         
            for i in dic3['dba_names']:
                dic3['dba_name']=i 
                yield self.save_csv(response,dic3).load_item()
        else:
            yield self.save_csv(response,dic3).load_item()

        if self.links:
            self.link=self.links.pop(0)
            self.permit_subtype=self.permit.pop(0)
            if "http" not in self.link:self.link="https://www.agr.state.il.us/sharepoint/"+self.link
            else:self.link=self.link
            yield scrapy.Request(url=self.link,callback=self.parse_data,dont_filter=True,errback=self.errors,meta={"max_retry_times":0})
    def errors(self,response):
        if self.links:
            self.link=self.links.pop(0)
            self.permit_subtype=self.permit.pop(0)
            if "http" not in self.link:self.link="https://www.agr.state.il.us/sharepoint/"+self.link
            else:self.link=self.link
            yield scrapy.Request(url=self.link,callback=self.parse_data,dont_filter=True,errback=self.errors,meta={"max_retry_times":0})
    def save_csv(self,response,data_dic):
        il = ItemLoader(item=IlAgricultureLicensesSpiderItem())
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'IL_Agriculture_Licenses')
        il.add_value('permit_type', 'agriculture_license')
        il.add_value('url', 'https://www2.illinois.gov/sites/agr/licenses/Pages/A-Z-License-List.aspx')
        for k in data_dic:
            il.add_value(k,(data_dic[k]))
        return il