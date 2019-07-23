# -*- coding: utf-8 -*-

'''
Created on 2019-May-08 10:26:43
TICKET NUMBER -AI_631
@author: Bharathi
'''
import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_631.items import DcSosSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.shell import inspect_response
from Data_scuff.utils.searchCriteria import SearchCriteria
from inline_requests import inline_requests
class DcSosSpider(CommonSpider):
    name = '631_dc_sos'
    allowed_domains = ['dc.gov']
    start_urls = ['https://corponline.dcra.dc.gov/Home.aspx']
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-631_Companies_SOS_DC_CurationReady'),
        'JIRA_ID':'AI_631',
        'TRACKING_OPTIONAL_PARAMS':['names'],
        'DOWNLOAD_DELAY':3,
        'COOKIES_ENABLED':True,
        # 'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('DcSosSpider'),
        'TOP_HEADER':{   ' non-profit_indicator': '','commencement date': 'Commencement Date','company_name': 'Name','company_subtype': 'Model Type','creation_date': 'Effective Date','dba_name': 'Trade Name','entity_id': 'File Number','foreign name': 'Foreign Name','governor file number': 'Governor File Number','is non-commercial registered agent': 'Is non-commercial Registered Agent','locale': 'Locale','location_address_string': 'Business Address','mixed_email': 'Email','mixed_name': 'Name.1','mixed_subtype': 'Type','person_address_string': 'Address','qualifier': 'Qualifier','status': 'Status'},
        'FIELDS_TO_EXPORT':['company_name','entity_id','creation_date','status','company_subtype','locale', 'qualifier','non-profit_indicator','commencement date','dba_name','foreign name','location_address_string','mixed_name','mixed_subtype','is non-commercial registered agent','person_address_string','mixed_email','governor file number','sourceName','url','ingestion_timestamp'],
        'NULL_HEADERS':['is non-commercial registered agent', 'locale', 'governor file number', 'commencement date', 'foreign name', 'qualifier']
        }
    def parse(self, response):
        form_data={
        "username": "belcy", 
        "password":"aitrg123",
        "LogOn":"Log On"
        }
        yield scrapy.FormRequest.from_response(response, formid="ValidateForm",formdata=form_data,
            callback=self.parse_next, dont_filter=True)
    final_list = []
    alpha_list = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
    search_elements = []
    def parse_next(self,response):
        if self.start.isdigit():
            self.search_elements = SearchCriteria.numberRange(self.start,self.end)
        elif self.start.isalpha():
            self.search_elements = SearchCriteria.strRange(self.start,self.end)
        elif self.start.isalnum():
            for s in self.alpha_list:
                for i in range(0,10):
                    search=s+str(i)
                    self.search_elements.append(search)
        drop_down=response.xpath('//*[@id="BizEntitySearch_EntityStatus"]/option/@value').extract()[1:]
        if len(self.final_list) == 0:
            for search  in self.search_elements:
                for drop in drop_down:
                    self.final_list.append(drop)
                    self.final_list.append(search)
        print('______________final_list:',self.final_list)
        if len(self.final_list)>0:
            val=self.final_list.pop(0)
            search_=self.final_list.pop(0)
            form_data3={"BizEntitySearch_String":str(search_),
            "Search":"Search",
            "BizEntitySearch_Type":"EntityName",
            "BizEntitySearch_DepthType":"StartsWith",
            "BizEntitySearch_EntityStatus":str(val),
            "BizEntitySearch_TradeNameStatus":""
            }
            yield scrapy.FormRequest(url="https://corponline.dcra.dc.gov/Home.aspx/ProcessRequest", callback=self.parse_for_listpage, formdata=form_data3, method='POST',  dont_filter=True,errback=self.handle_form_error)
    @inline_requests
    def parse_for_listpage(self,response):
        check=0
        names=response.xpath('//*[@id="BizEntitySearch_SearchResultsTable"]//tr')[1:]
        for name in names:
            name_link=name.xpath("td[2]/a/@href").extract_first()
            company_name=name.xpath('td[2]/a/text()').extract_first()
            print('____________company_name:',company_name)
            entity_id=name.xpath('td[3]/text()').extract_first()
            creation_date=name.xpath('td[4]/text()').extract_first()
            status=name.xpath('td[5]/text()').extract_first()
            company_subtype=name.xpath('td[6]/text()').extract_first()
            locale=name.xpath('td[7]/text()').extract_first()
            Qualifier=name.xpath('td[8]/text()').extract_first()
            non_profitindicator ="Yes" if "Non-Profit" in Qualifier else ''
            if name_link:
                parse_data=yield scrapy.Request(url="https://corponline.dcra.dc.gov"+name_link, dont_filter=True,meta={'optional':{'names':company_name}})
                commencement_date=parse_data.xpath("//*[contains(text(),'Commencement Date')]//following::span[1]/text()").extract_first()
                forgine_name=parse_data.xpath("//*[contains(text(),'Foreign Name')]//following::span[1]/text()").extract_first()
                loc_addr=parse_data.xpath("//legend[contains(text(),'Business Address')]//following-sibling::table//tr")
                location_address_string=''
                for loc in loc_addr:
                    check_loc=loc.xpath('td/text()').extract_first()
                    if 'data not found' in check_loc:
                        location_address_string='DC'
                    else:
                        check_line=loc.xpath('td/span')
                        for check in check_line:
                            lines=check.xpath('span[1]/text()').extract_first()
                            if 'Line1' in lines or 'Line2' in lines or 'Line3' in lines or 'Line4' in lines or 'Line5' in lines or 'City' in lines or 'State' in lines or 'Zip' in lines:
                                val_line=check.xpath('span[2]/text()').extract_first()
                                val_line1 = val_line if val_line is not None else ''
                                if len(val_line1)>3:
                                    location_address_string=(location_address_string + ', ' + val_line1) if (len(val_line1) > 2 and len(location_address_string) > 2) else (location_address_string+' '+val_line1)
                location_address_string=location_address_string
                location_address_string=' '.join(location_address_string.rsplit(',', 1)) if len(location_address_string)>3 and ',' in location_address_string else location_address_string
                person_address_string=mix_email=residential_agent=''
                agent_name=parse_data.xpath("//*[contains(text(),'Agent')]//following-sibling::table//tr/td/span/span[contains(text(),'Name')]//following::span[1]/text()").extract_first()
                if agent_name is None or len(agent_name)<3:
                    agent_name=''
                    mixed_subtype=''
                else:
                    agent_name=agent_name
                    mixed_subtype='Agent'
                agent_addrs=parse_data.xpath("//*[contains(text(),'Agent')]/following-sibling::table//tr")
                for agnt_addr in agent_addrs:
                    agnt_loc=agnt_addr.xpath('td/span')
                    for agnt in agnt_loc:
                        agnt_loc1=agnt.xpath('span[1]/text()').extract_first()
                        if 'Is non-commercial Registered Agent?' in agnt_loc1:
                            residential_agent=agnt.xpath('span[2]/text()').extract_first()
                        elif 'Email' in agnt_loc1:
                            mix_email=agnt.xpath('span[2]/text()').extract_first()
                        elif 'Line1' in agnt_loc1 or 'Line2' in agnt_loc1 or 'Line3' in agnt_loc1 or 'Line4' in agnt_loc1 or 'Line5' in agnt_loc1 or 'City' in agnt_loc1 or 'State' in agnt_loc1 or 'Zip' in agnt_loc1:
                            person_add=agnt.xpath('span[2]/text()').extract_first()
                            person_add = person_add if person_add is not None else ''
                            person_address_string=(person_address_string + ', ' + person_add) if (len(person_add) > 2 and len(person_address_string) > 2) else (person_address_string+' '+person_add)
                person_address_string=person_address_string
                person_address_string=' '.join(person_address_string.rsplit(',', 1)) if len(person_address_string)>4 and ',' in person_address_string else person_address_string
                #traders
                trades_rows=parse_data.xpath('//*[@id="TradeNameListTable"]//tr')[1:]
                for trd_row in trades_rows:
                    dba_name=trd_row.xpath('td[1]/text()').extract_first()
                    if dba_name is None:
                        pass
                    elif 'No Trade Names' in dba_name:
                        dba_name=''
                    else:
                        dba_name=dba_name
                    check=1
                    data_pass={'company_name':company_name,'entity_id':entity_id,'creation_date':creation_date,'status':status,'company_subtype':company_subtype,'Locale':locale,'Qualifier':Qualifier,'non_profitindicator':non_profitindicator,'CommencementDate':commencement_date,'dba_name':dba_name,'ForeignName':forgine_name,'location_address_string':location_address_string,'mixed_name':agent_name,'mixed_subtype':mixed_subtype,'IsnoncommercialRegisteredAgent':residential_agent,'person_address_string':person_address_string,'mixed_email':mix_email,'Governor_File_Number':'','permit_lic_desc':company_subtype}
                    yield self.save_to_csv(response, **data_pass)
                    #Governers
                    governr_rows=parse_data.xpath('//*[@id="EntityContactListTable"]//tr')[1:]
                    gov_address_string=''
                    for gov_row in governr_rows:
                        mix_sub=gov_row.xpath('td[1]/text()').extract_first()
                        if mix_sub is None or 'No contacts' in mix_sub:
                            pass
                        elif 'Governor' in mix_sub:
                            gov_filenum=gov_row.xpath('td[5]/text()').extract_first()
                            gov_name=gov_row.xpath('td[2]/text()').extract_first()
                            gov_add1=gov_row.xpath('td[3]/text()').extract()
                            gov_address_string=', '.join(gov_add1)
                            check=1
                            data_pass={'company_name':company_name,'entity_id':entity_id,'creation_date':creation_date,'status':status,'company_subtype':company_subtype,'Locale':locale,'Qualifier':Qualifier,'non_profitindicator':non_profitindicator,'CommencementDate':commencement_date,'dba_name':dba_name,'ForeignName':forgine_name,'location_address_string':location_address_string,'mixed_name':gov_name,'mixed_subtype':mix_sub,'IsnoncommercialRegisteredAgent':'','person_address_string':gov_address_string,'mixed_email':'','Governor_File_Number':gov_filenum,'permit_lic_desc':company_subtype}
                            yield self.save_to_csv(response, **data_pass)
                    if check == 0:
                        data_pass={'company_name':company_name,'entity_id':entity_id,'creation_date':creation_date,'status':status,'company_subtype':company_subtype,'Locale':locale,'Qualifier':Qualifier,'non_profitindicator':non_profitindicator,'CommencementDate':'','dba_name':'','ForeignName':'','location_address_string':'DC','mixed_name':'','mixed_subtype':'','IsnoncommercialRegisteredAgent':'','person_address_string':'','mixed_email':'','Governor_File_Number':'','permit_lic_desc':''}
                        yield self.save_to_csv(response, **data_pass)
            else:
                data_pass={'company_name':company_name,'entity_id':entity_id,'creation_date':creation_date,'status':status,'company_subtype':company_subtype,'Locale':locale,'Qualifier':Qualifier,'non_profitindicator':non_profitindicator,'CommencementDate':'','dba_name':'','ForeignName':'','location_address_string':'DC','mixed_name':'','mixed_subtype':'','IsnoncommercialRegisteredAgent':'','person_address_string':'','mixed_email':'','Governor_File_Number':'','permit_lic_desc':''}
                yield self.save_to_csv(response, **data_pass)
        if len(self.final_list)>0:
            yield scrapy.Request(url=self.start_urls[0],callback=self.parse,dont_filter=True)
    def save_to_csv(self, response ,**meta):
        il = ItemLoader(item=DcSosSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'DC_SOS')
        il.add_value('url', 'https://corponline.dcra.dc.gov/Home.aspx')
        il.add_value('is non-commercial registered agent', meta['IsnoncommercialRegisteredAgent'])
        il.add_value('qualifier', meta['Qualifier'].replace('None','') if meta['Qualifier'] and len(meta['Qualifier'])>2 else '')
        il.add_value('commencement date', meta['CommencementDate'])
        il.add_value('locale', meta['Locale'])
        il.add_value('entity_id', meta['entity_id'])
        il.add_value('person_address_string', str(meta['person_address_string']).replace('none,','').replace('None,','').replace('00000','').replace('none','').replace('None','') if meta['person_address_string'] and len(meta['person_address_string'])>3 else 'DC')
        il.add_value('company_subtype', meta['company_subtype'])
        il.add_value('status', meta['status'])
        il.add_value('company_name', meta['company_name'])
        il.add_value('governor file number', meta['Governor_File_Number'])
        il.add_value('mixed_subtype', meta['mixed_subtype'])
        il.add_value('location_address_string', str(meta['location_address_string']).replace('00000','').replace(',none','').replace(',None','').replace('none','').replace('None','') if meta['location_address_string'] and len(meta['location_address_string'])>3 else 'DC')
        il.add_value('dba_name', meta['dba_name'])
        il.add_value('mixed_name', meta['mixed_name'])
        il.add_value('creation_date', meta['creation_date'])
        il.add_value('foreign name', meta['ForeignName'])
        il.add_value('mixed_email', meta['mixed_email'])
        il.add_value('non-profit_indicator', meta['non_profitindicator'])
        return il.load_item()