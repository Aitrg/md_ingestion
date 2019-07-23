# -*- coding: utf-8 -*-

'''
Created on 2019-Jan-10 13:17:37
TICKET NUMBER -AI_748
@author: gaurav
'''

import datetime
import scrapy
import string
from scrapy.selector import Selector
from scrapy.shell import inspect_response
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from inline_requests import inline_requests
from Data_scuff.utils.searchCriteria import SearchCriteria
from Data_scuff.spiders.AI_748.items import VaMecklenburgBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils

class VaMecklenburgBuildingPermitsSpider(CommonSpider):
    name = '748_va_mecklenburg_building_permits'
    allowed_domains = ['mecklenburgcountync.gov']
    start_urls = ['https://webpermit.mecklenburgcountync.gov/Default.aspx?PossePresentation=SearchByAddress']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-748_Permits_Buildings_VA_Mecklenburg_CurationReady'),
        'JIRA_ID':'AI_748',
        'HTTPCACHE_ENABLED':False,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'DOWNLOAD_DELAY':1,
        # 'JOBDIR' : CustomSettings.getJobDirectory('VaMecklenburgBuildingPermitsSpider'),
        'TOP_HEADER':{'contractor id': 'Contractor ID','contractor_address_string': 'Contractor Address','contractor_lic_no': 'License #','contractor_lic_type': '','contractor_phone': 'Contractor Phone','equipment type': 'Equipment Type','inspection_date': 'Result Date','inspection_id': 'Confirmation #','inspection_pass_fail': 'Inspection Result','inspection_subtype': 'Task Requested','inspection_type': '','location_address_string': 'Property Address','master #': 'Master #','mixed_contractor_name': 'Contractor','mixed_name': 'Name','mixed_phone': ' Phone Number','mixed_subtype': 'Person type - Address Permit Holds & Address Occupancy Holds','occupancy type': 'Occupancy Type','occupancy_subtype': 'Property Use','parcel #': 'Parcel #','permit_lic_desc': 'Type of Work','permit_lic_fee': 'Permit Fee  ','permit_lic_no': 'Permit #/Sub-permits - Projects and Project Holds','permit_lic_status': 'Permit Status','Permit Type': 'permit_subtype','permit_type': '','person_address_string': ' Address','submittal #': 'Submittal #','type of building': 'Type of Building','usdc code': 'USDC Code'},
        'FIELDS_TO_EXPORT':['permit_lic_no','master #','submittal #','permit_subtype','permit_lic_status','location_address_string','parcel #','occupancy_subtype','permit_lic_desc','occupancy type','usdc code','type of building','equipment type','permit_lic_fee','mixed_name','mixed_subtype','mixed_phone','person_address_string','mixed_contractor_name','contractor id','contractor_phone','contractor_lic_no','contractor_lic_type','contractor_address_string','inspection_id','inspection_subtype','inspection_date','inspection_pass_fail','inspection_type','permit_type','sourceName','url','ingestion_timestamp'],
        'NULL_HEADERS':['master #', 'submittal #', 'parcel #', 'occupancy type', 'usdc code', 'type of building', 'equipment type', 'total cost/fee', ' address permit /occupancy hold reason', 'address permit /occupancy  hold note', 'contractor id']
        }
    search_businames=[]
    # data_pass={}
    @inline_requests
    def parse(self, response):
        main_page = yield scrapy.Request(url="https://webpermit.mecklenburgcountync.gov/Default.aspx?PossePresentation=SearchByAddress", dont_filter=True)
        datac = main_page.xpath("/html/body/div/form[1]/input[5]/@value").extract()[0]
        self.search_businames = SearchCriteria.strRange(self.start,self.end)
        for alpha in self.search_businames:
            for iv in range(0,10):
                dc = ('C','S0',447713, str(iv)),('C','S0',447712,str(alpha)),('F','',0,0)
                dc1 = datac+','+''.join(map(str,str(dc))).replace('((','(').replace('))',')').replace(' ','')
                formdata1 ={
                    'currentpaneid': '467087',
                    'paneid': '467088',
                    'functiondef': '5',
                    'sortcolumns': '{}',
                    'datachanges': str(dc1),
                    'comesfrom': 'posse',
                    'changesxml': ''}
                yield scrapy.FormRequest(url="https://webpermit.mecklenburgcountync.gov/Default.aspx?PossePresentation=SearchByAddress",formdata=formdata1, dont_filter = True, callback = self.parse2)
    def parse2(self, response):
        if response.url !='https://webpermit.mecklenburgcountync.gov/None':
            links=response.xpath('//*[@id="ctl00_cphPaneBand_pnlPaneBand"]/table[1]/tbody/tr/td[1]//a/@href').extract()
            for i in range(1,len(links)+1):
                link=response.xpath('//*[@id="ctl00_cphPaneBand_pnlPaneBand"]/table[1]/tbody['+str(i)+']/tr/td[1]//a/@href').extract_first()
                yield scrapy.Request(url='https://webpermit.mecklenburgcountync.gov/'+str(link),callback=self.parse_second,dont_filter=True)
    def parse_second(self, response):
        if response.url !='https://webpermit.mecklenburgcountync.gov/None':
            parse_urls=response.xpath('//table//tr/td/table/tbody/tr/td[1]//a/@href').extract()
            if len(parse_urls)>0:
                for url in range(1,len(parse_urls)+1):
                    parse_url=response.xpath('//table//tr/td/table/tbody['+str(url)+']/tr/td[1]//a/@href').extract_first()
                    yield scrapy.Request(url='https://webpermit.mecklenburgcountync.gov/'+str(parse_url),callback=self.parse_last,dont_filter=True)
            else:
                parcel_no=str(response.xpath("//*[contains(@id, 'Parcel')]/text()").extract_first()).strip().replace('None','')
                permit_subtype=''
                address=str(response.xpath("//*[contains(text(), 'Address')]/following::td[2]/span/text()").extract_first()).strip()
                if address=='None' or address=='':
                    address='VA'
                else:
                    address=address+', VA'
                permit_lic_status=str(response.xpath("//*[contains(text(), 'Status:')]/following::td[2]/span/text()").extract_first()).strip().replace('None','')
                data_pass= {'permit_lic_no':'','master #':'','submittal #':'','permit_subtype':permit_subtype,'permit_lic_status':permit_lic_status,'location_address_string':address,'parcel #':parcel_no,'occupancy_subtype':'','permit_lic_desc':'','occupancy type':'','usdc code':'','type of building':'','equipment type':'','permit_lic_fee':'','mixed_name':'','mixed_subtype':'','mixed_phone':'','person_address_string':'','mixed_contractor_name':'','contractor id':'','contractor_phone':'','contractor_lic_no':'','contractor_lic_type':'','contractor_address_string':'','inspection_id':'','inspection_subtype':'','inspection_date':'','inspection_pass_fail':'','inspection_type':'','permit_type':'','sourceName':'','url':'','ingestion_timestamp':''}
                yield self.save_to_csv(response, **data_pass)

    def parse_last(self, response):
        if response.url !='https://webpermit.mecklenburgcountync.gov/None':
            last_urls=response.xpath('//table//tr/td/table/tbody/tr/td[1]//a/@href').extract()
            number=response.xpath("//table[@class='possegrid']/tbody/tr/td[2]/span").extract_first()
            for v in range(1,len(last_urls)+1):
                last_url=response.xpath('//table//tr/td/table/tbody['+str(v)+']/tr/td[1]//a/@href').extract_first()
                yield scrapy.Request(url='https://webpermit.mecklenburgcountync.gov/'+str(last_url),callback=self.parse_final,dont_filter=True)
    @inline_requests
    def parse_final(self, response):
        if response.url !='https://webpermit.mecklenburgcountync.gov/None':
            project=response.xpath("//*[contains(text(), 'Project #')]/following::td[2]/span/text()").extract_first()
            permit_lic_no=response.xpath("//*[contains(text(), 'Permit #')]/following::td[2]/span/text()").extract_first()
            master=response.xpath("//*[contains(text(), 'Master #')]/following::td[2]/span/text()").extract_first()
            permit_subtype=response.xpath("//*[contains(text(), 'Permit Type')]/following::td[2]/span/text()").extract_first()
            submittal=response.xpath("//*[contains(text(), 'Submittal #')]/following::td[2]/span/text()").extract_first()
            permit_lic_status=response.xpath("//*[contains(text(), 'Permit Status')]/following::td[2]/span/text()").extract_first()
            location_address_string=str(response.xpath("//*[contains(@id, 'ProjectAddress')]/text()").extract_first()).strip()
            if location_address_string=='None' or location_address_string=='':
                location_address_string='VA'
            else:
                location_address_string=location_address_string+', VA'
            parcel_no=str(response.xpath("//*[contains(@id, 'Parcel')]/text()").extract_first()).strip().replace('None','')
            property_use=str(response.xpath("//*[contains(@id, 'Property')]/text()").extract_first()).strip().replace('None','')
            type_of_work=str(response.xpath("//*[contains(text(), 'Type of Work')]/following::td[2]/span/text()").extract_first()).replace('None','')
            occupancy_subtype=str(response.xpath("//*[contains(text(), '')]/following::td[2]/span/text()").extract_first()).replace('None','')
            occupancy_type=str(response.xpath("//*[contains(text(), 'Occupancy Type')]/following::td[2]/span/text()").extract_first()).replace('None','')
            usdc_code=str(response.xpath("//*[contains(text(), 'USDC Code')]/following::td[2]/span/text()").extract_first()).replace('None','')
            type_of_building=str(response.xpath("//*[contains(text(), 'Type of Building')]/following::td[2]/span/text()").extract_first()).replace('None','')
            equipment_type=str(response.xpath("//*[contains(text(), 'Equipment Type')]/following::td[2]/span/text()").extract_first()).replace('None','')
            permit_lic_fee=str(response.xpath("//*[contains(text(), 'Permit Fee')]/following::td[2]/span/text()").extract_first()).replace('None','')
           
            mixed_name=str(response.xpath("//*[contains(text(), 'Owner/Tenant')]/following::td[2]/span/text()").extract_first()).strip().replace('None','')
            if mixed_name=='None' or mixed_name=='':
                mixed_subtype=''
            else:
                mixed_subtype='Owner/Tenant'
            mixed_address=str(response.xpath("//*[contains(@id, 'OwnerTenantAddress')]/text()[1]").extract_first()).strip().replace('None','VA')
            mixed_phone=str(response.xpath("//*[contains(@name, 'OwnerTenantPhone')]/text()[1]").extract_first()).strip().replace('None','')
            if mixed_phone=='( ) -'or '( ) -' in mixed_phone:
                mixed_phone=''
            data_pass= {'permit_lic_no':permit_lic_no,'master #':master,'submittal #':submittal,'permit_subtype':permit_subtype,'permit_lic_status':permit_lic_status,'location_address_string':location_address_string,'parcel #':parcel_no,'occupancy_subtype':occupancy_subtype,'permit_lic_desc':permit_subtype,'occupancy type':occupancy_type,'usdc code':usdc_code,'type of building':type_of_building,'equipment type':equipment_type,'permit_lic_fee':permit_lic_fee,'mixed_name':mixed_name,'mixed_subtype':mixed_subtype,'mixed_phone':mixed_phone,'person_address_string':mixed_address,'mixed_contractor_name':'','contractor id':'','contractor_phone':'','contractor_lic_no':'','contractor_lic_type':'','contractor_address_string':'','inspection_id':'','inspection_subtype':'','inspection_date':'','inspection_pass_fail':'','inspection_type':'','permit_type':'','sourceName':'','url':'','ingestion_timestamp':''}
            yield self.save_to_csv(response, **data_pass)
            agent_name=response.xpath("//*[contains(@id, 'LienDisplay')]/text()[1]").extract_first()
            agent_type='Agent'
            agent_phone=response.xpath("//*[contains(@id, 'LienDisplay')]/text()[2]").extract_first()
            agent_details=response.xpath("//*[contains(@id, 'LienDisplay')]/text()").extract()
            agent_address=''

            for details in agent_details:
                if 'Physical Address' in details:
                    agent_address=details
                else:
                    agent_address='VA'

            contractor_details=response.xpath("//*[contains(text(), 'Contractor')]/following::td[2]/span/text()").extract()
            if len(contractor_details)>1:
                contractor_name=contractor_details[0]
                contractor_id=contractor_details[1]
                contractor_phone=str(response.xpath("//*[contains(@name, 'ContractorPhone')]/text()[1]").extract_first()).strip()
                if contractor_phone=='None' or contractor_phone=='':
                    contractor_phone=response.xpath("//*[contains(@name, 'ContractorHomeOwnerPhone')]/text()[1]").extract_first()
                contractor_lic_no=str(response.xpath("//*[contains(@name, 'ContractorLicense')]/text()[1]").extract_first()).strip()
                if contractor_lic_no=='None' or contractor_lic_no=='':
                    contractor_lic_no=response.xpath("//*[contains(@name, 'License_Number')]/text()[1]").extract_first()
                contractor_address_string=response.xpath("//*[contains(@id, 'ContractorHomeOwnerAddress')]/text()").extract()
                if len(contractor_address_string)>0:
                    contractor_address=', '.join(contractor_address_string)
                else:
                    contractor_address=str(response.xpath("//*[contains(@id, 'ContractorAddress')]/text()").extract_first()).strip()
                contractor_lic_type='contractor_license'
                data_pass= {'permit_lic_no':permit_lic_no,'master #':master,'submittal #':submittal,'permit_subtype':permit_subtype,'permit_lic_status':permit_lic_status,'location_address_string':location_address_string,'parcel #':parcel_no,'occupancy_subtype':occupancy_subtype,'permit_lic_desc':permit_subtype,'occupancy type':occupancy_type,'usdc code':usdc_code,'type of building':type_of_building,'equipment type':equipment_type,'permit_lic_fee':permit_lic_fee,'mixed_name':'','mixed_subtype':'','mixed_phone':'','person_address_string':'','mixed_contractor_name':contractor_name,'contractor id':contractor_id,'contractor_phone':contractor_phone,'contractor_lic_no':contractor_lic_no,'contractor_lic_type':contractor_lic_type,'contractor_address_string':contractor_address,'inspection_id':'','inspection_subtype':'','inspection_date':'','inspection_pass_fail':'','inspection_type':'','permit_type':'','sourceName':'','url':'','ingestion_timestamp':''}
                yield self.save_to_csv(response, **data_pass)
            urll=str(response.xpath('//*[@id="ctl00_cphPaneBand_pnlPaneBand"]/table//tr[1]/td[7]/span/a/@href').extract_first()).strip()
            if 'http://webpermit.mecklenburgcountync.gov/default.aspx?' in urll:
                parse_last_one=yield scrapy.Request(url=urll,dont_filter=True)
            
                linkk=str(parse_last_one.xpath("//table//tr/td/table/tbody/tr[1]/td[1]/span//a/@href").extract_first()).strip()
                if linkk!='None':
                    for v in range(1,len(linkk)+1):
                        linkks=parse_last_one.xpath("//table//tr/td/table/tbody["+str(v)+"]/tr[1]/td[1]/span//a/@href").extract_first()
                        parse_insp=yield scrapy.Request(url='https://webpermit.mecklenburgcountync.gov/'+str(linkks),dont_filter=True)
                        inspection_id=parse_insp.xpath("//*[contains(text(), 'Confirmation #')]/following::td[2]/span/text()").extract_first()
                        inspection_date=parse_insp.xpath("//*[contains(text(), 'Result Date')]/following::td[2]/span/text()").extract_first()
                        inspection_subtype=parse_insp.xpath("//table//tr/td/table/tbody[1]/tr/td[2]/span/text()").extract_first()
                        inspection_result=parse_insp.xpath("//table//tr/td/table/tbody[1]/tr/td[3]/span/text()").extract_first()
                        data_pass = {'permit_lic_no':permit_lic_no,'master #':master,'submittal #':submittal,'permit_subtype':permit_subtype,'permit_lic_status':permit_lic_status,'location_address_string':location_address_string,'parcel #':parcel_no,'occupancy_subtype':occupancy_subtype,'permit_lic_desc':permit_subtype,'occupancy type':occupancy_type,'usdc code':usdc_code,'type of building':type_of_building,'equipment type':equipment_type,'permit_lic_fee':permit_lic_fee,'mixed_name':'','mixed_subtype':'','mixed_phone':'','person_address_string':'','mixed_contractor_name':'','contractor id':'','contractor_phone':'','contractor_lic_no':'','contractor_lic_type':'','contractor_address_string':'','inspection_id':inspection_id,'inspection_subtype':inspection_subtype,'inspection_date':inspection_date,'inspection_pass_fail':inspection_result,'inspection_type':'building_inspection','permit_type':'','sourceName':'','url':'','ingestion_timestamp':''}
                        yield self.save_to_csv(response, **data_pass)
    def save_to_csv(self, response,**data_pass):

        il = ItemLoader(item=VaMecklenburgBuildingPermitsSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp',Utils.getingestion_timestamp())
        il.add_value('sourceName', 'VA_Mecklenburg_Building_Permits')
        il.add_value('url', 'https://webpermit.mecklenburgcountync.gov/Default.aspx?PossePresentation=SearchByAddress')
        il.add_value('person_address_string',data_pass['person_address_string'])
        il.add_value('permit_lic_no',data_pass['permit_lic_no'] )
        il.add_value('master #', data_pass['master #'])
        il.add_value('submittal #', data_pass['submittal #'])
        il.add_value('permit_subtype', data_pass['permit_subtype'])
        il.add_value('permit_lic_status', data_pass['permit_lic_status'])
        il.add_value('location_address_string', data_pass['location_address_string'])
        il.add_value('parcel #', data_pass['parcel #'])
        il.add_value('occupancy_subtype', data_pass['occupancy type'])
        il.add_value('permit_subtype', data_pass['permit_subtype'])
        il.add_value('occupancy type', data_pass['occupancy type'])
        il.add_value('usdc code',data_pass['usdc code'] )
        il.add_value('type of building', data_pass['type of building'])
        il.add_value('equipment type', data_pass['equipment type'])
        il.add_value('permit_lic_fee',data_pass['permit_lic_fee'] )
        il.add_value('mixed_name', data_pass['mixed_name'])
        il.add_value('mixed_subtype',data_pass['mixed_subtype'] )
        il.add_value('mixed_phone', data_pass['mixed_phone'])
        il.add_value('mixed_contractor_name', data_pass['mixed_contractor_name'])
        il.add_value('contractor id', data_pass['contractor id'])
        il.add_value('contractor_phone', data_pass['contractor_phone'])
        il.add_value('contractor_lic_no', data_pass['contractor_lic_no'])
        il.add_value('contractor_lic_type', data_pass['contractor_lic_type'])
        il.add_value('contractor_address_string', data_pass['contractor_address_string'])
        il.add_value('inspection_id', data_pass['inspection_id'])
        il.add_value('inspection_subtype', data_pass['inspection_subtype'])
        il.add_value('inspection_date', data_pass['inspection_date'])
        il.add_value('inspection_pass_fail', data_pass['inspection_pass_fail'])
        il.add_value('inspection_type', data_pass['inspection_type'])
        il.add_value('permit_type', 'building_permit')
        return il.load_item()
