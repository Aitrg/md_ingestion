# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-09 11:33:38
TICKET NUMBER -AI_116
@author: ait_python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_116.items import FlClayBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.shell import inspect_response
from scrapy import Request
import scrapy,re
import datetime
import json,math
from inline_requests import inline_requests
from Data_scuff.utils.searchCriteria import SearchCriteria


class FlClayBuildingPermitsSpider(CommonSpider):
    name = '116_fl_clay_building_permits'
    allowed_domains = ['claycountygov.com']
    start_urls = ['https://public.claycountygov.com/PermitSearch/']
    # start_urls = ['https://public.claycountygov.com/PermitSearch/#tab=Owner&sortfield=issuedate&sortdirection=D&owner=aa&status=all&page=1&v=285']
    # mian_url = ['https://public.claycountygov.com/PermitSearch/']
    # page_count = 0
    main_url = 'https://public.claycountygov.com/PermitSearch/'
    single_page_count = 0

    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('116_Permits_Buildings_FL_Clay_CurationReady'),
        'JIRA_ID':'AI_116',
        'DOWNLOAD_DELAY':0.5,
        'PROXY_DISABLED' : False,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('fl_clay_building_permits'),
        'TOP_HEADER':{
                         'contractor_lic_no': 'Contractor#',
                         'contractor_lic_type': '',
                         'inspection_date': '',
                         'inspection_id': '',
                         'inspection_pass_fail': '',
                         'inspection_subtype': '',
                         'inspection_type': '',
                         'inspector_comments': '',
                         'location_address_string': 'Proj Addr',
                         'mixed_contractor_name': 'Contractor Name',
                         'mixed_name': 'Owner',
                         'mixed_subtype': '',
                         'notes': 'Notes',
                         'number_of_stories': 'Stories',
                         'permit_lic_desc': '',
                         'permit_lic_eff_date': 'Issue Dt',
                         'permit_lic_no': 'Permit #',
                         'permit_lic_value': 'Valuation',
                         'permit_subtype': 'PERMIT TYPE',
                         'permit_type': '',
                         'person_address_string': 'Address',
                         'year_built': 'Actual Year Built'},
        'FIELDS_TO_EXPORT':[                        
                         'permit_lic_no',
                         'permit_subtype',
                         'permit_lic_desc',
                         'location_address_string',
                         'permit_lic_eff_date',
                         'notes',
                         'mixed_name',
                         'mixed_subtype',
                         'person_address_string',
                         'mixed_contractor_name',
                         'contractor_lic_no',
                         'contractor_lic_type',
                         'permit_lic_value',
                         'number_of_stories',
                         'year_built',
                         'inspection_id',
                         'inspection_date',
                         'inspection_subtype',
                         'inspection_pass_fail',
                         'inspector_comments',
                         'inspection_type',
                         'permit_type',
                         'url',
                         'sourceName',
                         'ingestion_timestamp',
                         ],
        'NULL_HEADERS':['notes']
        }
    
    value = True
    def parse(self, response):
        if self.value:
            self.search_element = SearchCriteria.strRange(self.start,self.end)
            self.value = False
        if len(self.search_element) > 0:
            parm = self.search_element.pop(0)
            page_count_link = 'https://public.claycountygov.com/permitsearch/API/Search/Count?tab=Owner&sortfield=issuedate&sortdirection=D&owner='+str(parm)+'&status=all&page=1&v=496'
            page_count_header = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Content-Type': 'application/json',
                # DNT: 1
                'Host': 'public.claycountygov.com',
                'Referer': 'https://public.claycountygov.com/PermitSearch/',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
            }
            yield scrapy.Request(url = page_count_link,callback=self.parse_user_det,headers=page_count_header,dont_filter=True,meta={"parm" : parm})

    def parse_user_det(self,response):
        # self.search_element = SearchCriteria.strRange(self.start,self.end)
        parm = response.meta['parm']
        data_count = json.loads(response.body_as_unicode())
        page_count = self.ret_count(data_count)
        for x in range(1,page_count+1):
            start_url_2 = 'https://public.claycountygov.com/permitsearch/API/Search/Permit?tab=Owner&sortfield=issuedate&sortdirection=D&owner='+str(parm)+'&status=all&page='+str(x)+'&v=376'
            head = {'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
                    # 'Cache-Control': 'max-age=0',
                    # 'Connection': 'keep-alive',
                    'Content-Type': 'application/json',
                    # 'DNT': '1',
                    'Host': 'public.claycountygov.com',
                    'Referer': 'https://public.claycountygov.com/PermitSearch/',
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
                    }
            yield scrapy.Request(url = start_url_2,callback=self.parse_detail,dont_filter=True,headers=head,meta=response.meta)
        yield scrapy.Request(url=self.main_url,callback=self.parse,dont_filter=True )       

    # @inline_requests
    def parse_detail(self,response):
        jsonres = json.loads(response.body_as_unicode())
        data = response.meta
        if jsonres:
            for x in jsonres:
                data['permit_number'] = x['permit_number']
                permit_num_link = 'https://public.claycountygov.com/permitsearch/API/Permit/Related?permitnumber='+str(x["permit_number"])
                head = {
                    'Accept': '*/*',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
                    'Content-Type': 'application/json',
                    'Host': 'public.claycountygov.com',
                    'Referer': 'https://public.claycountygov.com/PermitSearch/',
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
                }
                yield scrapy.Request(url=permit_num_link,callback=self.parse_permit,headers=head,dont_filter=True,meta=data)


    def parse_permit(self,response):
        item = response.meta
        jsonres_permit = json.loads(response.body_as_unicode())
       
        permit_notes_link = 'https://public.claycountygov.com/permitsearch/API/Permit/PermitNotes?permitnumber='+str(item['permit_number'])
        
        for x in jsonres_permit:
            item['permit_lic_no'] = response.meta['permit_number']
            item['permit_subtype'] = x['permit_type']
            item['permit_lic_desc'] = x['permit_type']
            item['location_address_string'] = ''
            if self.format_date(x["issue_date"]) == '01/01/1':
                item['permit_lic_eff_date'] = ''
            else:
                item['permit_lic_eff_date'] = self.format_date(x["issue_date"])
            item['permit_notes'] = ''
            
            if x['owner_name']:
                item['mixed_name'] = self._getDBA(x['owner_name'])[0]
                item['mixed_subtype'] = 'Owner'
            else:
                item['mixed_name'] = ''
                item['mixed_subtype'] = ''
            item['person_address_string'] = ''
            if x['contractor_name'] != "":
                item['mixed_contractor_name'] = self._getDBA(x['contractor_name'])[0]
                item['contractor_lic_no'] = x['contractor_number']
                item['contractor_lic_type'] = 'contractor_license'
            else:
                item['mixed_contractor_name'] = ''
                item['contractor_lic_no'] = ''
                item['contractor_lic_type'] = ''
            item['pin_complete'] = x['pin_complete']
            item['permit_lic_value'] = ''
            item['number_of_stories'] = ''
            item['year_built'] = ''
            item['inspection_id'] = ''
            item['inspection_date'] = ''
            item['inspection_subtype'] = ''
            item['inspection_pass_fail'] = ''
            item['inspector_comments'] = ''
            item['inspection_type'] = ''
        permit_notes_header = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Host': 'public.claycountygov.com',
            'Referer': 'https://public.claycountygov.com/PermitSearch/',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
        }
        yield scrapy.Request(url= permit_notes_link,callback=self.parse_permit_notes,headers=permit_notes_header,dont_filter=True,meta=item)

    # @inline_requests
    def parse_permit_notes(self,response):
        item = response.meta
        permit_next_page = 'https://qpublic.schneidercorp.com/Application.aspx?AppID=830&LayerID=15008&PageTypeID=4&KeyValue='
        link_pin = item['pin_complete']
        permit_next_page = permit_next_page+link_pin
    
        permit_notes_json = json.loads(response.body_as_unicode())
        notes = []
        for x in permit_notes_json:
            if '*' not in x['note']:
                p = re.compile(r'<.*?>')
                notes.append(p.sub('',x['note']))
        item['permit_notes'] =  ','.join(notes)
        yield scrapy.Request(url= permit_next_page,callback=self.parse_detail_2,dont_filter=True,meta=item)        

    def parse_detail_2(self,response):
        item = response.meta
        inspection_link = 'https://public.claycountygov.com/inspectionscheduler/API/Inspection/Permit/'+str(item['permit_number'])
        permit_lic_val = response.xpath('//*[@id="ctlBodyPane_ctl11_ctl01_grdValuation"]//tr/td[2]/text()').extract()
        location_address_string = response.xpath('//*[@id="ctlBodyPane_ctl00_ctl01_lblPropertyAddress"]/text()').extract()
        location_address_string = ','.join(location_address_string).strip()
        location_address_string = re.sub(r'(\d+)$',r',FL \1',location_address_string)
        item['location_address_string'] = location_address_string
        personal_address_string = response.xpath('//*[@id="ctlBodyPane_ctl02_ctl01_lstPrimaryOwner_ctl00_lblPrimaryOwnerAddress"]/text()').extract()
        personal_address_string = ''.join(location_address_string).strip().replace("\n",'')
        if personal_address_string:
            personal_address_string_re = re.search(r'.*(,FL )[\d]{5}', personal_address_string)
            if personal_address_string_re is None:
                item['person_address_string'] = personal_address_string_re.group()
            else:
                item['person_address_string'] = personal_address_string
        value = []
        for x in permit_lic_val:
            if x:
                x = x.replace("$",'').replace(',','')
                value.append(int(x))
        permit_lic_value = "$"+str(sum(value))
        number_of_stories = response.xpath('//*[@id="ctlBodyPane_ctl04_ctl01_lstBuildings_ctl00_lblStories"]/text()').get()
        year_built = response.xpath('//*[@id="ctlBodyPane_ctl04_ctl01_lstBuildings_ctl00_Label1"]/text()').get()
        item['permit_lic_value'] = permit_lic_value
        if number_of_stories == 'None' and number_of_stories ==None:
            item['number_of_stories'] = ""
        else:
            item['number_of_stories'] = str(number_of_stories)
        if year_built == 'None' and year_built == None:
            item['year_built']  = ''
        else:
            item['year_built'] = str(year_built)
        req_header = {
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json; charset=utf-8',
            'Host': 'public.claycountygov.com',
            'Referer': 'https://public.claycountygov.com/inspectionscheduler/',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
        }

        yield scrapy.Request(url= inspection_link,callback=self.parse_inspection_data,headers=req_header,dont_filter=True,meta=item)
    
    def parse_inspection_data(self,response):
        item = response.meta
        inspect_json = json.loads(response.body_as_unicode())
        csv_save_data = {
                        'permit_lic_no':"",
                         'permit_subtype':"",
                         'permit_lic_desc':'',
                         'location_address_string':'',
                         'permit_lic_eff_date':'',
                         'notes':'',
                         'mixed_name':'',
                         'mixed_subtype':'',
                         'person_address_string':'',
                         'mixed_contractor_name':'',
                         'contractor_lic_no':'',
                         'contractor_lic_type':'',
                         'permit_lic_value':'',
                         'number_of_stories':'',
                         'year_built':'',
                         'inspection_id':'',
                         'inspection_date':'',
                         'inspection_subtype':'',
                         'inspection_pass_fail':'',
                         'inspector_comments':'',
                         'inspection_type':''
        }
        csv_save_data['permit_lic_no'] = item['permit_lic_no']
        csv_save_data['permit_subtype'] = item['permit_subtype']
        csv_save_data['permit_lic_desc'] = item['permit_lic_desc']
        if item['location_address_string']:
            csv_save_data['location_address_string'] = item['location_address_string']
        else:
            csv_save_data['location_address_string'] = 'FL'
        csv_save_data['permit_lic_eff_date'] = item['permit_lic_eff_date']
        csv_save_data['notes'] = item['permit_notes']
        csv_save_data['mixed_name'] = item['mixed_name']
        csv_save_data['mixed_subtype'] = item['mixed_subtype']
        csv_save_data['person_address_string'] = item['person_address_string']
        csv_save_data['permit_lic_value'] = item['permit_lic_value']
        csv_save_data['number_of_stories'] = item['number_of_stories']
        csv_save_data['year_built'] = item['year_built']
        yield self.save_to_csv(response,**csv_save_data).load_item()
        if item['mixed_contractor_name']:
            csv_save_data['mixed_name'] = ''
            csv_save_data['mixed_subtype'] = ''
            csv_save_data['person_address_string'] = ''
            csv_save_data['mixed_contractor_name'] = item['mixed_contractor_name']
            csv_save_data['contractor_lic_no'] = item['contractor_lic_no']
            csv_save_data['contractor_lic_type'] = item['contractor_lic_type']
            yield self.save_to_csv(response,**csv_save_data).load_item()
        if len(inspect_json)>0:
            for x in inspect_json:
                if x["InsDesc"] != 'No Inspections':
                    csv_save_data['inspection_id'] = x["PermitNo"]
                    csv_save_data['inspection_date'] = x["DisplayInspDateTime"]
                    csv_save_data['inspection_subtype'] = x['InsDesc']
                    csv_save_data['inspection_pass_fail'] = x["ResultDescription"]
                    csv_save_data['inspector_comments'] = x["Comment"]
                    csv_save_data['inspection_type'] = 'building_inspection'
                    csv_save_data['mixed_contractor_name'] = ''
                    csv_save_data['contractor_lic_no'] = ''
                    csv_save_data['contractor_lic_type'] = ''                
                    yield self.save_to_csv(response,**csv_save_data).load_item()



    def ret_count(self,data):
        return math.ceil(data/20)

    def save_to_csv(self,response,**meta_data):
        il = ItemLoader(item=FlClayBuildingPermitsSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('permit_lic_no',str(meta_data['permit_lic_no']))
        il.add_value('permit_subtype', meta_data['permit_subtype'])
        il.add_value('permit_lic_desc', meta_data['permit_lic_desc'])
        il.add_value('location_address_string', meta_data['location_address_string'])
        il.add_value('permit_lic_eff_date', meta_data['permit_lic_eff_date'])
        il.add_value('notes', meta_data['notes'])
        il.add_value('mixed_name', meta_data['mixed_name'])
        il.add_value('mixed_subtype', meta_data['mixed_subtype'])
        il.add_value('person_address_string', meta_data['person_address_string'])
        il.add_value('mixed_contractor_name', meta_data['mixed_contractor_name'])
        il.add_value('contractor_lic_no', meta_data['contractor_lic_no'])
        il.add_value('contractor_lic_type', meta_data['contractor_lic_type'])
        il.add_value('permit_lic_value', meta_data['permit_lic_value'])
        if meta_data['number_of_stories'] == 'None':
            il.add_value('number_of_stories','')
        else:
            il.add_value('number_of_stories', meta_data['number_of_stories'])
        if meta_data['year_built'] == 'None':
            il.add_value('year_built', '')
        else:
            il.add_value('year_built', meta_data['year_built'])
        il.add_value('inspection_id', meta_data['inspection_id'])
        il.add_value('inspection_date', meta_data['inspection_date'])
        il.add_value('inspection_subtype', meta_data['inspection_subtype'])
        il.add_value('inspection_pass_fail', meta_data['inspection_pass_fail'])
        il.add_value('inspector_comments', meta_data['inspector_comments'])
        il.add_value('inspection_type', meta_data['inspection_type'])
        il.add_value('permit_type', "building_permit")
        il.add_value('url', "http://www.claycountygov.com/about-us/local-government/public-records-search/permits")
        il.add_value('sourceName', 'FL_Clay_Building_Permits')
        return il