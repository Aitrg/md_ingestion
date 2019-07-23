# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-17 05:18:21
TICKET NUMBER -AI_257
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import scrapy
import re
from Data_scuff.utils.searchCriteria import SearchCriteria
from inline_requests import inline_requests
from Data_scuff.spiders.AI_257.items import WiDouglasBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils

class WiDouglasBuildingPermitsSpider(CommonSpider):
    name = '257_wi_douglas_building_permits'
    allowed_domains = ['douglascountywi.org']
    start_urls = ['https://gcs.douglascountywi.org/gcswebportal/search.aspx']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-257_Permits_Buildings_WI_Dougles_CurationReady'),
        'JIRA_ID':'AI_257',
        'CONCURRENT_REQUESTS':1,
        # 'DOWNLOAD_DELAY':1,
        # 'JOBDIR' : CustomSettings.getJobDirectory('WiTrempealeauBuildingPermitsSpider'),
        'HTTPCACHE_ENABLED':False,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'TOP_HEADER':{'dba_name': '','inspection_date': 'Date','inspection_pass_fail': 'Status.1','inspection_subtype': 'Name','inspection_type': '','inspector_comments': 'Comments','issue #': 'Issue #','location_address_string': 'Property Address+ WI','mail_address_string': 'Billing Address','mixed_name': 'Owner ','mixed_subtype': '','municipality': 'Municipality','parcel number': 'Parcel Number','permit_lic_desc': '','permit_lic_eff_date': 'Issue Date','permit_lic_fee': 'Fee Total','permit_lic_no': 'Application #','permit_lic_status': 'Status','permit_subtype': 'Type','permit_type': '','person': 'Person','prop type': 'Prop Type'},
        'FIELDS_TO_EXPORT':['prop type','parcel number','municipality','location_address_string','mixed_name','dba_name','mixed_subtype','mail_address_string','permit_subtype','permit_lic_no','issue #','permit_lic_eff_date','permit_lic_status','permit_lic_fee','inspection_type','inspection_date','inspection_subtype','inspection_pass_fail','person','inspector_comments','permit_lic_desc','permit_type','sourceName','url','ingestion_timestamp'],
        'NULL_HEADERS':['person', 'prop type', 'issue #', 'municipality', 'parcel number']
        }

    value=True
    alpha1=[]
    alpha=[]
    def __init__(self, start=None, end=None,startnum=None,endnum=None, proxyserver=None, *a, **kw):
        super(WiDouglasBuildingPermitsSpider, self).__init__(start,end,proxyserver=None,*a, **kw)
        import csv
        import os
        current_file_path = os.path.abspath(os.path.dirname(__file__))+'/AI_257_permit_no_list_{}_{}.csv'.format(self.start,self.end)
        self.csv = open(current_file_path, "w")
        columnTitleRow = "parcel_no\n"
    def parse(self, response):
        form_data={
            '__EVENTTARGET': 'ctl00$cphMainApp$LinkButtonPermitTab',
            '__VIEWSTATE': response.xpath("//input[@id='__VIEWSTATE']/@value").extract_first(),
            '__EVENTVALIDATION':response.xpath("//input[@id='__EVENTVALIDATION']/@value").extract_first(),
            '__VIEWSTATEGENERATOR': 'DCE30F85'}
        yield scrapy.FormRequest(url='https://gcs.douglascountywi.org/gcswebportal/search.aspx',callback=self.parse_things,dont_filter=True,formdata=form_data)
    def parse_things(self, response):
        if self.value:
            self.search_element = SearchCriteria.strRange(self.start,self.end)
            self.value=False
        # print ('@@@@@@@@@22',self.search_element)
        if len(self.search_element)>0:
            val=self.search_element.pop(0)
            form_data1={
                'ctl00$cphMainApp$ToolkitScriptManager1': 'ctl00$cphMainApp$upSearch|ctl00$cphMainApp$ButtonPermitSearch',
                '__VIEWSTATEGENERATOR': 'DCE30F85',
                '__VIEWSTATE': response.xpath("//input[@id='__VIEWSTATE']/@value").extract_first(),
                '__EVENTVALIDATION':response.xpath("//input[@id='__EVENTVALIDATION']/@value").extract_first(),
                'ctl00$cphMainApp$PermitSearchCriteria1$TextBoxRefLastName': str(val),
                '__ASYNCPOST': 'true',
                'ctl00$cphMainApp$ButtonPermitSearch': 'Search For Permits'}
            yield scrapy.FormRequest(url='https://gcs.douglascountywi.org/gcswebportal/search.aspx',callback=self.parse_second,dont_filter=True,formdata=form_data1)
    @inline_requests
    def parse_second(self, response):
        if 'Your search returned no results.' in response.text:
            pass
        else:
            vals=response.xpath('//*[@id="ctl00_cphMainApp_GridViewPermitResults"]//tr/td[2]/a/@href').extract()
            for v in range(3,len(vals)+2):
                parcelss_noo=str(response.xpath('//*[@id="ctl00_cphMainApp_GridViewPermitResults"]//tr['+str(v)+']/td[5]/a/text()').extract_first()).strip().replace('None','')
                
                if parcelss_noo=='' or parcelss_noo==None:
                    linkk=str(response.xpath('//*[@id="ctl00_cphMainApp_GridViewPermitResults"]//tr['+str(v)+']/td[4]/a/@href').extract_first()).strip().replace('None','')
                    link_val=linkk.split("'")
                    link_val=link_val[1]
                    form_data_fs={
                        'ctl00$cphMainApp$ToolkitScriptManager1': 'ctl00$cphMainApp$upSearch|'+str(link_val),
                        '__EVENTTARGET': str(link_val),
                        '__VIEWSTATE': response.text.split('__VIEWSTATE|')[1].split('|')[0],
                        '__EVENTVALIDATION':response.text.split('__EVENTVALIDATION|')[1].split('|')[0],
                        '__VIEWSTATEGENERATOR': 'DCE30F85',
                        '__ASYNCPOST': 'true'}

                    parse_first = yield scrapy.FormRequest(url='ttps://gcs.douglascountywi.org/gcswebportal/search.aspx',method='POST',dont_filter=True,formdata=form_data_fs)
                    insp_rep_1=parse_first.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr/td[1]/a/@href").extract()
                    if len(insp_rep_1)>0:
                        for u in range(2, len(insp_rep_1)+2):
                            permit_subtype_1=str(parse_first.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[1]/a/text()").extract_first()).strip().replace('None','')
                            permit_lic_desc_1=permit_subtype_1
                            if permit_lic_desc_1=='' or permit_lic_desc_1==None:
                                permit_lic_desc_1='Building Permit'
                            appl_no_1=parse_first.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[2]/a/text()").extract_first()
                            issue_no_1=parse_first.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[3]/a/text()").extract_first()
                            issue_date_1=parse_first.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[4]/a/text()").extract_first()
                            permit_lic_status_1=parse_first.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[6]/a/text()").extract_first()
                            permit_lic_fee_1=parse_first.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[7]/a/text()").extract_first()
                            data_pass={'prop type':'Real Estate','parcel number':'','municipality':'','location_address_string':'WI','mixed_name':'','dba_name':'','mixed_subtype':'','mail_address_string':'','permit_subtype':permit_subtype_1,'permit_lic_no':appl_no_1,'issue #':issue_no_1,'permit_lic_eff_date':issue_date_1,'permit_lic_status':permit_lic_status_1,'permit_lic_fee':permit_lic_fee_1,'inspection_type':'','inspection_date':'','inspection_subtype':'','inspection_pass_fail':'','person':'','inspector_comments':'','permit_lic_desc':permit_lic_desc_1,'permit_type':'building_permit','sourceName':'','url':'','ingestion_timestamp':''}
                            yield self.save_to_csv(response, **data_pass)

                else:
                    csv_row = str(parcelss_noo) + "\n"
                    self.csv.write(csv_row) 
                    link_no=str(response.xpath('//*[@id="ctl00_cphMainApp_GridViewPermitResults"]//tr['+str(v)+']/td[2]/a/@href').extract_first())
                    link_val=link_no.split("'")
                    link_val=link_val[1]
                    form_data2={
                        'ctl00$cphMainApp$ToolkitScriptManager1': 'ctl00$cphMainApp$upSearch|'+str(link_val),
                        '__EVENTTARGET': str(link_val),
                        '__VIEWSTATE': response.text.split('__VIEWSTATE|')[1].split('|')[0],
                        '__EVENTVALIDATION':response.text.split('__EVENTVALIDATION|')[1].split('|')[0],
                        '__VIEWSTATEGENERATOR': 'DCE30F85',
                        '__ASYNCPOST': 'true'}
                    parse_third = yield scrapy.FormRequest(url='https://gcs.douglascountywi.org/gcswebportal/search.aspx',method='POST',dont_filter=True,formdata=form_data2)

                    form_data3={
                        'ctl00$cphMainApp$ToolkitScriptManager1': 'ctl00$cphMainApp$SearchDetailsPermit$upSearchDetails|ctl00$cphMainApp$SearchDetailsPermit$LinkButtonParcel',
                        '__EVENTTARGET': 'ctl00$cphMainApp$SearchDetailsPermit$LinkButtonParcel',
                        '__VIEWSTATE': parse_third.text.split('__VIEWSTATE|')[1].split('|')[0],
                        '__EVENTVALIDATION':parse_third.text.split('__EVENTVALIDATION|')[1].split('|')[0],
                        '__VIEWSTATEGENERATOR': 'DCE30F85',
                        '__ASYNCPOST': 'true'}
                    parse_property=yield scrapy.FormRequest(url='https://gcs.douglascountywi.org/gcswebportal/search.aspx',method='POST',dont_filter=True,formdata=form_data3)
                    prop_type=parse_property.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_RecordTitle1_LabelTitlePropType"]/text()').extract_first()
                    parcel_no=parse_property.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_RecordTitle1_LabelTitleParcelNum"]/text()').extract_first()
                    municipality=parse_property.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_RecordTitle1_LabelMunicipality"]/text()').extract_first()
                    prop_address=str(parse_property.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_RecordTitle1_LabelTitlePropAddr"]/text()').extract_first()).strip().replace('None','').replace('NONE','')
                    if prop_address==''or prop_address==None:
                        prop_address='WI'
                    else:
                        prop_address=prop_address+', WI'
                    valss=parse_property.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_RecordTitle1_lblBillingAddress"]/text()').extract()
                    if len(valss)==3:
                        name1=[valss[0]]
                        address=valss[1]+', '+valss[2]
                    elif len(valss)>3:
                        name1=valss[0:-2]
                        address=valss[-2]+', '+valss[-1]
                    address=address.replace(' WI ',', WI ')
                    for mixed_name1 in name1:
                        if ' dba ' in mixed_name1 or '(DBA ' in mixed_name1 or 'DBA:' in mixed_name1 or ' DBA ' in mixed_name1:
                            mixed_name1=mixed_name1.replace(' dba ',' DBA ').replace('(DBA ',' DBA ').replace('DBA:',' DBA ')
                            mixed_name1=mixed_name1.split(' DBA ')
                            mixed_name=mixed_name1[0]
                            dba_name=mixed_name1[1]
                        else:
                            mixed_name=mixed_name1
                            dba_name=''
                        # data_pass={'prop type':prop_type,'parcel number':parcel_no,'municipality':municipality,'location_address_string':prop_address,'mixed_name':mixed_name,'dba_name':'','mixed_subtype':'Owner','mail_address_string':address,'permit_subtype':'','permit_lic_no':'','issue #':'','permit_lic_eff_date':'','permit_lic_status':'','permit_lic_fee':'','inspection_type':'','inspection_date':'','inspection_subtype':'','inspection_pass_fail':'','person':'','inspector_comments':'','permit_lic_desc':'','permit_type':'building_permit','sourceName':'','url':'','ingestion_timestamp':''}
                        # yield self.save_to_csv(response, **data_pass)

                    insp_rep=parse_third.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr/td[1]/a/@href").extract()
                    if len(insp_rep)>0:
                        for u in range(2, len(insp_rep)+2):
                            detail_link=parse_third.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[1]/a/@href").extract_first()
                            permit_subtype=str(parse_third.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[1]/a/text()").extract_first()).strip().replace('None','')
                            permit_lic_desc=permit_subtype
                            if permit_lic_desc=='' or permit_lic_desc==None:
                                permit_lic_desc='Building Permit'
                            appl_no=parse_third.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[2]/a/text()").extract_first()
                            issue_no=parse_third.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[3]/a/text()").extract_first()
                            issue_date=parse_third.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[4]/a/text()").extract_first()
                            permit_lic_status=parse_third.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[6]/a/text()").extract_first()
                            permit_lic_fee=str(parse_third.xpath("//table[@id='ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_gvApplications']//tr["+str(u)+"]/td[7]/a/text()").extract_first()).strip().replace('$0.00','')
                            link=detail_link.split("'")
                            link=link[1]
                            form_data4={
                                'ctl00$cphMainApp$ToolkitScriptManager1': 'ctl00$cphMainApp$SearchDetailsPermit$upSearchDetails|'+str(link),
                                '__EVENTTARGET':str(link),
                                '__VIEWSTATE': parse_third.text.split('__VIEWSTATE|')[1].split('|')[0],
                                '__EVENTVALIDATION':parse_third.text.split('__EVENTVALIDATION|')[1].split('|')[0],
                                '__VIEWSTATEGENERATOR': 'DCE30F85',
                                '__ASYNCPOST': 'true'}
                            parse_insp=yield scrapy.FormRequest(url='https://gcs.douglascountywi.org/gcswebportal/search.aspx',method='POST',dont_filter=True,formdata=form_data4)
                            lenn=parse_insp.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_FormView1_gvActivities"]//tr/td[1]').extract()
                            if len(lenn)>0 and 'There are no activities associate' not in lenn[0]:
                                for i in range(2,len(lenn)+2):
                                    insp_date=parse_insp.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_FormView1_gvActivities"]//tr['+str(i)+']/td[1]/text()').extract_first()
                                    insp_type='building_inspection'
                                    insp_subtype=parse_insp.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_FormView1_gvActivities"]//tr['+str(i)+']/td[2]/span/text()').extract_first()
                                    inspection_pass_fail=parse_insp.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_FormView1_gvActivities"]//tr['+str(i)+']/td[3]/span/text()').extract_first()
                                    person=str(parse_insp.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_FormView1_gvActivities"]//tr['+str(i)+']/td[4]/span/text()').extract_first()).strip().replace('None','')
                                    # print ('!)@@@@@@@@@@@@2',person)
                                    inspector_comments=str(parse_insp.xpath('//*[@id="ctl00_cphMainApp_SearchDetailsPermit_PermitDetails1_FormView1_gvActivities"]//tr['+str(i)+']/td[5]/text()').extract_first()).strip().replace('None','')
                                    inspector_comments=re.sub('\s+',' ',inspector_comments)
                                    data_pass={'prop type':prop_type,'parcel number':parcel_no,'municipality':municipality,'location_address_string':prop_address,'mixed_name':mixed_name,'dba_name':dba_name,'mixed_subtype':'Owner','mail_address_string':address,'permit_subtype':permit_subtype,'permit_lic_no':appl_no,'issue #':issue_no,'permit_lic_eff_date':issue_date,'permit_lic_status':permit_lic_status,'permit_lic_fee':permit_lic_fee,'inspection_type':insp_type,'inspection_date':insp_date,'inspection_subtype':insp_subtype,'inspection_pass_fail':inspection_pass_fail,'person':person,'inspector_comments':inspector_comments,'permit_lic_desc':permit_lic_desc,'permit_type':'building_permit','sourceName':'','url':'','ingestion_timestamp':''}
                                    yield self.save_to_csv(response, **data_pass)

                            else:
                                data_pass={'prop type':prop_type,'parcel number':parcel_no,'municipality':municipality,'location_address_string':prop_address,'mixed_name':mixed_name,'dba_name':dba_name,'mixed_subtype':'Owner','mail_address_string':address,'permit_subtype':permit_subtype,'permit_lic_no':appl_no,'issue #':issue_no,'permit_lic_eff_date':issue_date,'permit_lic_status':permit_lic_status,'permit_lic_fee':permit_lic_fee,'inspection_type':'','inspection_date':'','inspection_subtype':'','inspection_pass_fail':'','person':'','inspector_comments':'','permit_lic_desc':permit_lic_desc,'permit_type':'building_permit','sourceName':'','url':'','ingestion_timestamp':''}
                                yield self.save_to_csv(response, **data_pass)

        next_page=str(response.xpath("//*[@id='pager']/table[@border='0']//tr[1]/td/span/following::td[1]/a/@href").extract_first())
        if 'Page$' in next_page:
            next_link=next_page.split("'")
            next_link=next_link[-2]
            form_data_last={
                'ctl00$cphMainApp$ToolkitScriptManager1': 'ctl00$cphMainApp$upSearch|ctl00$cphMainApp$GridViewPermitResults',
                '__EVENTTARGET':'ctl00$cphMainApp$GridViewPermitResults',
                '__EVENTARGUMENT':str(next_link),
                '__VIEWSTATE': response.text.split('__VIEWSTATE|')[1].split('|')[0],
                '__EVENTVALIDATION':response.text.split('__EVENTVALIDATION|')[1].split('|')[0],
                '__VIEWSTATEGENERATOR': 'DCE30F85',
                '__ASYNCPOST': 'true'}
            yield scrapy.FormRequest(url='https://gcs.douglascountywi.org/gcswebportal/search.aspx',method='POST',dont_filter=True,formdata=form_data_last,callback=self.parse_second)
        if len(self.search_element)>0:
            yield scrapy.Request(url=response.url,callback=self.parse_things,dont_filter=True)
    def save_to_csv(self, response,**data_pass):
        if data_pass['permit_lic_desc']=='' or data_pass['permit_lic_desc']==None:
            data_pass['permit_lic_desc']='Building Permit'
        il = ItemLoader(item=WiDouglasBuildingPermitsSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('url', 'https://gcs.douglascountywi.org/gcswebportal/search.aspx')
        il.add_value('sourceName', 'WI_Douglas_Building_Permits')
        il.add_value('inspector_comments', data_pass['inspector_comments'])
        il.add_value('mixed_name', data_pass['mixed_name'])
        il.add_value('permit_subtype', data_pass['permit_subtype'])
        il.add_value('permit_lic_desc', data_pass['permit_lic_desc'])
        il.add_value('mixed_subtype', data_pass['mixed_subtype'])
        il.add_value('permit_type', data_pass['permit_type'])
        il.add_value('permit_lic_fee', data_pass['permit_lic_fee'])
        il.add_value('inspection_pass_fail', data_pass['inspection_pass_fail'])
        il.add_value('permit_lic_status', data_pass['permit_lic_status'])
        il.add_value('location_address_string', data_pass['location_address_string'])
        il.add_value('dba_name', data_pass['dba_name'])
        il.add_value('inspection_subtype', data_pass['inspection_subtype'])
        il.add_value('permit_lic_eff_date', data_pass['permit_lic_eff_date'])
        il.add_value('permit_lic_no', data_pass['permit_lic_no'])
        il.add_value('prop type', data_pass['prop type'])
        il.add_value('inspection_date', data_pass['inspection_date'])
        il.add_value('inspection_type', data_pass['inspection_type'])
        il.add_value('person', data_pass['person'])
        il.add_value('municipality', data_pass['municipality'])
        il.add_value('issue #', data_pass['issue #'])
        il.add_value('parcel number', data_pass['parcel number'])
        il.add_value('mail_address_string', data_pass['mail_address_string'])
        return il.load_item()