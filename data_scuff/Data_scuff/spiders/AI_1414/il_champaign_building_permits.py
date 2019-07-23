'''Created on 2019-Jun-17 07:34:16
TICKET NUMBER -AI_1414
@author: python'''
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import scrapy
import pandas as pd
import datetime
import copy
from Data_scuff.spiders.AI_1414.items import IlChampaignBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import re
from inline_requests import inline_requests
from Data_scuff.utils.searchCriteria import SearchCriteria
class IlChampaignBuildingPermitsSpider(CommonSpider):
    name = '1414_il_champaign_building_permits'
    allowed_domains = ['champaign.il.us']
    start_urls = ['http://etrakit.ci.champaign.il.us/etrakit3/Search/permit.aspx'] 
    handle_httpstatus_list = [500]   
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1414_Permits_Building_IL_Champaign_CurationReady'),
        'JIRA_ID':'AI_1414',
        'DOWNLOAD_DELAY':0.5,
        'CONCURENT_REQUEST':1,
        'TRACKING_OPTIONAL_PARAMS':['permit_lic_no'],
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('il_champaign_building_permits'),
        'TOP_HEADER':{'apn': 'APN','approved date': 'Approved Date','contractor_address_string': 'Address.2','contractor_dba': '','dba_name': '','finaled date': 'Finaled Date','inspection_date': 'Completed','inspection_pass_fail': 'Result','inspection_subtype': 'Type.1','inspection_type': '','location_address_string': 'Address','mixed_contractor_name': 'CONTRACTOR','mixed_name': 'Name','mixed_subtype': '','notes': 'Notes','permit_applied_date': 'Applied Date','permit_lic_desc': 'Short Description','permit_lic_eff_date': 'Issued Date','permit_lic_exp_date': 'Expiration Date','permit_lic_fee': 'Fees','permit_lic_no': 'Permit #','permit_lic_status': 'Status','permit_subtype': 'Type','permit_type': '','person_address_string': 'Address.1','property type': 'Property Type','scheduled date': 'Scheduled Date','subtype': 'Subtype'},
        'FIELDS_TO_EXPORT':[   
        'permit_lic_no','permit_subtype','subtype','permit_lic_desc','permit_lic_status','permit_applied_date','approved date','permit_lic_eff_date','finaled date', 'permit_lic_exp_date','notes','location_address_string','property type','apn','mixed_name','dba_name','mixed_subtype','person_address_string','mixed_contractor_name','contractor_dba','contractor_address_string','permit_lic_fee','inspection_subtype','inspection_pass_fail','scheduled date','inspection_date','inspection_type','permit_type','sourceName','url','ingestion_timestamp'],
        'NULL_HEADERS':['apn', 'scheduled date', 'subtype', 'notes', 'finaled date', 'approved date', 'property type']
        }
    search_element = [] 
    check_first = True
    end_date = ''
    def parse(self, response):
        if self.check_first:
            self.check_first = False
            self.search_element = SearchCriteria.dateRange(self.start,self.end, freq='1D', formatter='%m/%d/%Y')
            self.end_date = self.search_element.pop(0)
        if len(self.search_element) > 0:
            start_date = copy.copy(self.end_date)
            self.end_date = self.search_element.pop(0)
            formdata={'ctl00$RadScriptManager1': 'ctl00$RadScriptManager1|ctl00$cplMain$btnSearch','__EVENTTARGET':' ctl00$cplMain$btnSearch','__EVENTARGUMENT': '','__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),'__VIEWSTATEGENERATOR': '2A136539','ctl00$ucLogin$hfDashboardRedirect': 'https://etrakit.champaignil.gov/etrakit/dashboard.aspx','ctl00$ucLogin$hfCartRedirect': 'https://etrakit.champaignil.gov/etrakit/ShoppingCart.aspx?iscartview=true','ctl00$ucLogin$hfHome':' https://etrakit.champaignil.gov/etrakit/default.aspx','ctl00_ucLogin_RadTextBox2_ClientState': '{"enabled":true,"emptyMessage":"Password","validationText":"","valueAsString":"","lastSetTextBoxValue":"Password"}','ctl00_ucLogin_txtPassword_ClientState': '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","lastSetTextBoxValue":""}', 
            'ctl00$cplMain$ddSearchBy': 'Permit_Main.ISSUED',
            'ctl00$cplMain$ddSearchOper': 'EQUALS',
            'ctl00$cplMain$txtSearchString':str(start_date),
            'ctl00_cplMain_tcSearchDetails_ClientState': '{"selectedIndexes":["2"],"logEntries":[],"scrollState":{}}','__ASYNCPOST': 'true','RadAJAXControlID': 'ctl00_RadAjaxManager1'}
            yield scrapy.FormRequest(url=response.url,formdata=formdata,method='POST',dont_filter=True,callback=self.parse_details,meta={'page':1,'start_date':str(start_date)})
    @inline_requests
    def parse_details(self, response):
        page_no=response.meta['page']
        start_date=response.meta['start_date']
        tr_list=response.xpath('//*[@id="ctl00_cplMain_rgSearchRslts_ctl00"]//tr[@class="rgRow" or @class="rgAltRow"]')
        for ind,tr in enumerate(tr_list):
            value='RowClick;'+str(ind)
            permit_lic_no=tr.xpath('td[1]/text()').extract_first()
            form_data_link={'ctl00$RadScriptManager1': 'ctl00$ctl00$cplMain$rgSearchRsltsPanel|ctl00$cplMain$rgSearchRslts',
            'ctl00$cplMain$ddSearchBy': 'Permit_Main.ISSUED',
            'ctl00$cplMain$ddSearchOper': 'EQUALS',
            'ctl00$cplMain$txtSearchString': str(start_date),
            'ctl00$ucLogin$hfDashboardRedirect': 'https://etrakit.champaignil.gov/etrakit/dashboard.aspx','ctl00$ucLogin$hfCartRedirect': 'https://etrakit.champaignil.gov/etrakit/ShoppingCart.aspx?iscartview=true','ctl00$ucLogin$hfViewEditProfile': 'static value','ctl00$ucLogin$hfHome': 'https://etrakit.champaignil.gov/etrakit/default.aspx','__EVENTTARGET': 'ctl00$cplMain$rgSearchRslts','__EVENTARGUMENT': str(value),'__VIEWSTATE': response.text.split('__VIEWSTATE|')[1].split('|')[0],'__VIEWSTATEGENERATOR': '2A136539','__ASYNCPOST': 'true','RadAJAXControlID': 'ctl00_RadAjaxManager1'}
            parse_response=yield scrapy.FormRequest(url='https://etrakit.champaignil.gov/etrakit/Search/permit.aspx',method='POST',formdata=form_data_link,dont_filter=True,meta={'permit_lic_no':permit_lic_no,'optional':{'permit_lic_no':permit_lic_no}})
            permit_lic_no=parse_response.meta['permit_lic_no']
            permit_subtype=parse_response.xpath('//*[@id="cplMain_ctl08_lblPermitType"]/text()').extract_first()
            subtype=parse_response.xpath('//*[@id="cplMain_ctl08_lblPermitSubtype"]/text()').extract_first()
            permit_lic_desc=parse_response.xpath('//*[@id="cplMain_ctl08_lblPermitDesc"]/text()').extract_first()
            permit_lic_status=parse_response.xpath('//*[@id="cplMain_ctl08_lblPermitStatus"]/text()').extract_first()
            permit_applied_date=parse_response.xpath('//*[@id="cplMain_ctl08_lblPermitAppliedDate"]/text()').extract_first()
            approved_date=parse_response.xpath('//*[@id="cplMain_ctl08_lblPermitApprovedDate"]/text()').extract_first()
            permit_lic_eff_date=parse_response.xpath('//*[@id="cplMain_ctl08_lblPermitIssuedDate"]/text()').extract_first()
            finaled_date=parse_response.xpath('//*[@id="cplMain_ctl08_lblPermitFinaledDate"]/text()').extract_first()
            permit_lic_exp_date=parse_response.xpath('//*[@id="cplMain_ctl08_lblPermitExpirationDate"]/text()').extract_first()
            notes1=parse_response.xpath('//*[@id="cplMain_ctl08_lblPermitNotes"]/text()').extract()
            notes=self.val_strip(','.join(notes1))
            loc_addr_1=parse_response.xpath('normalize-space(//*[@id="cplMain_ctl09_hlSiteAddress"]/text())').extract_first()
            loc_addr_2=parse_response.xpath('normalize-space(//*[@id="cplMain_ctl09_lblSiteCityStateZip"]/text())').extract_first()
            if loc_addr_2 and len(loc_addr_2)>2:
                location_address_string=loc_addr_1+', '+loc_addr_2
            elif loc_addr_1 and len(loc_addr_1)>2:
                location_address_string=loc_addr_1+', IL'
            else:
                location_address_string='IL'
            if 'IL,' in location_address_string:
                location_address_string=location_address_string.replace('IL,','IL')
            else:
                location_address_string=location_address_string
            property_type=parse_response.xpath('//*[@id="cplMain_ctl09_lblPropertyType"]/text()').extract_first()
            apn=parse_response.xpath('//*[@id="cplMain_RadPageViewSiteInfo"]//tr[4]//a/text()').extract_first()
            permit_lic_fee=parse_response.xpath('//*[@id="cplMain_ctl11_lblTotalFees"]/text()[2]').extract_first()
            insp_list=[]
            insp=parse_response.xpath('//*[@id="ctl00_cplMain_ctl12_rgInspectionInfo_ctl00"]//tr')[1:]
            stories=year_built=''
            for ins in insp:
                inspection_subtype=ins.xpath('td[1]/text()').extract_first()
                inspection_pass_fail=ins.xpath('td[2]/text()').extract_first()
                scheduled_date=ins.xpath('td[3]/span/text()').extract_first()
                inspection_date=ins.xpath('td[5]/text()').extract_first()
                inspection_type='building_inspection'
                insp_dict={'inspection_subtype':inspection_subtype,'inspection_pass_fail':inspection_pass_fail,'scheduled_date':scheduled_date,'inspection_date':inspection_date,'inspection_type':inspection_type}
                insp_list.append(insp_dict)
            data_pass={'permit_lic_no':permit_lic_no,'mixed_name':'','mixed_contractor_name':'','permit_subtype':permit_subtype,'subtype':subtype,'permit_lic_desc':permit_lic_desc,'permit_lic_status':permit_lic_status,'permit_applied_date':permit_applied_date,'approved_date':approved_date,'permit_lic_eff_date':permit_lic_eff_date,'finaled_date':finaled_date,'permit_lic_exp_date':permit_lic_exp_date,'notes':notes,'location_address_string':self.val_strip(location_address_string),'property_type':property_type,'apn':apn,'person_address_string':'','mixed_subtype':'','inspection_subtype':'','inspection_pass_fail':'','scheduled_date':'','inspection_date':'','inspection_type':'','permit_type':'','stories':'','year_built':'','dba_name':'','permit_lic_fee':permit_lic_fee,'contractor_address_string':'','contractor_dba':''}
            check=0
            mixed_table=parse_response.xpath('//*[@id="ctl00_cplMain_ctl10_rgContactInfo_ctl00"]//tr[@class="rgRow" or @class="rgAltRow"]')
            name_list = []
            for mixed in mixed_table:
                mixed_subtype=mixed.xpath('td[1]/text()').extract_first()
                if mixed_subtype and 'OWNER' in mixed_subtype:
                    dup_key='A'
                elif mixed_subtype and 'CONTRACTOR' in mixed_subtype:
                    dup_key='B'
                elif mixed_subtype and 'HVAC' in mixed_subtype:
                    dup_key='C'
                elif mixed_subtype and 'PLUMBING' in mixed_subtype:
                    dup_key='D'
                elif mixed_subtype and 'ELECTRICAL' in mixed_subtype:
                    dup_key='E'
                elif mixed_subtype and 'SPRINKLER' in mixed_subtype:
                    dup_key='F'
                elif mixed_subtype and 'TENANT' in mixed_subtype:
                    dup_key='G'
                elif mixed_subtype and 'ARCHITECT' in mixed_subtype:
                    dup_key='H'
                elif mixed_subtype and 'APPLICANT' in mixed_subtype:
                    dup_key='I'
                else:
                    dup_key='J'
                name=mixed.xpath('td[2]/text()').extract_first()
                mixed_name=self._getDBA(name)[0]
                dba_name=self._getDBA(name)[1]
                person_add1=mixed.xpath('td[5]/text()').extract_first()
                person_add2=mixed.xpath('td[6]/text()').extract_first()
                if person_add2 and len(person_add2)>2:
                    person_address_string=person_add1+', '+person_add2
                elif person_add1 and len(person_add1)>2:
                    person_address_string=person_add1+', IL'
                else:
                    person_address_string='IL'
                mixed_dict = {'mixed_name':mixed_name,'dba_name':dba_name, 'mixed_subtype':mixed_subtype,'person_address_string':person_address_string,'dup_key':dup_key}
                name_list.append(mixed_dict)
            name_list_val = []
            if len(name_list) > 0:
                df =pd.DataFrame([i for i in name_list if i]).drop_duplicates().sort_values('dup_key', ascending=True).fillna('')
                def joinFunc(g, column):
                    val = [str(each) for each in g[column] 
                         if pd.notnull(each) and str(each)]
                    if val:
                        return val[0]
                    return ''
                groups = df.groupby('mixed_name', as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df = groups.apply(groupFunct).fillna('').reset_index(drop=True)
                name_list_val = df.to_dict('records')  
                if len(name_list_val) > 0:
                    for name_val in name_list_val:
                        if name_val['mixed_name'] and 'CONTRACTOR' not in name_val['mixed_subtype']:
                            own_dict={}
                            own_dict=data_pass.copy()
                            own_dict.update(name_val)
                            check=1
                            yield self.save_to_csv(response, **own_dict)                   
                        if 'CONTRACTOR' in name_val['mixed_subtype']:
                            cont_dic={}
                            cont_dic=data_pass.copy()
                            cont_dic['mixed_contractor_name']=name_val['mixed_name']
                            cont_dic['contractor_address_string']=name_val['person_address_string']
                            cont_dic['contractor_dba']=name_val['dba_name']
                            cont_dic['mixed_subtype']=''
                            check=3
                            yield self.save_to_csv(parse_response, **cont_dic)
            if insp_list and len(insp_list)>0:
                for inspect in insp_list:
                    ins_dict={}
                    ins_dict=data_pass.copy()
                    ins_dict.update(inspect)
                    check=4
                    yield self.save_to_csv(parse_response, **ins_dict)
            if check==0:
                yield self.save_to_csv(parse_response, **data_pass)
        page=response.xpath('//*[@id="ctl00_cplMain_rgSearchRslts_ctl00"]/tfoot//tr[5]/td/span/text()').extract_first()
        if page:
            next_page=page.split(' of ')[1]
            head={'Accept': '*/*','Connection': 'keep-alive','Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8','origin': 'https://etrakit.champaignil.gov','referer': 'https://etrakit.champaignil.gov/etrakit/Search/permit.aspx','user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.80 Safari/537.36'}
            if int(page_no) < int(next_page)+1:
                page_data={'ctl00$RadScriptManager1': 'ctl00$ctl00$cplMain$rgSearchRsltsPanel|ctl00$cplMain$rgSearchRslts','RadScriptManager1_TSM': ';;System.Web.Extensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35:en-US:b7585254-495e-4311-9545-1f910247aca5:ea597d4b:b25378d2;Telerik.Web.UI, Version=2013.2.717.40, Culture=neutral, PublicKeyToken=121fae78165ba3d4:en-US:0507d587-20ad-4e22-b866-76bd3eaee2df:16e4e7cd:ed16cbdc:f7645509:24ee1bba:92fe8ea0:f46195d3:fa31b949:874f8ea2:19620875:490a9d4e:bd8f85e4:b7778d6c:58366029:e330518b:1e771326:8e6f0d33:6a6d718d;','ctl00_ucLogin_rwmLogin_ClientState': '','ctl00$ucLogin$hfDashboardRedirect': 'https://etrakit.champaignil.gov/etrakit/dashboard.aspx','ctl00$ucLogin$hfCartRedirect': 'https://etrakit.champaignil.gov/etrakit/ShoppingCart.aspx?iscartview=true','ctl00$ucLogin$hfViewEditProfile': 'static value','ctl00$ucLogin$hfHome': 'https://etrakit.champaignil.gov/etrakit/default.aspx','ctl00$ucLogin$hfSetupAnAccountForPublic': 'https://etrakit.champaignil.gov/etrakit/publicUserAccount.aspx?action=npa','ctl00$ucLogin$hfSetupAnAccountForContractor': 'https://etrakit.champaignil.gov/etrakit/RegistrationConfirmation.aspx','ctl00$ucLogin$hfContractorCSLBVerification': 'DISABLED','ctl00$ucLogin$ddlSelLogin': 'Contractor','ctl00$ucLogin$ddlSelContractor':' A. G','ctl00$ucLogin$RadTextBox2': 'Password','ctl00_ucLogin_RadTextBox2_ClientState': '{"enabled":true,"emptyMessage":"Password","validationText":"","valueAsString":"","lastSetTextBoxValue":"Password"}','ctl00$ucLogin$txtPassword': '','ctl00_ucLogin_txtPassword_ClientState': '{"enabled":true,"emptyMessage":"","validationText":"","valueAsString":"","lastSetTextBoxValue":""}','ctl00$hfGoogleKey': '','ctl00$cplMain$activeTab': '','ctl00$cplMain$hfActivityMode': '',
                'ctl00$cplMain$ddSearchBy': 'Permit_Main.ISSUED',
                'ctl00$cplMain$ddSearchOper': 'EQUALS',
                'ctl00$cplMain$txtSearchString': str(start_date),
                'ctl00_cplMain_rgSearchRslts_ClientState': '','ctl00_cplMain_tcSearchDetails_ClientState': '{"selectedIndexes":["2"],"logEntries":[],"scrollState":{}}','ctl00_cplMain_RadMultiPageSearch_ClientState': '','ctl00_cplMain_rw_ClientState': '','__EVENTTARGET': 'ctl00$cplMain$rgSearchRslts','__EVENTARGUMENT': 'FireCommand:ctl00$cplMain$rgSearchRslts$ctl00;Page;next','__LASTFOCUS': '','__VIEWSTATE': response.text.split('__VIEWSTATE|')[1].split('|')[0],'__VIEWSTATEGENERATOR': '2A136539','__ASYNCPOST': 'true','RadAJAXControlID': 'ctl00_RadAjaxManager1'}
                yield scrapy.FormRequest(url='https://etrakit.champaignil.gov/etrakit/Search/permit.aspx', method='POST', callback=self.parse_details,headers=head, dont_filter=True, meta={'page':int(page_no)+1,'start_date':start_date}, formdata=page_data)
            else:
                yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)
        else:
                yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)
    def save_to_csv(self, response, **meta):
        il = ItemLoader(item=IlChampaignBuildingPermitsSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('url', 'http://etrakit.ci.champaign.il.us/etrakit3/Search/permit.aspx')
        il.add_value('sourceName', 'IL_Champaign_Building_Permits')
        il.add_value('finaled date', meta['finaled_date'])
        il.add_value('inspection_date', meta['inspection_date'])
        il.add_value('contractor_dba', meta['contractor_dba'])
        il.add_value('mixed_contractor_name', meta['mixed_contractor_name'])
        il.add_value('dba_name', meta['dba_name'])
        il.add_value('apn', meta['apn'])
        il.add_value('permit_lic_fee', meta['permit_lic_fee'])
        il.add_value('location_address_string', meta['location_address_string'])
        il.add_value('person_address_string', meta['person_address_string'])
        il.add_value('subtype', meta['subtype'])
        il.add_value('permit_subtype', meta['permit_subtype'])
        il.add_value('inspection_subtype', meta['inspection_subtype'])
        il.add_value('mixed_subtype', meta['mixed_subtype'])
        il.add_value('contractor_address_string', meta['contractor_address_string'])
        il.add_value('permit_lic_status', meta['permit_lic_status'])
        il.add_value('permit_lic_exp_date', meta['permit_lic_exp_date'])
        il.add_value('permit_lic_no', meta['permit_lic_no'])
        il.add_value('notes', meta['notes'])
        il.add_value('property type', meta['property_type'])
        il.add_value('mixed_name', meta['mixed_name'])
        il.add_value('inspection_pass_fail', meta['inspection_pass_fail'])
        il.add_value('approved date', meta['approved_date'])
        il.add_value('permit_lic_eff_date', meta['permit_lic_eff_date'])
        il.add_value('permit_applied_date', meta['permit_applied_date'])
        il.add_value('scheduled date', meta['scheduled_date'])
        il.add_value('permit_lic_desc', meta['permit_lic_desc'] if meta['permit_lic_desc'] and len(meta['permit_lic_desc']) > 2 else meta['permit_subtype'] if meta['permit_subtype'] and len(meta['permit_subtype']) > 2 else 'Building Permit')
        il.add_value('inspection_type', meta['inspection_type'])
        il.add_value('permit_type', 'building_permit')
        return il.load_item()
    def val_strip(self,value):
        if value:
            try:
                str_value = value.replace('&','').replace('#', '')
                str_value = re.sub('\s+', ' ', str_value)
                return str_value.strip()
            except:
                return ''
        else:
            return ''