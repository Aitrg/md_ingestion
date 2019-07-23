# -*- coding: utf-8 -*-

'''
Created on 2018-Dec-10 06:47:04
TICKET NUMBER -AI_578
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_578.items import NvClarkBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from Data_scuff.utils.searchCriteria import SearchCriteria
import scrapy
import datetime
import re
import pandas as pd
import io
import requests
from inline_requests import inline_requests
from scrapy.selector.unified import Selector
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils
from scrapy.http.request.form import _get_form, _get_inputs
import operator
import itertools
import datetime
from scrapy.shell import inspect_response

class NvClarkBuildingPermitsSpider(CommonSpider):
    name = '578_nv_clark_building_permits'
    allowed_domains = ['clarkcountynv.gov']
    start_urls = ['https://citizenaccess.clarkcountynv.gov/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=Building']
    main_url='https://citizenaccess.clarkcountynv.gov/CitizenAccess/'
    modifyformdata = {}
    form_url = None
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-578_Permits_Buildings_NV_Clark_20050101_20190101'),
        'JIRA_ID':'AI_578',
        'DOWNLOAD_DELAY':.5,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('NvClarkBuildingPermitsSpider'),
        'TOP_HEADER':{                        'Conditions': '',
                         'location_address_string': 'Work Location',
                         'mixed_contractor_address_string': 'Contractor Address',
                         'mixed_contractor_name': 'Contractor Name',
                         'mixed_contractor_phone': 'Contractor Phone',
                         'mixed_name': 'Owner Name',
                         'mixed_phone': 'Owner Phone',
                         'mixed_subtype': '',
                         'occupancy_subtype': 'Application Type ',
                         'parcel number': 'Parcel Number',
                         'permit_lic_desc': 'Project Description',
                         'permit_lic_eff_date': 'Date',
                         'permit_lic_exp_date': 'Plan Expiration',
                         'permit_lic_no': 'Permit Number',
                         'permit_lic_status': 'Permit/Complaint Status',
                         'permit_lic_value': 'Job Value($)',
                         'permit_subtype': 'Permit Type',
                         'permit_type': '',
                         'person_address_string': 'Owner Address',
                         'project name': 'Project Name',
                         'short notes': 'Short Notes'},
        'FIELDS_TO_EXPORT':['permit_lic_eff_date',
                         'permit_lic_no',
                         'permit_subtype',
                         'Conditions',
                         'permit_lic_desc',
                         'occupancy_subtype',
                         'permit_lic_value',
                         'project name',
                         'permit_lic_status',
                         'location_address_string',
                         'parcel number',
                         'short notes',
                         'permit_lic_exp_date',
                         'mixed_contractor_name',
                         'mixed_contractor_address_string',
                         'mixed_contractor_phone',
                         'mixed_name',
                         'mixed_subtype',
                         'person_address_string',
                         'mixed_phone',
                         'permit_type',
                         'url',
                         'sourceName',
                         'ingestion_timestamp'],
        'NULL_HEADERS':['project name', 'parcel number', 'short notes']
        }
    check = True
    def parse(self, response):
        if self.check:
            self.start = datetime.datetime.strptime(self.start, '%Y%m%d').strftime('%m/%d/%Y')
            self.end = datetime.datetime.strptime(self.end, '%Y%m%d').strftime('%m/%d/%Y')
            self.check = False

        form = _get_form(response, formname=None, formid='aspnetForm', formnumber=0, formxpath=None)
        formdata = _get_inputs(form, formdata={'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate': self.start,
                                               'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate': self.end,
                                               '__EVENTTARGET': 'ctl00$PlaceHolderMain$btnNewSearch'},
                               clickdata=None, dont_click=False, response=response)
        self.modifyformdata = dict(formdata)
        self.form_url = response.url
        yield scrapy.FormRequest(url=response.url, method='POST',
                                       formdata=formdata,
                                       dont_filter=True,
                                       errback=self.handle_form_error,
                                       callback=self.form_postresponse)

    def form_postresponse(self, response):
        form_args = JavaScriptUtils.getValuesFromdoPost(response.xpath("//*[@id='ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_gdvPermitListtop4btnExport']/@href").extract_first())
        form = _get_form(response, formname=None, formid='aspnetForm', formnumber=0, formxpath=None)
        formdata = _get_inputs(form, formdata={
                    '__EVENTARGUMENT': form_args['__EVENTARGUMENT'],
                    '__ASYNCPOST': 'true',
                    '__EVENTTARGET': form_args['__EVENTTARGET'],
                    'ctl00$ScriptManager1': 'ctl00$PlaceHolderMain$dgvPermitList$updatePanel|{}'.format(form_args['__EVENTTARGET'])
                }, clickdata=None, dont_click=False, response=response)
        yield scrapy.FormRequest(url=response.url, method='POST',
                                       formdata=formdata,
                                       dont_filter=True,
                                       errback=self.handle_form_error,
                                       callback=self.response_download)
    
    def response_download(self, _):
        now = datetime.datetime.now()
        yield scrapy.Request('{}Export2CSV.ashx?flag={}'.format(self.main_url,
                                                                '%02d%02d' % (now.second, now.minute)),
                                  callback=self.getCSVFileReponse, meta={'page':1})
     
    @inline_requests
    def getCSVFileReponse(self, response):
        print ('=====================================')
        rawData = pd.read_csv(io.StringIO(response.text)).fillna('')
        count = 0
        for _row2 in rawData.to_dict('records'):
            print(_row2.get('Permit Number'))
            _row2['Record Number']= '001166-19PA'
            getLandingPage_res = yield scrapy.Request(self.start_urls[0], meta={'optional':{'Permit_Number':_row2['Permit Number']},
                                                             'cookiejar': _row2['Permit Number'], 'data':_row2},
                                                       dont_filter=True)
           
            _row1 = getLandingPage_res.meta['data']
     
            update_formdata = {'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate': self.start,
                                                   'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate': self.end,
                                                   '__EVENTTARGET': 'ctl00$PlaceHolderMain$btnNewSearch',
                                                   'ctl00$PlaceHolderMain$generalSearchForm$txtGSPermitNumber':_row1['Permit Number']}

            getPermitLic_View_res = yield scrapy.FormRequest(url=self.form_url, method='POST',
                                                   formdata={**self.modifyformdata, **update_formdata},
                                                   meta={'optional':{'Permit_Number':_row1['Permit Number']}, 'data':_row1},
                                                   dont_filter=True)

            check_insert = 0
            _row = getPermitLic_View_res.meta['data']
            
            project_description=getPermitLic_View_res.xpath('//span[contains(text(),"Project Description:")]/ancestor::div[1]//td/text()').extract_first()
           
            occupancy_subtype=getPermitLic_View_res.xpath(' //*[contains(text(), "Application Type - ")]/ancestor::*/following-sibling::*/span/text()').extract_first()

            condition=getPermitLic_View_res.xpath('//span[@id="ctl00_PlaceHolderMain_capConditions_lblNotice"]//text()').extract()
            condition=''.join(condition)




            print('\n\n')
            print('--------------------',condition)

            permit_lic_value=getPermitLic_View_res.xpath('//h2[contains(text(),"Job Value($):")]/ancestor::span/following-sibling::span/text()').extract_first()
            if permit_lic_value:
                permit_lic_value=permit_lic_value.replace('$','')
            parcel_number=getPermitLic_View_res.xpath('//h2[contains(text(),"Parcel Number:")]/following-sibling::div/text()').extract_first()

            address_string=getPermitLic_View_res.xpath('//*[@id="tbl_worklocation"]//tr/td[2]//text()').extract()
            address_string1=','.join(address_string)
            address_string1=address_string1.rstrip(',')
            location_address_string=address_string1 if self.state_list(address_string1) else re.sub(r'(\d+)$', r'NV \1', address_string1) 
            location_address_string=location_address_string.replace(',','') 
            location_address_string=location_address_string+', NV' if location_address_string else 'NV'
            # if parcel_number is None:
            #     location_address_string=''
            # else:
            #     location_address_string=location_address_string
            rm=lambda data:'' if data is None else re.sub('\s+',' ',data)
            
            
            contractor_name=getPermitLic_View_res.xpath('//table[@id="tbl_licensedps"]//tr/td[2]/text()').extract_first()
            contractor_address=''
            contractor_phone=''
            if contractor_name:
                contractor_add=getPermitLic_View_res.xpath('//table[@id="tbl_licensedps"]//tr//td//text()').extract()[2:6]
                 
                contractor_address1=[ x for x in contractor_add if 'Phone' not in x and 'Contractor' not in x and 'Neveda' not in x and 'Owner' not in x and 'View' not in x]
                contractor_address2=''.join(contractor_address1).replace(',United States','').replace('ATTN: ','').replace('(702) 275-3512','')
                contractor_address=''.join(self.split(contractor_address2,',',-1))
             
                print('\n\n')
                print('____________________:',contractor_address)
 

                
                contractor_phone=getPermitLic_View_res.xpath('//td[contains(text(),"Home Phone:")]/following::div[@class="ACA_PhoneNumberLTR"]/text()').extract_first()
           
            owner_name=getPermitLic_View_res.xpath('///span[contains(text(),"Owner:")]//following::tr[2]/td/text()').extract_first()
            owner_address=''
            owner_phone=''
            mixed_subtype=''

            if owner_name:
                if owner_name:
                    mixed_subtype='Owner'
                else:
                    mixed_subtype=''
                owner_add=getPermitLic_View_res.xpath('//span[contains(text(),"Owner:")]/ancestor::td//following-sibling::tr/td/text()').extract()[:-1]
                owner_address=','.join(owner_add)
             
            il = ItemLoader(item=NvClarkBuildingPermitsSpiderItem(),response=getPermitLic_View_res)

            il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
            il.add_value('permit_type', 'Building_Permit')
            il.add_value('sourceName', 'WA_King_Seattle_Building_Permits')
            il.add_value('url', 'https://cosaccela.seattle.gov/portal/Cap/CapHome.aspx?module=DPDPermits&TabName=DPDPermits')
            il.add_value('permit_lic_no', _row.get('Permit Number', ''))
            il.add_value('permit_subtype', _row.get('Permit Type', ''))
            il.add_value('permit_lic_status', _row.get('Status', ''))
            il.add_value('project name', _row.get('Project Name',''))
            il.add_value('permit_lic_eff_date',_row.get('Date',''))
            il.add_value('permit_lic_desc',_row.get('Description') if _row.get('Description') else  _row.get('Permit Type'))
            il.add_value('short notes',_row.get('Short Notes',''))
            il.add_value('occupancy_subtype',occupancy_subtype)
            il.add_value('location_address_string',location_address_string)
            il.add_value('Conditions',condition)
            il.add_value('permit_lic_value',permit_lic_value)
            il.add_value('parcel number',parcel_number)
            il.add_value('mixed_contractor_name',contractor_name)
            il.add_value('mixed_contractor_address_string',contractor_address)
            il.add_value('mixed_contractor_phone',contractor_phone)
            il.add_value('mixed_name',owner_name)
            il.add_value('mixed_subtype',mixed_subtype)
            il.add_value('person_address_string',owner_address)
            il.add_value('mixed_phone',owner_phone)
            yield il.load_item()

    def split(self,strng, sep, pos):
          strng = strng.split(sep)
          return sep.join(strng[:pos]), sep.join(strng[pos:])









                         
                         
                         
                         
                         
                         

            # print(mixed_contractor_address.strip())
            # print(mixed_contractor_phone)

            # yield il.load_item()
            # print(_row)










        # count = 0
        # for _row2 in rawData.to_dict('records'):
        #     # _row2['Record Number']= '001166-19PA'
        #     getLandingPage_res = yield scrapy.Request(self.start_urls[0], meta={'optional':{'Record_Number':_row2['Record Number']},
        #                                                      'cookiejar': _row2['Record Number'], 'data':_row2},
        #                                                dont_filter=True)
            
        #     _row1 = getLandingPage_res.meta['data']
        #     print ('_______',_row1)
        #     # _row1['Record Number']= '001166-19PA'
        #     # self.modifyformdata['ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate'] = ''
        #     # self.modifyformdata['ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate'] = ''
        #     update_formdata = {'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate': self.start,
        #                                            'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate': self.end,
        #                                            '__EVENTTARGET': 'ctl00$PlaceHolderMain$btnNewSearch',
        #                                            'ctl00$PlaceHolderMain$generalSearchForm$txtGSPermitNumber':_row1['Record Number']}

            # getPermitLic_View_res = yield scrapy.FormRequest(url=self.form_url, method='POST',
            #                                        formdata={**self.modifyformdata, **update_formdata},
            #                                        meta={'optional':{'Record_Number':_row1['Record Number']}, 'data':_row1},
            #                                        dont_filter=True)

    # def response_download(self, _):
    #   print('&&&&&&&&&&&&&&&&&&&&&')
    #   now = datetime.datetime.now()
    #   yield scrapy.Request('https://citizenaccess.clarkcountynv.gov/CitizenAccess/Cap/CapDetail.aspx?Module=Building&TabName=Building&capID1=REC18&capID2=00000&capID3=01OLS&agencyCode=CLARKCO&IsToShowInspection=',callback=self.parse_lists, meta={'page':1})
    # def response_download(self, _):
    #     now = datetime.datetime.now()
    #     yield scrapy.Request('{}Export2CSV.ashx?flag={}'.format(self.main_url,
    #                                                             '%02d%02d' % (now.second, now.minute)),
    #                               callback=self.getCSVFileReponse, meta={'page':1})
    # # @inline_requests

    # def parse_lists(self,response):
    #     # print('==========',response.text)

    #     rawData = pd.read_csv(io.StringIO(response.text)).fillna('')
    #     print('\n\n\n')
    #     print(rawdata)


        # inspect_response(response,self)
        # first_page_data=response.meta


        # tr_lists=response.xpath('//*[@id="ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList"]/tr')[1:-2]
        # for ind_val, tr in enumerate(tr_lists):
        #   print('\n\n')
        #   print(ind_val)
        #   print(tr)
        #   print('==========================')


            # path=tr.xpath('')
            # com_path='ctl00_PlaceHolderMain_dgvPermitList_gdvPermitList_ctl{}'.format(str(ind_val + 2).zfill(2))
            # record_number=tr.xpath('//*[@id="'+str(com_path)+'_lblPermitNumber1"]/text()').extract_first()
            # if record_number:
            #   link_url=tr.xpath('//*[@id="'+str(com_path)+'_hlPermitNumber"]/@href').extract_first()
            #   rec_type=tr.xpath('//*[@id="'+str(com_path)+'_lblType"]/text()').extract_first()
            #   eff_date=tr.xpath('//*[@id="'+str(com_path)+'_lblUpdatedTime"]/text()').extract_first()
            #   proj_name=tr.xpath('//*[@id="'+str(com_path)+'_lblProjectName"]/text()').extract_first()
            #   short_notes=tr.xpath('//*[@id="'+str(com_path)+'_lblShortNote"]/text()').extract_first()
                # status=tr.xpath('//*[@id="'+str(com_path)+'_lblStatus"]/text()').extract_first()
    #           description = tr.xpath('//*[@id="'+str(com_path)+'_lblDescription"]/text()').extract_first()
    #           href_link = self.main_url+link_url
    #           first_page_data = {'rec_num':record_number,
    #           'href_url':href_link,
    #           'record_type':rec_type,
    #           'effective_date':eff_date,
    #           'project_name':proj_name,
    #           'short_note':short_notes,
    #           'description':description,
    #           'status':status}
    #           if link_url:
    #               rec_num_page = yield scrapy.Request(url= href_link, dont_filter=True, meta=first_page_data)
    #               app_type=parcel_number=''
    #               first_page_data=rec_num_page.meta
    #               data_pass={}
    #               application_type = rec_num_page.xpath('//div/span[contains(text(),"Application Type - ")]/ancestor::div/following-sibling::div/span[@class="ACA_SmLabel ACA_SmLabel_FontSize"]/text()').extract_first()
    #               job_val = rec_num_page.xpath('//div/span/h2[contains(text(),"Job Value($):")]/following::span[1]/text()').extract_first()
    #               condition=rec_num_page.xpath('//*[@id="ctl00_PlaceHolderMain_dvContent"]/div[6]/div[1]').extract_first()
    #               new_condition=self.data_clean(condition)
    #               expiry_date = rec_num_page.xpath('//div/span[contains(text(),"Permit Expiration:")]/following::span[1]/text()').extract_first()
    #               parcel_num_header = rec_num_page.xpath('//*[@id="ctl00_PlaceHolderMain_PermitDetailList1_palParceList"]/div[1]/h2').extract_first()
    #               parcel_num_head = self.data_clean(parcel_num_header)
    #               if parcel_num_head:
    #                   parcel_num=rec_num_page.xpath('//*[@id="ctl00_PlaceHolderMain_PermitDetailList1_palParceList"]/div[1]/div').extract_first()
    #                   parcel_number=self.data_clean(parcel_num)
    #               work_location1 = rec_num_page.xpath('//*[@id="tbl_worklocation"]//tr/td[2]/span').extract_first()
    #               work_location = self.data_clean(work_location1)
    #               if 'NV' in work_location:
    #                   work_location = work_location
    #               else:
    #                   work_location = work_location + ', NV'


    #               all_list = []
    #               owners_list = []
    #               tenaent_list =[]
    #               owner_comp_name = own_address = proj_desc =''
    #               det_td_lists = rec_num_page.xpath('//*[@id="ctl00_PlaceHolderMain_PermitDetailList1_TBPermitDetailTest"]/tr/td')

    #               for td in det_td_lists:
    #                   header_lbl = td.xpath('div/h1/span/text()').extract_first()
    #                   data_lbl = td.xpath('div/span/table/tr/td').extract()

    #                   if 'Contractor' in header_lbl:
    #                       for lic_value in data_lbl[1:]:
    #                           if len(self.data_clean(lic_value)) > 2:
    #                               mixed_cont_name = prof_lic_addr = lic_phone =''
    #                               lic_value_list = lic_value.split('<br>')
    #                               for lic_ind, lic_split_value in enumerate(lic_value_list):
    #                                   if '<tr>' in lic_split_value:
    #                                       tr_values = lic_split_value.split('</tr>')
    #                                       for tr_val in tr_values:
    #                                           geting_values = self.data_clean(tr_val)
    #                                           if 'Phone' in tr_val:
    #                                               lic_phone = (lic_phone + '; ' + geting_values) if len(lic_phone) > 2 and lic_phone != geting_values else geting_values
    #                                               if lic_phone:
    #                                                   if '; 00' in lic_phone:
    #                                                       lic_phone = lic_phone.replace('; 00','')
    #                                                   else:
    #                                                       lic_phone = lic_phone

    #                                   elif lic_ind == 0:
    #                                       mixed_cont_name = self.data_clean(lic_split_value)
    #                                   elif lic_ind > 0:
    #                                       checking_val = self.data_clean(lic_split_value)
    #                                       if checking_val == mixed_cont_name:
    #                                           pass

    #                                       else:
    #                                           prof_lic_addr = (prof_lic_addr + ', ' + checking_val) if (len(checking_val) > 2 and len(prof_lic_addr) > 2) else (prof_lic_addr+' '+checking_val)

    #                                   if len(lic_value_list) == lic_ind+1:
    #                                       add_dict ={'mixed_contractor_name':mixed_cont_name, 'mixed_contractor_address_string':prof_lic_addr, 'mixed_contractor_phone':lic_phone}
    #                                       tenaent_list.append(add_dict)

                        
    #                   elif 'Owner:' in header_lbl:
    #                       for owner_values in data_lbl[1:]:
    #                           owner_list = owner_values.split('</tr>')
    #                           if len(owner_list) > 1:
    #                               for own_ind, own_value in enumerate(owner_list):
    #                                   if own_ind == 0:
    #                                       owner_comp_name = self.data_clean(own_value)
    #                                   elif len(self.data_clean(own_value)) > 2:
    #                                       own_address = (own_address + ', ' + self.data_clean(own_value)) if len(own_address) > 2  else self.data_clean(own_value) 
    #                                       if 'NV' in own_address or ' NV' in own_address:
    #                                           own_address = own_address
    #                                       else:
    #                                           own_address = own_address + ', NV'

    #                                   if len(owner_list) == own_ind+1:
    #                                       add_dict1 ={'mixed_name':owner_comp_name, 'person_address_string':own_address, 'mixed_subtype':'Owner'}
    #                                       owners_list.append(add_dict1)

    #               data_pass={'permit_lic_eff_date':first_page_data['effective_date'], 'permit_lic_no': first_page_data['rec_num'], 'permit_subtype':first_page_data['record_type'], 'Conditions':new_condition, 'permit_lic_desc': first_page_data['description'],       'occupancy_subtype':application_type, 'permit_lic_value':job_val,'project name': first_page_data['project_name'],'permit_lic_status': first_page_data['status'],'location_address_string':work_location,'parcel number':parcel_number,'short notes': first_page_data['short_note'],'permit_lic_exp_date':expiry_date,'mixed_contractor_name':'','mixed_contractor_address_string':'','mixed_contractor_phone':'','mixed_name':'','mixed_subtype':'','person_address_string':'','mixed_phone':'','permit_type':''}

    #               if len(owners_list) > 0:
    #                   for dict_value in owners_list:
    #                       own={**data_pass, **dict_value}
    #                       yield self.save_to_csv(response, **own)

    #               if len(tenaent_list) > 0:
    #                   for dect_value in tenaent_list:
    #                       con={**data_pass, **dect_value}
    #                       yield self.save_to_csv(response, **con)
    #           else:
    #               data_pass = {'permit_lic_eff_date':first_page_data['effective_date'], 'permit_lic_no': first_page_data['rec_num'], 'permit_subtype':first_page_data['record_type'], 'Conditions':'', 'permit_lic_desc':first_page_data['description'],'occupancy_subtype':'', 'permit_lic_value':'','project name': first_page_data['project_name'],'permit_lic_status':status,'location_address_string':'','parcel number':'','short notes': first_page_data['short_note'],'permit_lic_exp_date':'','mixed_contractor_name':'','mixed_contractor_address_string':'','mixed_contractor_phone':'','mixed_name':'','mixed_subtype':'','person_address_string':'','mixed_phone':'','permit_type':''}
    #               yield self.save_to_csv(response, **data_pass)
    #           # break

    #   # pagination
    #   next_page = response.xpath('//td[@class="aca_pagination_td aca_pagination_PrevNext"]/a[contains(text(),"Next >")]/@href').extract_first()
    #   # print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++",next_page,response.meta)

    #   if next_page:
    #       form_args = JavaScriptUtils.getValuesFromdoPost(next_page)
    #       if response.meta['page'] == 1:
    #           form_data_page1 = {
    #           '__EVENTTARGET': form_args['__EVENTTARGET'],
    #           '__ASYNCPOST': 'true',
    #           'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate': response.meta['start_date'],
    #           'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate': response.meta['end_date'],
    #           '__EVENTARGUMENT': form_args['__EVENTARGUMENT'],
    #           'ctl00$ScriptManager1': 'ctl00$PlaceHolderMain$dgvPermitList$updatePanel|{}'.format(form_args['__EVENTTARGET'])}
    #           yield scrapy.FormRequest.from_response(response, method='POST', formname='aspnetForm', dont_filter=True,
    #                                                  meta={'url':response.meta['url'], 'page':response.meta['page'] + 1, 'start_date':response.meta['start_date'], 'end_date':response.meta['end_date']},formdata=form_data_page1, callback=self.parse_lists)
    #       else:
                
    #           form_data_page2 = {
    #               '__EVENTARGUMENT': form_args['__EVENTARGUMENT'],
    #               '__VIEWSTATE': response.text.split('__VIEWSTATE|')[1].split('|')[0],
    #               '__ASYNCPOST': 'true',
    #               'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate': response.meta['start_date'],
    #               'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate': response.meta['end_date'],
    #               '__EVENTTARGET': form_args['__EVENTTARGET'],
    #               'ctl00$ScriptManager1': 'ctl00$PlaceHolderMain$dgvPermitList$updatePanel|{}'.format(form_args['__EVENTTARGET'])
    #           }

    #           yield scrapy.FormRequest(url=response.meta['url'], method='POST', formdata=form_data_page2, callback=self.parse_lists,
    #                      meta={'url':response.meta['url'], 'page':response.meta['page'] + 1, 'start_date':response.meta['start_date'], 'end_date':response.meta['end_date']}, dont_filter=True)
    #   elif len(self.search_element) > 0:
    #       yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)


    # def val_strip(self,value):
    #   if value:
    #       try:
    #           str_value = value.replace('&','').replace('#', '').replace(',','')
    #           str_value = re.sub('\s+', ' ', str_value)
    #           return str_value.strip()
    #       except:
    #           return ''
    #   else:
    #       return ''

    # #remove the html tag <> values
    # def data_clean(self, value):
    #   if value:
    #       try:
    #           clean_tags = re.compile('<.*?>')
    #           desc_list = re.sub('\s+', ' ', re.sub(clean_tags, '', value))
    #           desc_list_rep = desc_list.replace('&amp;', '&').replace('Home Phone:','').replace('Mobile Phone:','').replace('Fax:','')
    #           return desc_list_rep.strip()
    #       except:
    #           return ''
    #   else:
    #       return ''

    # def save_to_csv(self, response, **meta):
    #   il = ItemLoader(item=NvClarkBuildingPermitsSpiderItem(),response=response)
    #   il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
    #   #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
    #   il.add_value('sourceName', 'NV_Clark_Building_Permits')
    #   il.add_value('url', 'https://citizenaccess.clarkcountynv.gov/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=Building')
    #   il.add_value('permit_lic_eff_date', meta['permit_lic_eff_date'])
    #   il.add_value('permit_lic_no', meta['permit_lic_no'])
    #   il.add_value('permit_subtype', meta['permit_subtype'])
    #   il.add_value('Conditions', meta['Conditions'])
    #   if meta['permit_lic_desc']:
    #       il.add_value('permit_lic_desc', meta['permit_lic_desc'])
    #   else:
    #       if meta['permit_subtype']:
    #           il.add_value('permit_lic_desc', meta['permit_subtype'])
    #       else:
    #           il.add_value('permit_lic_desc', 'Building Permit')
    #   il.add_value('occupancy_subtype', meta['occupancy_subtype'])
    #   il.add_value('permit_lic_value', meta['permit_lic_value'])
    #   il.add_value('project name', meta['project name'])
    #   il.add_value('permit_lic_status', meta['permit_lic_status'])
    #   il.add_value('person_address_string', meta['person_address_string'] if len(meta['person_address_string']) > 2 else 'NV')
    #   il.add_value('parcel number', meta['parcel number'])
    #   il.add_value('short notes', meta['short notes'])
    #   il.add_value('permit_lic_exp_date', meta['permit_lic_exp_date'])
    #   il.add_value('mixed_contractor_name', meta['mixed_contractor_name'])
    #   il.add_value('mixed_contractor_address_string', meta['mixed_contractor_address_string'])
    #   il.add_value('mixed_contractor_phone', meta['mixed_contractor_phone'])
    #   il.add_value('mixed_name', meta['mixed_name'])
    #   il.add_value('mixed_subtype', meta['mixed_subtype'])
    #   il.add_value('location_address_string', meta['location_address_string'])
    #   il.add_value('mixed_phone', meta['mixed_phone'])
    #   il.add_value('permit_type', 'building_permit')
    #   return il.load_item()

