# -*- coding: utf-8 -*-
'''
Created on 2019-Jul-08 11:24:30
TICKET NUMBER -AI_1410
@author: ait
'''
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import scrapy
from scrapy.http import HtmlResponse
from inline_requests import inline_requests
from scrapy.shell import inspect_response
import datetime
import re
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils
import json
import os
from Data_scuff.spiders.AI_1410.items import IlCookEvanstonBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils

class IlCookEvanstonBuildingPermitsSpider(CommonSpider):
    name = '1410_il_cook_evanston_building_permits'
    allowed_domains = ['cityofevanston.org']
    # start_urls = ['https://permits.cityofevanston.org/CitizenAccess/']
    start_urls=['https://permits.cityofevanston.org/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=HOME']
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1410_Permits_Building_IL_Cook_Evan ston_CurationReady'),
        'JIRA_ID':'AI_1410',
        'COOKIES_ENABLED':True,
        'DOWNLOAD_DELAY':10,
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('il_cook_evanston_building_permits'),
        'TOP_HEADER':{                        'contractor_address_string': 'Address.1',
                         'contractor_dba': '',
                         'contractor_lic_no': '',
                         'contractor_lic_type': '',
                         'contractor_phone': 'Home Phone',
                         'dba_name': '',
                         'person_email':'',
                         'inspection_date': '',
                         'inspection_id': '',
                         'inspection_pass_fail': 'Result',
                         'inspection_subtype': 'INSPECTION TYPE',
                         'inspection_type': '',
                         'inspector_comments': '',
                         'location_address_string': 'Work Location',
                         'mixed_contractor_name': 'Licensed Professional',
                         'mixed_name': 'Name',
                         'contractor_email':'',
                         'mixed_subtype': '',
                         'occupancy_subtype': 'Building Use Type',
                         'parcel number': 'Parcel Number',
                         'permit_lic_desc': 'Project Description',
                         'permit_lic_fee': 'Total paid fees',
                         'permit_lic_no': 'Record',
                         'permit_lic_status': 'Record Status',
                         'permit_subtype': 'Record Type',
                         'permit_type': '',
                         'person_address_string': 'Address',
                         'person_phone': 'Home phone'},
        'FIELDS_TO_EXPORT':[                        
                         'permit_lic_no',
                         'permit_subtype',
                         'permit_lic_status',
                         'location_address_string',
                         'occupancy_subtype',
                         'parcel number',
                         'permit_lic_desc',
                         'permit_lic_fee',
                         'mixed_name',
                         'dba_name',
                         'mixed_subtype',
                         'person_email',
                         'person_address_string',
                         'person_phone',
                         'mixed_contractor_name',
                         'contractor_dba',
                         'contractor_email',
                          'contractor_address_string',
                         'contractor_phone',
                         'contractor_lic_type',
                         'contractor_lic_no',
                         'inspection_id',
                         'inspection_subtype',
                         'inspection_date',
                         'inspection_pass_fail',
                         'inspector_comments',
                         'inspection_type',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp',
                         ],
        'NULL_HEADERS':['parcel number']
        }

    def parse(self, response):
        # a=
        # b=
        self.s = datetime.datetime.strptime(self.start, "%Y%m%d")
        self.e = datetime.datetime.strptime(self.end, "%Y%m%d")
        
        self.acs=response.xpath('//*[@id="ACA_CS_FIELD"]/@value').extract_first()
        # inspect_response(response,self)
        formdata={
                    'ctl00$ScriptManager1': 'ctl00$PlaceHolderMain$updatePanel|ctl00$PlaceHolderMain$btnNewSearch',
                    'ACA_CS_FIELD': response.xpath('//*[@id="ACA_CS_FIELD"]/@value').extract_first(),
                    '__EVENTTARGET': 'ctl00$PlaceHolderMain$btnNewSearch',
                    '__EVENTARGUMENT':'' ,
                    '__LASTFOCUS':'' ,
                    '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                    '__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                    'ctl00$HeaderNavigation$hdnShoppingCartItemNumber': '',
                    'ctl00$HeaderNavigation$hdnShowReportLink': 'N',
                    'ctl00$PlaceHolderMain$addForMyPermits$collection': 'rdoNewCollection',
                    'ctl00$PlaceHolderMain$addForMyPermits$txtName': 'name',
                    'ctl00$PlaceHolderMain$addForMyPermits$txtDesc':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSPermitNumber':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$ddlGSPermitType':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate': str(self.s),
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate_ext_ClientState':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate_watermark_exd_ClientState':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate':str(self.e),
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate_ext_ClientState':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSFirstName':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSLastName':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSBusiName':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl0':'', 
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl0_watermark_exd_ClientState':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl1':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl1_watermark_exd_ClientState':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$ddlGSDirection':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSStreetName':'', 
                    'ctl00$PlaceHolderMain$generalSearchForm$ddlGSStreetSuffix':'', 
                    'ctl00$PlaceHolderMain$generalSearchForm$ddlGSUnitType':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSUnitNo':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSParcelNo':'', 
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSCity':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$ddlGSState$State1':'', 
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit':'', 
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ZipFromAA': '0',
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_zipMask':'' ,
                    'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ext_ClientState':'', 
                    'ctl00$PlaceHolderMain$hfASIExpanded':'' ,
                    'ctl00$PlaceHolderMain$txtHiddenDate':'' ,
                    'ctl00$PlaceHolderMain$txtHiddenDate_ext_ClientState':'' ,
                    'ctl00$PlaceHolderMain$dgvPermitList$lblNeedReBind':'' ,
                    'ctl00$PlaceHolderMain$dgvPermitList$gdvPermitList$hfSaveSelectedItems':'' ,
                    'ctl00$PlaceHolderMain$dgvPermitList$inpHideResumeConf':'' ,
                    'ctl00$PlaceHolderMain$hfGridId':'' ,
                    'ctl00$HDExpressionParam':'', 
                    '__ASYNCPOST': 'true'

        }
        header={
                'Host': 'permits.cityofevanston.org',
                'Origin': 'https://permits.cityofevanston.org',
                'Referer': 'https://permits.cityofevanston.org/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=HOME',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36',
                'X-MicrosoftAjax': 'Delta=true',
                'X-Requested-With': 'XMLHttpRequest'
                }



        yield scrapy.FormRequest(url=response.url,formdata=formdata,headers=header,dont_filter=True,callback=self.parse_next,meta={'page':1},errback=self.parse_error)
    @inline_requests
    def parse_next(self,response):
        page_no=response.meta['page']
        self.lis_spce=lambda data:[dat for dat in filter(lambda strs:strs.replace('[\n\t\r]','').strip(),data)]
        
        res=''
        if isinstance(response,HtmlResponse):
            res=response
        else:
            res = HtmlResponse('https://permits.cityofevanston.org/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=HOME', body=str.encode(response.text))
            self.viewstate = response.text.split('__VIEWSTATE|')[1].split('|')[0]
            self.eventvalidation = response.text.split('__VIEWSTATEGENERATOR|')[1].split('|')[0]
        self.page=res.xpath("//td/span[text()='"+str(page_no)+"']/ancestor::td/following-sibling::td/a/@href").extract_first()
        self.table_links=res.xpath("//tr/td[3]/div/a/@href").extract()
        if self.table_links:
            link=self.table_links.pop(0)
            # link='/CitizenAccess/Cap/CapDetail.aspx?Module=Building&TabName=Building&capID1=18006&capID2=00000&capID3=00486&agencyCode=EVANSTON&IsToShowInspection='
            print("links: ",link)
            yield scrapy.Request(url='https://permits.cityofevanston.org'+link,dont_filter=True,callback=self.parse_data,meta={'page':response.meta['page']})

    @inline_requests               
    def parse_data(self,response):
        page_no=response.meta['page']
        dic={}    
        extract_data=response
        # if 'https://permits.cityofevanston.org/CitizenAccess/Cap/CapDetail.aspx?Module=Building&TabName=Building&capID1=18ADD&capID2=00000&capID3=00025&agencyCode=EVANSTON&IsToShowInspection=' == response.url:
            # inspect_response(extract_data,self)

        dic['permit_lic_no']=extract_data.xpath("//span[text()='Record']/following-sibling::span/text()").extract_first()
        # print(dic)
        if dic['permit_lic_no']:
            dic['permit_type']='building_permit'
            dic['inspection_date']=''
            dic['inspector_comments']=''
            dic['inspection_type']=''
            dic['inspection_pass_fail']=''
            dic['inspection_subtype']=''
            dic['inspection_id']=''
            dic['contractor_lic_no']=''
            dic['contractor_lic_type']=''
            dic['contractor_address_string']=''
            dic['contractor_phone']=''
            dic['mixed_contractor_name']=''
            dic['contractor_dba']=''
            dic['contractor_email']=''


            dic['permit_subtype']=extract_data.xpath("//span[@id='ctl00_PlaceHolderMain_lblPermitType']/text()").extract_first()
            dic['permit_lic_status']=extract_data.xpath("//span[text()='Record Status:']/following-sibling::span/text()").extract_first()
            address=extract_data.xpath("//table[@id='tbl_worklocation'][contains(text(),'')]/ancestor::tr//text()").extract()
            dic['location_address_string']=self.format_address(address)
            dic['occupancy_subtype']=extract_data.xpath("//span[contains(text(),'Building Use Type: ')]/following::span/text()").extract_first()
            dic['parcel number']=extract_data.xpath("//h2[contains(text(),'Parcel Number:')]/following-sibling::div/text()").extract_first()
            permit_lic_desc=extract_data.xpath("//div/h1/span[contains(text(),'Project Description:')]/ancestor::td//text()").extract()
            dic['permit_lic_desc']=' '.join(self.lis_spce(permit_lic_desc)).replace("Project Description:",'')

 #'''_______________________________fees page _____________________________________'''

            try:     
                sc=extract_data.xpath("//div[@class='ACA_Area_CapDetail']/script/text()").extract()[-1]
                ss=sc[sc.find('PageMethods.DisplayFeePaid(')+27:sc.find('callbackFeePaidDetailInfo')-2]
                bdata=(ss.replace('"','')).split(',')
                
                body_data={"pageNum":bdata[0].strip(),"moduleName":bdata[1].strip(),"reportName":bdata[2].strip(),"receiptNbr":bdata[3].strip(),"reportID":bdata[4].strip(),"displayReceiptReport":bdata[5].strip()}

                json_header={
                           "Content-Type": "application/json; charset=UTF-8",
                            "Host": "permits.cityofevanston.org",
                            "Origin": "https://permits.cityofevanston.org",
                            "Referer": str(extract_data.url),
                             "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36",
                }

                fees_json=yield scrapy.Request(url='https://permits.cityofevanston.org/CitizenAccess/Cap/CapDetail.aspx/DisplayFeePaid',dont_filter=True,body=str(body_data),method="POST",headers=json_header)
                fees=json.loads(fees_json.body_as_unicode())
                dic['permit_lic_fee']=''
                if (fees['d']):
                    html=HtmlResponse("https://permits.cityofevanston.org/CitizenAccess/Cap/CapDetail.aspx/DisplayFeePaid", body=str.encode(fees['d']))
                    permit_lic_fee=html.xpath("//strong/i/text()").extract_first()
                    if permit_lic_fee:
                        dic['permit_lic_fee']=permit_lic_fee.replace('Total paid fees: ','')
            except:
                print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
                print('error')
                print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
                pass
# '''_______________________________applicant info ___________________________________'''

            applicantfname=extract_data.xpath("//span[contains(text(),'Applicant:')]/following::span[1]/table//tr/td/div/span[@class='contactinfo_firstname']/text()").extract_first()
            applicantlname=extract_data.xpath("//span[contains(text(),'Applicant:')]/following::span[1]/table//tr/td/div/span[@class='contactinfo_lastname']/text()").extract_first()
            applicantbname=extract_data.xpath("//span[contains(text(),'Applicant:')]/following::span[1]/table//tr/td/div/span[@class='contactinfo_businessname']/text()").extract_first()
            if applicantfname and applicantlname:
                applicant=applicantfname+' '+applicantlname
            else:
                if applicantfname:
                    applicant=applicantfname
                elif applicantlname:
                    applicant=applicantlname
                else:
                    applicant=''

            address=extract_data.xpath("//span[contains(text(),'Applicant:')]/following::span[1]/table//tr/td/div/span[@class='contactinfo_addressline1']/text()").extract_first()
            adddres1=extract_data.xpath("//span[contains(text(),'Applicant:')]/following::span[1]/table//tr/td/div/span[@class='contactinfo_region']/text()").extract()
            if address and adddres1:
                person_address=address+', '+' '.join(adddres1)
            else:
                if address:
                    person_address=address
                elif adddres1:
                    person_address=''.join(adddres1)
                else:
                    person_address=''
            applicantphone=extract_data.xpath("//span[contains(text(),'Applicant:')]/following::span[1]//td[contains(text(),'Home Phone')]/following-sibling::td/div/text()").extract_first()
            applicantemail=extract_data.xpath("//span[contains(text(),'Applicant:')]/following::span[1]//td/text()").extract()
            if applicantemail:
                if '@' in applicantemail[-1]:
                    applicantemailid=applicantemail[-1]
                else:
                    applicantemailid=''         

            if applicant or person_address or applicantphone or applicantemail:
                if applicantbname:
                    names=[applicant,applicantbname]
                else:
                    names=[applicant]
                for name in names:
                    if name:
                        dic['mixed_name']=self._getDBA(name)[0]
                        dic['dba_name']=self._getDBA(name)[1]
                    else:
                        dic['mixed_name']=''
                        dic['dba_name']=''
                    dic['mixed_subtype']='Applicant'
                    dic['person_address_string']=person_address.replace('IL,','IL').replace(', IL','IL').replace(',  IL','IL').replace('IL',', IL').replace('-- 00000','IL').replace(' ,','')
                    # if ', IL' == dic['person_address_string']:dic['person_address_string']='IL'
                    dic['person_phone']=applicantphone
                    dic['person_email']=applicantemailid
                    yield self.save_csv(response,dic).load_item()
            else:
                dic['mixed_name']=''
                dic['dba_name']=''
                dic['mixed_subtype']=''
                dic['person_address_string']=''
                dic['person_phone']=''
                dic['person_email']=''
                # inspect_response(extract_data,self)
                print(dic)
                yield self.save_csv(response,dic).load_item()

 # ''' ____________________________contractor info_______________________________________________ '''  
             
            if extract_data.xpath("//h2[contains(text(),'Contractor information')]"):
                contrac_name=[]
                contrac_fname=extract_data.xpath("//div[@class='ACA_ConfigInfo ACA_FLeft']//span[@class='contactinfo_firstname']/text()").extract_first()
                contrac_lname=extract_data.xpath("//div[@class='ACA_ConfigInfo ACA_FLeft']//span[@class='contactinfo_lastname']/text()").extract_first()
                if contrac_fname and contrac_lname:contrac_name=[contrac_fname+' '+contrac_lname]
                else:
                    if contrac_fname:contrac_name=[contrac_fname]
                    elif contrac_lname:contrac_name=[contrac_fname]
                    else:contrac_name=[]
                contrac_businessname=extract_data.xpath("//div[@class='ACA_ConfigInfo ACA_FLeft']//span[@class='contactinfo_businessname']/text()").extract_first()
                if contrac_businessname:
                    contrac_name.append(contrac_businessname)
                contrac_add1=extract_data.xpath("//div[@class='ACA_ConfigInfo ACA_FLeft']//span[@class='contactinfo_addressline1']/text()").extract()
                contrac_add2=extract_data.xpath("//div[@class='ACA_ConfigInfo ACA_FLeft']//span[@class='contactinfo_region']/text()").extract()
                if contrac_add1 and contrac_add2:
                    contrac_address=' '.join(contrac_add1)+', '+' '.join(contrac_add2).replace(', ',',').replace(',',', ').replace('IL,','IL').replace(', IL','IL').replace(',  IL','IL').replace('IL',', IL')
                    dic['contractor_address_string']=contrac_address
                else:
                    if contrac_add1:
                        contrac_address=contrac_add1
                    elif contrac_add2:
                        contrac_address=(contrac_add2)
                    else:
                        contrac_address=''
                    dic['contractor_address_string']=', '.join(contrac_address).replace("IL,",'IL').replace(",IL","IL").replace("IA,",'IA').replace(",IA","IA").replace(",IN","IN").replace("IN,",'IN').replace(',Home Phone:','').replace(' ,','')
                if contrac_name:
                    for i in contrac_name:
                        dic['mixed_contractor_name']=self._getDBA(i)[0]
                        dic['contractor_dba']=self._getDBA(i)[0]
                        # dic['mixed_subtype']=''
                        # dic['contractor_address_string']=', '.join(contrac_address).replace("IL,",'IL').replace(",IL","IL").replace("IA,",'IA').replace(",IA","IA").replace(",IN","IN").replace("IN,",'IN').replace(',Home Phone:','').replace(' ,','')
                        dic['contractor_phone']=extract_data.xpath("//div[@class='ACA_ConfigInfo ACA_FLeft']//div[@class='ACA_PhoneNumberLTR']/text()").extract_first()
                        dic['contractor_email']=extract_data.xpath("//div[@class='ACA_ConfigInfo ACA_FLeft']//td[contains(text(),'E-mail:')]/following::td/text()").extract_first()
                        print("\n\n\n\n\n\n\n\n\n\n")
                        print(dic['mixed_contractor_name'],'\n',dic['contractor_address_string'],'\n',dic['contractor_phone'],'contractor_email')
                        print("\n\n\n\n\n\n\n\n\n\n")

                        yield self.save_csv(response,dic).load_item()

# '''# _____________________________________owner info ___________________________________'''

            owner=extract_data.xpath("//span[contains(text(),'Owner')]/following::span[1]/table//tr/td/text()").extract()
            if owner:
                name=owner[0]
                owneraddress=owner[1:]
                dic['mixed_name']=self._getDBA(name)[0]
                dic['dba_name']=self._getDBA(name)[1]
                dic['mixed_subtype']='Owner'
                dic['person_address_string']=', '.join(owneraddress).replace('IL,','IL').replace(', IL','IL').replace(',  IL','IL').replace('IL',', IL').replace('-- 00000','IL').replace(',Home Phone:','')
                dic['person_phone']=''
                dic['person_email']=''
                yield self.save_csv(response,dic).load_item()
            

#'''____________________contractor - Licensed Professional:table ______________________'''

            dic['mixed_name']=''
            dic['dba_name']=''
            dic['mixed_subtype']=''
            dic['person_address_string']=''
            dic['person_phone']=''
            dic['person_email']=''
            Licensed=extract_data.xpath("//table[@id='tbl_licensedps']/tr[1]/td/text()").extract()

            if Licensed:
                # inspect_response(extract_data,self)

                licenced_data=self.lis_spce(Licensed)
                licenced_name=licenced_data[:2]
                contractor_lic_no=licenced_data[-1]
                if ' ' in contractor_lic_no:
                    d=contractor_lic_no.split(' ')

                    dic['contractor_lic_no']=d[-1]

                    dic['contractor_lic_type']=' '.join(d[:-1])
                else:
                    dic['contractor_lic_no']=contractor_lic_no
                    dic['contractor_lic_type']=''
                if 'IL-' in dic['contractor_lic_type']:
                    dic['contractor_lic_no']="IL-"+dic['contractor_lic_no'].replace('IL-','').replace('IL','')
                    dic['contractor_lic_type']=dic['contractor_lic_type'].replace('IL-','').replace('IL','')
                    dic['contractor_lic_no']=''
                    dic['contractor_lic_type']=dic['contractor_lic_type']+'GC'
                if 'OWNER' == dic['contractor_lic_no']:
                    dic['contractor_lic_no']=''
                    dic['contractor_lic_type']=dic['contractor_lic_type']+'OWNER'

                licenced_address=(licenced_data[2:-1])

                dic['contractor_address_string']=','.join(licenced_address).replace("IL,",'IL').replace(",IL","IL").replace("IA,",'IA').replace(",IN","IN").replace("IN,",'IN').replace(",IA","IA").replace(',Home Phone:','').replace(' ,','')
                dic['contractor_phone']=extract_data.xpath("//table[@id='tbl_licensedps']//td[contains(text(),'Home Phone:')]/following-sibling::td/div/text()").extract_first()
                dic['contractor_email']=extract_data.xpath("//table[@id='tbl_licensedps']//td[contains(text(),'Email')]/following-sibling::td/div/text()").extract_first()
                for i in licenced_name:
                    dic['mixed_contractor_name']=self._getDBA(i)[0]
                    dic['contractor_dba']=self._getDBA(i)[1]
                    yield self.save_csv(response,dic).load_item()
    
    # '''# <<<<<<<<<<<<<<<<<<<< _________________more contractor ______________________>>>>>>>>>>>>>>>'''

            more_licenced=extract_data.xpath("//a[contains(text(),'View Additional Licensed Professionals')]")
            if more_licenced:
                # inspect_response(response,self)
                more_licenced_table=extract_data.xpath("//tr/td/strong/a[contains(text(),'View Additional Licensed Professionals')]/following::tr[@tips='tr_licenseProfessional']")
                for j in more_licenced_table:
                    more_licence=j.xpath(".//td/text()").extract()

                    if more_licence:
                        licenced_data1=self.lis_spce(more_licence)
                        licenced_name1=licenced_data1[1:3]
                        contractor_lic_no=licenced_data1[-1]
                        if ' ' in contractor_lic_no:
                            d=contractor_lic_no.split(' ')
                            dic['contractor_lic_no']=d[-1]
                            dic['contractor_lic_type']=' '.join(d[:-1])
                        else:
                            dic['contractor_lic_no']=contractor_lic_no
                            dic['contractor_lic_type']=''
                        if 'IL-' in dic['contractor_lic_type']:
                            dic['contractor_lic_no']=dic['contractor_lic_no'].replace('IL-','').replace('IL','')
                            dic['contractor_lic_type']=dic['contractor_lic_type'].replace('IL-','').replace('IL','')
                        if 'GC' == dic['contractor_lic_no']:
                            dic['contractor_lic_no']=''
                            dic['contractor_lic_type']=dic['contractor_lic_type']+'GC'
                        if 'OWNER' == dic['contractor_lic_no']:
                            dic['contractor_lic_no']=''
                            dic['contractor_lic_type']=dic['contractor_lic_type']+'OWNER'

                        licenced_address1=(licenced_data1[3:-2])
                        dic['contractor_phone']=j.xpath(".//td/div/text()").extract_first()
                        dic['contractor_email']=''
                        dic['contractor_address_string']=','.join(licenced_address1).replace("IL,",'IL').replace(",IL","IL").replace("IA,",'IA').replace(",IA","IA").replace(",IN","IN").replace("IN,",'IN').replace(',Home Phone:','').replace(' ,','')
                        for i in licenced_name1:
                            dic['mixed_contractor_name']=i
                            yield self.save_csv(response,dic).load_item()

# '''# _______________________________________inspection parse yiled ________________________________'''

            aca=extract_data.xpath("//div[@id='ctl00_PlaceHolderMain_InspectionList_inspectionUpdatePanel']/a/@href").extract_first()
            EVENTTARGET=aca[aca.find('(new WebForm_PostBackOptions("')+30:aca.find('",')]
            inspection_formdata={
                            'ctl00$ScriptManager1': 'ctl00$PlaceHolderMain$InspectionList$inspectionUpdatePanel|'+str(EVENTTARGET),
                            'ACA_CS_FIELD': self.acs,
                            '__EVENTTARGET': str(EVENTTARGET),
                            '__EVENTARGUMENT':'', 
                            '__VIEWSTATE':extract_data.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(), 
                            '__VIEWSTATEGENERATOR':extract_data.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                            'ctl00$HeaderNavigation$hdnShoppingCartItemNumber':'', 
                            'ctl00$HeaderNavigation$hdnShowReportLink': 'N',
                            'ctl00$PlaceHolderMain$addForDetailPage$collection':'rdoNewCollection',
                            'ctl00$PlaceHolderMain$addForDetailPage$txtName':'', 
                            'ctl00$PlaceHolderMain$addForDetailPage$txtDesc':'', 
                            'ctl00$PlaceHolderMain$attachmentEdit$txtValidateSaveAction':'', 
                            'ctl00$PlaceHolderMain$attachmentEdit$hdfEditingFileIds':'', 
                            'ctl00$PlaceHolderMain$attachmentEdit$fileSelect$hdFinishedFileArray':'', 
                            'ctl00$PlaceHolderMain$attachmentEdit$hdAllFinishedFileArray':'', 
                            'ctl00$HDExpressionParam':'', 
                            '__ASYNCPOST': 'true'
            }
            inspection_header={
                                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                                "Host": "permits.cityofevanston.org",
                                "Origin": "https://permits.cityofevanston.org",
                                "Referer":str(extract_data.url) ,
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36",
                                "X-MicrosoftAjax": "Delta=true",
                                "X-Requested-With": "XMLHttpRequest",
            }
            yield scrapy.FormRequest(extract_data.url,formdata=inspection_formdata,headers=inspection_header,method="POST",dont_filter=True,callback=self.inspection_page,meta={'page':response.meta['page'],'ins':1,'data':dic,'acs':self.acs})

# '''#_______________________________________________nextlink_____________________________________  '''              
        if self.table_links:
                link=self.table_links.pop(0)
                print("links: ",link)
                yield scrapy.Request(url='https://permits.cityofevanston.org'+link,dont_filter=True,callback=self.parse_data,meta={'page':response.meta['page']})
        else:
# '''_______________________________________________________pagination___________________________________'''
            if self.page: 
                page_no=str(int(page_no)+1)
                form_args_pagn = JavaScriptUtils.getValuesFromdoPost(self.page)
                # print('\n\n')
                # print('pageno: ',page_no)
                # print(form_args_pagn)
                # print('\n\n')

                formdata1={
                        'ctl00$ScriptManager1': 'ctl00$PlaceHolderMain$dgvPermitList$updatePanel|'+str(form_args_pagn['__EVENTTARGET']),
                        'ctl00$HeaderNavigation$hdnShoppingCartItemNumber':'', 
                        'ctl00$HeaderNavigation$hdnShowReportLink': 'N',
                        'ctl00$PlaceHolderMain$addForMyPermits$collection': 'rdoNewCollection',
                        'ctl00$PlaceHolderMain$addForMyPermits$txtName': 'name',
                        'ctl00$PlaceHolderMain$addForMyPermits$txtDesc':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSPermitNumber':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$ddlGSPermitType':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate': str(self.s),
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate_ext_ClientState':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate_watermark_exd_ClientState':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate': str(self.e),
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate_ext_ClientState':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSFirstName':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSLastName':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSBusiName':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl0':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl0_watermark_exd_ClientState':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl1':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl1_watermark_exd_ClientState':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$ddlGSDirection':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSStreetName':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$ddlGSStreetSuffix':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$ddlGSUnitType':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSUnitNo':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSParcelNo':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSCity':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$ddlGSState$State1':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ZipFromAA':'0',
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_zipMask':'', 
                        'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ext_ClientState':'', 
                        'ctl00$PlaceHolderMain$hfASIExpanded':'', 
                        'ctl00$PlaceHolderMain$txtHiddenDate':'', 
                        'ctl00$PlaceHolderMain$txtHiddenDate_ext_ClientState':'', 
                        'ctl00$PlaceHolderMain$dgvPermitList$lblNeedReBind':'', 
                        'ctl00$PlaceHolderMain$dgvPermitList$gdvPermitList$hfSaveSelectedItems':'', 
                        'ctl00$PlaceHolderMain$dgvPermitList$inpHideResumeConf':'', 
                        'ctl00$PlaceHolderMain$hfGridId':'', 
                        'ctl00$HDExpressionParam':'', 
                        '__EVENTTARGET': str(form_args_pagn['__EVENTTARGET']),
                        '__EVENTARGUMENT':'', 
                        '__LASTFOCUS':'', 
                        '__VIEWSTATE': self.viewstate,
                        '__VIEWSTATEGENERATOR': self.eventvalidation ,
                        'ACA_CS_FIELD': self.acs,
                        '__AjaxControlToolkitCalendarCssLoaded':'', 
                        '__ASYNCPOST': 'true'
                    }
                    # print("\n\n\n\n\n\n\n\n\n",self.acs,"\n\n\n\n\n\n\n\n\n\n")
                header1={
                        'Host': 'permits.cityofevanston.org',
                        'Origin': 'https://permits.cityofevanston.org',
                        'Referer': 'https://permits.cityofevanston.org/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=HOME',
                        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36',
                        'X-MicrosoftAjax': 'Delta=true',
                        'X-Requested-With': 'XMLHttpRequest'
                    }

                yield scrapy.FormRequest(url='https://permits.cityofevanston.org/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=HOME',formdata=formdata1,headers=header1,callback=self.parse_next,dont_filter=True,meta={'page':page_no},errback=self.parse_error)


# '''#______________________________pagination_over___________________inspection page>>>>>>>>>>>'''

    @inline_requests    
    def inspection_page(self,response):
        dic=response.meta['data']
        inspection_data=HtmlResponse(response.url, body=str.encode(response.text))
        # inspect_response(inspection_data,self)
        ins_table=inspection_data.xpath("//table[@id='ctl00_PlaceHolderMain_InspectionList_gvListCompleted']/tr[@class='InspectionListRow']")
        i=ins_table.xpath("//div[@class='ACA_LinkButton']/span/a/@onclick").extract_first()
        if i:
            for row in ins_table:
                dic['inspection_pass_fail']=row.xpath(".//td[@class='ACA_Width45em']/span[1]/text()").extract_first()
                ins_data=row.xpath(".//td[@class='ACA_Width45em']/span[2]/text()").extract_first()
                dic['inspection_subtype']=ins_data[:ins_data.find("(")-1]
                dic['inspection_id']=''.join(re.findall(r'\d+',ins_data))
                
                ins_link=row.xpath(".//div[@class='ACA_LinkButton']/span/a/@onclick").extract_first()
                ins_page_link=ins_link[ins_link.find("showInspectionPopupDialog('")+27:ins_link.find("',")]
                ins_page=yield scrapy.Request(url='https://permits.cityofevanston.org'+ins_page_link,dont_filter=True,)
                # inspect_response(ins_page,self)
                date=ins_page.xpath("//span[@id='ctl00_phPopup_Inspection_lblStatusDate']/text()").extract_first()
                dic['inspection_date']=''
                if date:
                    if ' ' in date:
                        sp=date.split(' ')
                        dic['inspection_date']=sp[0]
                    else:
                        dic['inspection_date']=sp
                inspage_data=ins_page.xpath("//div[@id='ctl00_phPopup_Inspection_ResultCommentList_InspectionResultCommentListPanel']//span/text()").extract()
                
                if inspage_data:
                    inspage_data1=' '.join(inspage_data[1:])
                    dic['inspector_comments']=inspage_data1
                dic['inspection_type']='building_inspection'
                dic['mixed_contractor_name'] =''  
                dic['contractor_dba']  =''
                dic['contractor_email']   ='' 
                dic['contractor_address_string'] =''  
                dic['contractor_phone']=''
                dic['contractor_lic_type'] =''
                dic['contractor_lic_no']=''
                yield self.save_csv(response,dic).load_item()

            ins_next_page=inspection_data.xpath("//td/span[text()='"+str(response.meta['ins'])+"']/following::a/@href").extract_first()
            if ins_next_page:
                form_args_pagn = JavaScriptUtils.getValuesFromdoPost(ins_next_page)
                event=ins_next_page[ins_next_page.find("javascript:__doPostBack('")+25:ins_next_page.find(",")-1]
                viewstate = response.text.split('__VIEWSTATE|')[1].split('|')[0]
                __VIEWSTATEGENERATOR=response.text.split('__VIEWSTATEGENERATOR|')[1].split('|')[0]
                ACA_CS_FIELD=response.text.split('ACA_CS_FIELD|')[1].split('|')[0]

                s=inspection_data.xpath("//text()").extract()[-1]
                acs=s[s.find("|ACA_CS_FIELD|")+14:-1]


                formdata={
                            'ctl00$ScriptManager1':    'ctl00$PlaceHolderMain$InspectionList$completedPanel|'+event,
                            'ctl00$HeaderNavigation$hdnShoppingCartItemNumber':'',    
                            'ctl00$HeaderNavigation$hdnShowReportLink':    'N',
                            'ctl00$PlaceHolderMain$addForDetailPage$collection':   'rdoNewCollection',
                            'ctl00$PlaceHolderMain$addForDetailPage$txtName':'',  
                            'ctl00$PlaceHolderMain$addForDetailPage$txtDesc':'',  
                            'ctl00$PlaceHolderMain$attachmentEdit$txtValidateSaveAction':'',  
                            'ctl00$PlaceHolderMain$attachmentEdit$hdfEditingFileIds':''  ,
                            'ctl00$PlaceHolderMain$attachmentEdit$fileSelect$hdFinishedFileArray':'', 
                            'ctl00$PlaceHolderMain$attachmentEdit$hdAllFinishedFileArray':'' ,
                            'ctl00$HDExpressionParam':'', 
                            '__EVENTTARGET'  : event,
                            '__EVENTARGUMENT':'', 
                            '__VIEWSTATE': viewstate,
                            '__VIEWSTATEGENERATOR' :__VIEWSTATEGENERATOR,
                            'ACA_CS_FIELD' :ACA_CS_FIELD   ,
                            '__ASYNCPOST' :'true'
    
                }
                # print(formdata)
                header={
                        "Host": "permits.cityofevanston.org",
                        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:66.0) Gecko/20100101 Firefox/66.0",
                        "Accept": "*/*",
                        "Accept-Language": "en-US,en;q=0.5",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Referer": response.url,
                        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
                        "TE": "Trailers",
                }
                yield scrapy.FormRequest(response.url,formdata=formdata,headers=header,meta={'page':response.meta['page'],'ins':1+response.meta['ins'],'data':dic,'acs':response.meta['acs']},dont_filter=True,callback=self.inspection_page)
                # inspect_response(s,self)

    def format_address(self,address):
        if address:
            add=','.join(self.lis_spce(address))
            if 'IL' in add:
                add=add.replace('IL,','IL').replace(',, IL',', IL').replace('IL,','IL').replace(',Home Phone:','').replace('IN,',',IN').replace('IA,',',IA').replace('-- 00000','IA')
                add_split=add.split(' IL ')
                add_join=', IL '.join(add_split)
                return add_join
            else:
                return add.replace('IL,','IL').replace(',, IL',', IL').replace('IL,','IL').replace(',Home Phone:','').replace('IN,',',IN').replace('IA,',',IA').replace('PA,',',PA').replace('-- 00000','IL')
        else:return "IL"

    def save_csv(self,response,data_dic):

        if data_dic['person_address_string']:
            if ', IL' == data_dic['person_address_string'].strip():
                data_dic['person_address_string']='IL'
            else:
                data_dic['person_address_string']=data_dic['person_address_string'].replace(' , IL',', IL')
        else:
            data_dic['person_address_string']=''
        il = ItemLoader(item=IlCookEvanstonBuildingPermitsSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags,lambda data:re.sub(r'\s+', ' ',data) if data else '',replace_escape_chars)        
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'IL_Cook_Evanston_Building_Permits')
        il.add_value('url', 'https://permits.cityofevanston.org/CitizenAccess/')
        for k in data_dic:
            il.add_value(k,(data_dic[k]))
        return il





    def parse_error(self,response):
        pass
        # if self.page: 
        #     page_no=str(int(page_no)+1)
        #     form_args_pagn = JavaScriptUtils.getValuesFromdoPost(self.page)
        #     # print('\n\n')
        #     # print('pageno: ',page_no)
        #     # print(form_args_pagn)
        #     # print('\n\n')

        #     formdata1={
        #             'ctl00$ScriptManager1': 'ctl00$PlaceHolderMain$dgvPermitList$updatePanel|'+str(form_args_pagn['__EVENTTARGET']),
        #             'ctl00$HeaderNavigation$hdnShoppingCartItemNumber':'', 
        #             'ctl00$HeaderNavigation$hdnShowReportLink': 'N',
        #             'ctl00$PlaceHolderMain$addForMyPermits$collection': 'rdoNewCollection',
        #             'ctl00$PlaceHolderMain$addForMyPermits$txtName': 'name',
        #             'ctl00$PlaceHolderMain$addForMyPermits$txtDesc':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSPermitNumber':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$ddlGSPermitType':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate': str(self.s),
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate_ext_ClientState':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$ctl00_PlaceHolderMain_generalSearchForm_txtGSStartDate_watermark_exd_ClientState':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate': str(self.e),
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate_ext_ClientState':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSFirstName':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSLastName':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSBusiName':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl0':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl0_watermark_exd_ClientState':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl1':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl1_watermark_exd_ClientState':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$ddlGSDirection':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSStreetName':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$ddlGSStreetSuffix':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$ddlGSUnitType':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSUnitNo':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSParcelNo':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSCity':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$ddlGSState$State1':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ZipFromAA':'0',
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_zipMask':'', 
        #             'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ext_ClientState':'', 
        #             'ctl00$PlaceHolderMain$hfASIExpanded':'', 
        #             'ctl00$PlaceHolderMain$txtHiddenDate':'', 
        #             'ctl00$PlaceHolderMain$txtHiddenDate_ext_ClientState':'', 
        #             'ctl00$PlaceHolderMain$dgvPermitList$lblNeedReBind':'', 
        #             'ctl00$PlaceHolderMain$dgvPermitList$gdvPermitList$hfSaveSelectedItems':'', 
        #             'ctl00$PlaceHolderMain$dgvPermitList$inpHideResumeConf':'', 
        #             'ctl00$PlaceHolderMain$hfGridId':'', 
        #             'ctl00$HDExpressionParam':'', 
        #             '__EVENTTARGET': str(form_args_pagn['__EVENTTARGET']),
        #             '__EVENTARGUMENT':'', 
        #             '__LASTFOCUS':'', 
        #             '__VIEWSTATE': self.viewstate,
        #             '__VIEWSTATEGENERATOR': self.eventvalidation ,
        #             'ACA_CS_FIELD': self.acs,
        #             '__AjaxControlToolkitCalendarCssLoaded':'', 
        #             '__ASYNCPOST': 'true'
        #         }
        #         # print("\n\n\n\n\n\n\n\n\n",self.acs,"\n\n\n\n\n\n\n\n\n\n")
        #     header1={
        #             'Host': 'permits.cityofevanston.org',
        #             'Origin': 'https://permits.cityofevanston.org',
        #             'Referer': 'https://permits.cityofevanston.org/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=HOME',
        #             'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.108 Safari/537.36',
        #             'X-MicrosoftAjax': 'Delta=true',
        #             'X-Requested-With': 'XMLHttpRequest'
        #         }

        #     yield scrapy.FormRequest(url='https://permits.cityofevanston.org/CitizenAccess/Cap/CapHome.aspx?module=Building&TabName=HOME',formdata=formdata1,headers=header1,callback=self.parse_next,dont_filter=True,meta={'page':page_no},errback=self.parse_error)




















        # # self.state['items_count'] = self.state.get('items_count', 0) + 1
        # il = ItemLoader(item=IlCookEvanstonBuildingPermitsSpiderItem(),response=response)
        # # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        # il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        # il.add_value('sourceName', 'il_cook_evanston_building_permits')
        # il.add_value('url', 'https://permits.cityofevanston.org/CitizenAccess/')
        # il.add_xpath('inspection_subtype', '')
        # il.add_xpath('parcel number', '')
        # il.add_xpath('permit_lic_desc', '')
        # il.add_xpath('mixed_subtype', '')
        # il.add_xpath('person_address_string', '')
        # il.add_xpath('inspection_pass_fail', '')
        # il.add_xpath('contractor_lic_no', '')
        # il.add_xpath('person_phone', '')
        # il.add_xpath('dba_name', '')
        # il.add_xpath('inspection_type', '')
        # il.add_xpath('permit_type', '')
        # il.add_xpath('occupancy_subtype', '')
        # il.add_xpath('inspection_date', '')
        # il.add_xpath('permit_lic_no', '')
        # il.add_xpath('permit_lic_status', '')
        # il.add_xpath('inspection_id', '')
        # il.add_xpath('mixed_contractor_name', '')
        # il.add_xpath('permit_subtype', '')
        # il.add_xpath('contractor_lic_type', '')
        # il.add_xpath('contractor_address_string', '')
        # il.add_xpath('contractor_phone', '')
        # il.add_xpath('contractor_dba', '')
        # il.add_xpath('inspector_comments', '')
        # il.add_xpath('permit_lic_fee', '')
        # il.add_xpath('location_address_string', '')
        # il.add_xpath('mixed_name', '')
        # return il.load_item()