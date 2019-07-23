# -*- coding: utf-8 -*-
'''
Created on 2019-Jul-16 05:11:59
TICKET NUMBER -AI_1483
@author: Prazi
'''
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_1483.items import AlMassageTherapyLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
import json
from inline_requests import inline_requests
import re
from scrapy.http import FormRequest, Request
class AlMassageTherapyLicensesSpider(CommonSpider):
    name = '1483_al_massage_therapy_licenses'
    allowed_domains = ['alabama.gov']
    start_urls = ['http://www.almtbd.alabama.gov/licensee.aspx']
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1483_Licenses_Massage_Therapy_AL_CurationReady'),
        'JIRA_ID':'AI_1483',
        'DOWNLOAD_DELAY':0.2,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('AlMassageTherapyLicensesSpider'),
        'TOP_HEADER':{'approved by': 'Approved by','approved date': 'Approved Date','category': 'Category','company_email': 'Email','company_fax': 'Fax','company_name': 'Name','company_phone': 'Phone Number','company_website': 'Website Address','dba_name': '','location_address_string': 'Address','mail_address_string': 'Mailing Address','permit_lic_desc': '','permit_lic_eff_date': 'Effective Date','permit_lic_exp_date': 'Expiration Date','permit_lic_no': 'License Number','permit_lic_status': 'Status','permit_type': '','person_name': '','person_subtype': '','renewal date': 'Renewal Date'},
        'FIELDS_TO_EXPORT':['category','company_name','dba_name','approved by','permit_lic_no','renewal date','permit_lic_status','location_address_string','mail_address_string','person_name','person_subtype','company_phone','company_fax','permit_lic_eff_date','permit_lic_exp_date','approved date','company_email','company_website','permit_lic_desc','permit_type','url','sourceName','ingestion_timestamp'],
        'NULL_HEADERS':['category', 'approved by', 'renewal date', 'approved date']
        }
    check_first=True
    param=[]
    def parse(self, response):
        if self.check_first:
            self.check_first=False
            drop_down=response.xpath('//*[@id="ContentPlaceHolder1_ddlCategory"]/option/@value').extract()[1:]
            self.param=drop_down
            self.param = drop_down[int(self.start):int(self.end)]
        if len(self.param)>0:
            index=self.param.pop(0)
            form_data={'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$ddlCategory','__EVENTTARGET': 'ctl00$ContentPlaceHolder1$ddlCategory','__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),'__VIEWSTATEGENERATOR': response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),'__EVENTVALIDATION': response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),'ctl00$ContentPlaceHolder1$ddlCategory': str(index),'__ASYNCPOST': 'true'
            }
            yield FormRequest(url='http://www.almtbd.alabama.gov/licensee.aspx',callback=self.parse_next,method='POST',dont_filter=True,formdata=form_data,meta={'index':index})
    def parse_next(self,response):
        j=response.meta['index']
        responseValues = response.text.split('|')
        viewstate = ""
        viewgenerator = ""
        event=""
        for i in range(len(responseValues)):
            if responseValues[i] == "__VIEWSTATE":
                viewstate = responseValues[i+1]
            if responseValues[i] == "__VIEWSTATEGENERATOR":
                viewgenerator = responseValues[i+1]
            if responseValues[i] == "__EVENTVALIDATION":
                event = responseValues[i+1]
        form_data={
            'ctl00$ContentPlaceHolder1$ScriptManager1': 'ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$btnSubmit','ctl00$ContentPlaceHolder1$ddlCategory': str(j),'__VIEWSTATE': viewstate,'__VIEWSTATEGENERATOR': viewgenerator,'__EVENTVALIDATION': event,'__ASYNCPOST': 'true','ctl00$ContentPlaceHolder1$btnSubmit': 'Submit'
            }
        current_page = 1
        yield scrapy.FormRequest(url='http://www.almtbd.alabama.gov/licensee.aspx',callback=self.get_data,method='POST',dont_filter=True,formdata=form_data,meta={'j':j,'current_page':current_page})
    def get_data(self,response):
        meta={} 
        responseValues = response.text.split('|')
        viewstate = ""
        viewgenerator = ""
        event=""
        for i in range(len(responseValues)):
            if responseValues[i] == "__VIEWSTATE":
                viewstate = responseValues[i+1]
            if responseValues[i] == "__VIEWSTATEGENERATOR":
                viewgenerator = responseValues[i+1]
            if responseValues[i] == "__EVENTVALIDATION":
                event = responseValues[i+1]
        j=response.meta['j']
        current_page = response.meta['current_page']
        if str(j)=='1':
            meta['category']=meta['company_name']=meta['permit_lic_no']=meta['Renewal_Date']=meta['permit_lic_status']=meta['location_address_string']=meta['person_name']=meta['person_subtype']=meta['company_phone']=meta['permit_lic_eff_date']=meta['permit_lic_exp_date']=meta['permit_lic_desc']=meta['approved_by']=meta['approved_date']=meta['fax']=meta['email']=meta['mailing_address']=meta['website_address']=''
            meta['category']='Licensee'
            value=response.xpath('//*[@id="ContentPlaceHolder1_GridView1"]//tr/td/div').extract()
            manager1='ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$GridView1'
            eventarget='ctl00$ContentPlaceHolder1$GridView1'
            for k in range(0,len(value)):
                meta['company_name']=response.xpath('//*[@id="ContentPlaceHolder1_GridView1_Label1_'+str(k)+'"]/text()').extract_first()
                meta['permit_lic_no']=response.xpath('//*[@id="ContentPlaceHolder1_GridView1_lblLicenseNumberGrid_'+str(k)+'"]/text()').extract_first()
                meta['Renewal_Date']=response.xpath('//*[@id="ContentPlaceHolder1_GridView1_lblRenewalDate_'+str(k)+'"]/text()').extract_first()
                meta['permit_lic_status']=response.xpath('//*[@id="ContentPlaceHolder1_GridView1_lblStatus_'+str(k)+'"]/text()').extract_first()
                meta['location_address_string']=response.xpath('//*[@id="ContentPlaceHolder1_GridView1_lblAddress_'+str(k)+'"]/text()').extract_first()
                meta['location_address_string']=self.address1(meta['location_address_string'])
                meta['permit_lic_desc']='Therapy License for '+self._getDBA(meta['company_name'])[0]
                yield self.save_to_csv(response,**meta)
        if str(j)=='2':
            meta['category']=meta['company_name']=meta['permit_lic_no']=meta['Renewal_Date']=meta['permit_lic_status']=meta['location_address_string']=meta['person_name']=meta['person_subtype']=meta['company_phone']=meta['permit_lic_eff_date']=meta['permit_lic_exp_date']=meta['permit_lic_desc']=meta['approved_by']=meta['approved_date']=meta['fax']=meta['email']=meta['mailing_address']=meta['website_address']=''
            meta['category']='Establishments'
            value=response.xpath('//*[@id="ContentPlaceHolder1_gvEstablishments"]//tr/td/div').extract()
            manager1='ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$gvEstablishments'
            eventarget='ctl00$ContentPlaceHolder1$gvEstablishments'
            for k in range(0,len(value)):
                meta['company_name']=response.xpath('//*[@id="ContentPlaceHolder1_gvEstablishments_Label1_'+str(k)+'"]/text()').extract_first()
                meta['person_name']=response.xpath('//*[@id="ContentPlaceHolder1_gvEstablishments_lblOwner_'+str(k)+'"]/text()').extract_first()
                if meta['person_name']:
                    meta['person_subtype']='Owner'
                meta['company_phone']=response.xpath('//*[@id="ContentPlaceHolder1_gvEstablishments_lblPhoneNumber_'+str(k)+'"]/text()').extract_first()
                meta['permit_lic_no']=response.xpath('//*[@id="ContentPlaceHolder1_gvEstablishments_lblLicenseNumberGrid_'+str(k)+'"]/text()').extract_first()
                meta['permit_lic_eff_date']=response.xpath('//*[@id="ContentPlaceHolder1_gvEstablishments_lblEffectiveDate_'+str(k)+'"]/text()').extract_first()
                meta['permit_lic_exp_date']=response.xpath('//*[@id="ContentPlaceHolder1_gvEstablishments_Label2_'+str(k)+'"]/text()').extract_first()
                meta['permit_lic_status']=response.xpath('//*[@id="ContentPlaceHolder1_gvEstablishments_lblStatus_'+str(k)+'"]/text()').extract_first()
                meta['location_address_string']=response.xpath('//*[@id="ContentPlaceHolder1_gvEstablishments_lblAddress_'+str(k)+'"]/text()').extract_first()
                meta['location_address_string']=self.address1(meta['location_address_string'])
                meta['permit_lic_desc']='Therapy License for '+self._getDBA(meta['company_name'])[0]
                yield self.save_to_csv(response,**meta)
        if str(j)=='3':
            meta['category']=meta['company_name']=meta['permit_lic_no']=meta['Renewal_Date']=meta['permit_lic_status']=meta['location_address_string']=meta['person_name']=meta['person_subtype']=meta['company_phone']=meta['permit_lic_eff_date']=meta['permit_lic_exp_date']=meta['permit_lic_desc']=meta['approved_by']=meta['approved_date']=meta['fax']=meta['email']=meta['mailing_address']=meta['website_address']=''
            meta['category']='CE Providers'
            value=response.xpath('//*[@id="ContentPlaceHolder1_gvCEProviders"]//tr/td/div').extract()
            manager1='ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$gvCEProviders'
            eventarget='ctl00$ContentPlaceHolder1$gvCEProviders'
            z=2
            for k in range(0,len(value)):
                meta['company_name']=response.xpath('//*[@id="ContentPlaceHolder1_gvCEProviders_Label1_'+str(k)+'"]/text()').extract_first()
                meta['approved_by']=response.xpath('//*[@id="ContentPlaceHolder1_gvCEProviders_lblOwner_'+str(k)+'"]/text()').extract_first()
                meta['person_name']=response.xpath('//*[@id="ContentPlaceHolder1_gvCEProviders_lblPhoneNumber_'+str(k)+'"]/text()').extract_first()
                meta['approved_date']=response.xpath('//*[@id="ContentPlaceHolder1_gvCEProviders_lblEffectiveDate_'+str(k)+'"]/text()').extract_first()
                meta['company_phone']=response.xpath('//*[@id="ContentPlaceHolder1_gvCEProviders_lblPhone_'+str(k)+'"]/text()').extract_first()
                meta['fax']=response.xpath('//*[@id="ContentPlaceHolder1_gvCEProviders_lblFax_'+str(k)+'"]/text()').extract_first()
                if meta['fax']:
                    meta['fax']=meta['fax'].replace('n/a','')
                meta['email']=response.xpath('//*[@id="ContentPlaceHolder1_gvCEProviders"]//tr['+str(z)+']/td/div/a/text()').extract_first()
                if meta['email']:
                    meta['email']=meta['email'].replace('n/a','')
                meta['location_address_string']=response.xpath('//*[@id="ContentPlaceHolder1_gvCEProviders_lblAddress_'+str(k)+'"]/text()').extract_first()
                meta['location_address_string']=self.address1(meta['location_address_string'])
                meta['permit_lic_desc']='Therapy License for '+self._getDBA(meta['company_name'])[0]
                yield self.save_to_csv(response,**meta)
                z+=1
        if str(j)=='4':
            meta['category']=meta['company_name']=meta['permit_lic_no']=meta['Renewal_Date']=meta['permit_lic_status']=meta['location_address_string']=meta['person_name']=meta['person_subtype']=meta['company_phone']=meta['permit_lic_eff_date']=meta['permit_lic_exp_date']=meta['permit_lic_desc']=meta['approved_by']=meta['approved_date']=meta['fax']=meta['email']=meta['mailing_address']=meta['website_address']=''
            meta['category']='Schools'
            value=response.xpath('//*[@id="ContentPlaceHolder1_gvSchools"]//tr/td/div').extract()
            manager1='ctl00$ContentPlaceHolder1$UpdatePanel1|ctl00$ContentPlaceHolder1$gvSchools'
            eventarget='ctl00$ContentPlaceHolder1$gvSchools'
            z=2
            for k in range(0,len(value)):
                meta['company_name']=response.xpath('//*[@id="ContentPlaceHolder1_gvSchools_Label1_'+str(k)+'"]/text()').extract_first()
                meta['mailing_address']=response.xpath('//*[@id="ContentPlaceHolder1_gvSchools_lblMailingAddress_'+str(k)+'"]/text()').extract_first()
                if meta['mailing_address']:
                    meta['mailing_address']=meta['mailing_address'].replace(' AL ',', AL ')
                meta['company_phone']=response.xpath('//*[@id="ContentPlaceHolder1_gvSchools_lblPhone_'+str(k)+'"]/text()').extract_first()
                meta['website_address']=response.xpath('//*[@id="ContentPlaceHolder1_gvSchools"]//tr['+str(z)+']/td/div/a/text()').extract_first()
                meta['permit_lic_exp_date']=response.xpath('//*[@id="ContentPlaceHolder1_gvSchools_Label6_'+str(k)+'"]/text()').extract_first()
                meta['permit_lic_desc']='Therapy License for '+self._getDBA(meta['company_name'])[0]
                meta['location_address_string']=meta['mailing_address']
                yield self.save_to_csv(response,**meta)
                z+=1
        current_page=current_page+1
        form_data_page={'ctl00$ContentPlaceHolder1$ScriptManager1': str(manager1),'ctl00$ContentPlaceHolder1$ddlCategory': str(j),'__EVENTTARGET': str(eventarget),'__EVENTARGUMENT': 'Page$'+str(current_page),'__VIEWSTATE': viewstate,'__VIEWSTATEGENERATOR': viewgenerator,'__EVENTVALIDATION':event,'__ASYNCPOST': 'true'
        }
        if value:
            yield scrapy.FormRequest(url='http://www.almtbd.alabama.gov/licensee.aspx',callback=self.get_data,formdata=form_data_page,dont_filter=True,meta={'current_page': current_page,'j':j})
        elif len(self.param) > 0:
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)
    def save_to_csv(self,response,**meta):
        il = ItemLoader(item=AlMassageTherapyLicensesSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'AL_Massage_Therapy_Licenses')
        il.add_value('url', 'http://www.almtbd.alabama.gov/licensee.aspx')
        il.add_value('category',meta['category'])
        il.add_value('company_name',self._getDBA(meta['company_name'])[0])
        il.add_value('dba_name',self._getDBA(meta['company_name'])[1])
        il.add_value('approved by',meta['approved_by'])
        il.add_value('permit_lic_no',meta['permit_lic_no'])
        il.add_value('renewal date',meta['Renewal_Date'])
        il.add_value('permit_lic_status',meta['permit_lic_status'])
        il.add_value('location_address_string',meta['location_address_string'])
        il.add_value('mail_address_string',meta['mailing_address'])
        il.add_value('person_name',meta['person_name'])
        il.add_value('person_subtype',meta['person_subtype'])
        il.add_value('company_phone',meta['company_phone'])
        il.add_value('company_fax',meta['fax'])
        il.add_value('permit_lic_eff_date',meta['permit_lic_eff_date'])
        il.add_value('permit_lic_exp_date',meta['permit_lic_exp_date'])
        il.add_value('approved date',meta['approved_date'])
        il.add_value('company_email',meta['email'])
        il.add_value('company_website',meta['website_address'])
        il.add_value('permit_lic_desc',meta['permit_lic_desc'])
        il.add_value('permit_type', 'therapy_license')
        return il.load_item()
    def address1(self, address):
        if address:
            address = address.replace(' WA ',', WA ').replace(' OR ',', OR ').replace(' CA ',', CA ').replace(' NV ',', NV ').replace(' ID ',', ID ').replace(' MT ',', MT ').replace(' UT ',', UT ').replace(' AZ ',', AZ ').replace(' NM ',', NM ').replace(' CO ',', CO ').replace(' WY ',', WY ').replace(' ND ',', ND ').replace(' SD ',', SD ').replace(' NE ',', NE ').replace(' KS ',', KS ').replace(' OK ',', OK ').replace(' TX ',', TX ').replace(' MN ',', MN ').replace(' IA ',', IA ').replace(' MO ',', MO ').replace(' AR ',', AR ').replace(' LA ',', LA ').replace(' WI ',', WI ').replace(' IL ',', IL ').replace(' MS ',', MS ').replace(' AL ',', AL ').replace(' TN ',', TN ').replace(' KY ',', KY ').replace(' OH ',', OH ').replace(' MI ',', MI ').replace(' IN ',', IN ').replace(' GA ',', GA ').replace(' FL ',', FL ').replace(' SC ',', SC ').replace(' NC ',', NC ').replace(' VA ',', VA ').replace(' DC ',', DC ').replace(' PA ',', PA ').replace(' NY ',', NY ').replace(' VT ',', VT ').replace(' ME ',', ME ').replace(' NH ',', NH ').replace(' MA ',', MA ').replace(' RI ',', RI ').replace(' CT ',', CT ').replace(' NJ ',', NJ ').replace(' DE ',', DE ').replace(' MD ',', MD ').replace(' WV ',', WV ').replace(' HI ',', HI ').replace(' AK ',', AK ')
        return address