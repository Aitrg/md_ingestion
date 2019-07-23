# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-17 06:34:55
TICKET NUMBER -AI_1461
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import scrapy 
import json
from Data_scuff.spiders.AI_1461.items import AlMedicalPersonLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.shell import inspect_response
from inline_requests import inline_requests
from Data_scuff.utils.searchCriteria import SearchCriteria

class AlMedicalPersonLicensesSpider(CommonSpider):
    name = '1461_al_medical_person_licenses'
    allowed_domains = ['igovsolution.com']
    start_urls = ['https://abme.igovsolution.com/online/Lookups/Individual_Lookup.aspx']
    site_key='6LchcFEUAAAAAJdfnpZDr9hVzyt81NYOspe29k-x'
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AlMedicalPersonLicensesSpider'),
        'JIRA_ID':'AI_1461',
        'HTTPCACHE_ENABLED': False,
        'CONCURRENT_REQUESTS':1,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        # 'JOBDIR' : CustomSettings.getJobDirectory('al_medical_person_licenses'),
        'TOP_HEADER':{                        
                         'coq status': 'COQ status',
                         'location_address_string': 'Location',
                         'pa/crnp/cnm name': 'PA/CRNP/CNM Name',
                         'permit_lic_desc': 'License description',
                         'permit_lic_eff_date': 'Issue date',
                         'permit_lic_exp_date': 'Expiration date',
                         'permit_lic_no': 'License number',
                         'permit_lic_status': 'License status',
                         'permit_subtype': 'License type',
                         'permit_type': '',
                         'person_name': "Licensee name/physician's name",
                         "physician's license": "Physician's License",
                         'practice type': 'Practice Type',
                         'ra/cp number': 'RA/CP Number',
                         'school name': 'School Name'},
        'FIELDS_TO_EXPORT':[                        
                         'person_name',
                         "physician's license",
                         'pa/crnp/cnm name',
                         'ra/cp number',
                         'location_address_string',
                         'permit_subtype',
                         'permit_lic_status',
                         'coq status',
                         'permit_lic_no',
                         'permit_lic_desc',
                         'permit_lic_eff_date',
                         'permit_lic_exp_date',
                         'practice type',
                         'school name',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp'],


        'NULL_HEADERS':['ra/cp number', 'coq status', 'practice type', 'pa/crnp/cnm name', 'school name', "physician's license"]
        }
    search_element=[]

    # def __init__(self, start=None, end=None,startnum=None,endnum=None, proxyserver=None, *a, **kw):
        # super(AzPharmacyIndividualSpider, self).__init__(start,end, proxyserver=None,*a, **kw) 
        # self.searchCriteria = SearchCriteria.strRange(start,end)

    def parse(self, response):

        yield scrapy.Request(response.urljoin('/online/JS_grd/Grid.svc/Verifycaptcha'),
                   method='POST',body=json.dumps({"resp":self.getcaptchaCoder(self.site_key).resolver(response.url),
                                    "uip":""}),
                   headers={'Content-Type':'application/json'}, callback=self.verify_captcha)

    def dict(self, lic_typee):
        dict1={
        'MD':'Medical Doctor','DO':'Doctor of Osteopathy','L':'Limited MD or DO','PA':'Physician Assistant','AA':'Anesthesiologist Assistant','TA':'temporary Physician Assistant','SP':'Special Purpose(Tele Medicine)','RA':'','CP':'','ACSC':'Alabama Controlled Substance Certificate','QACSC':'Qualified Alabama Controlled Substance Certificate','QACSCNP':'','LPSP':'','RSV':''}
        return dict1.get(lic_typee, "")
    def verify_captcha(self, response):
        if len(self.search_element) == 0:
            self.search_element = SearchCriteria.strRange(self.start,self.end)
        if len(self.search_element) > 0:
            param=self.search_element.pop(0)
            print('--------------------param---------',param)
            jsonresponse = json.loads(response.body_as_unicode())
            formdata = {
                'county':'-1',
                'fname': '',
                'lictype': '-1',
                'lname': str(param),
                'lnumber':'',
                'page': '1',
                'pageSize': '20',
                'sdata': [],
                'sortby': '',
                'sortexp': '',
                'vid': jsonresponse['d'],
            }

            yield scrapy.Request(response.urljoin('/online/JS_grd/Grid.svc/GetIndv_license'),
                           method='POST',
                       body=json.dumps(formdata),
                   headers={'Content-Type':'application/json'},
                   callback=self.getIndv_license,meta={'page':'2','vid':jsonresponse['d'],'param':param})
    @inline_requests
    def getIndv_license(self, response):
        meta=response.meta
        aa =json.loads(json.loads(response.body_as_unicode())['d'])
        val = json.loads(json.loads(response.body_as_unicode())['d'])['Response']
        temp_name=''
        if val:
            for item_ in json.loads(val):
                lic_id = item_['App_ID']
                lic_type = item_['License_Type']
                temp_name=item_['Name']
                if lic_type == 'MD' or lic_type == 'DO' or lic_type == 'L' or lic_type == 'SP':
                    detail_page=yield scrapy.Request(url='https://abme.igovsolution.com/online/ABME_Prints/Print_MD_DO_Laspx.aspx?appid='+str(lic_id),method='GET')
                elif lic_type =='PA' or lic_type=='TA':
                    detail_page=yield scrapy.Request(url='https://abme.igovsolution.com/online/ABME_Prints/Print_PA_TA.aspx?appid='+str(lic_id),method='GET')
                elif lic_type =='AA':
                    detail_page=yield scrapy.Request(url='https://abme.igovsolution.com/online/ABME_Prints/Print_AA.aspx?appid='+str(lic_id),method='GET')
                elif lic_type =='RA':
                    detail_page=yield scrapy.Request(url='https://abme.igovsolution.com/online/ABME_Prints/Print_RA.aspx?appid='+str(lic_id),method='GET')
                elif lic_type =='CP':
                    detail_page=yield scrapy.Request(url='https://abme.igovsolution.com/online/ABME_Prints/Print_CP.aspx?appid='+str(lic_id),method='GET')
                elif lic_type =='ACSC':
                    detail_page=yield scrapy.Request(url='https://abme.igovsolution.com/online/ABME_Prints/Print_other.aspx?appid='+str(lic_id),method='GET')
                elif lic_type =='QASC' or lic_type=='QACSCNP':
                    detail_page=yield scrapy.Request(url='https://abme.igovsolution.com/online/ABME_Prints/print_Quality_CSC.aspx?appid='+str(lic_id),method='GET')
                elif lic_type =='LPSP':
                    detail_page=yield scrapy.Request(url='https://abme.igovsolution.com/online/ABME_Prints/Print_LPSPaspx.aspx?appid='+str(lic_id),method='GET')
                person_name1= detail_page.xpath("//span[contains(text(),'Licensee name')]/ancestor::div/following-sibling::div/span/text()").extract_first()
                print("permit_subtype----------------------------------------->",lic_type)
                permit_subtype=self.dict(lic_type)
                print("permit_subtype-------------------------------------->",permit_subtype)
                person_name=person_name1 if person_name1 else temp_name
                textt = "Physician's License:" or "physician's license:"
                physician_lic= detail_page.xpath('//span[contains(text(),"'+textt+'")]/ancestor::div/following-sibling::div/span/text()').extract_first()
                pa_cp_ra_name = detail_page.xpath("//span[contains(text(),'PA/CRNP/CNM Name:')]/ancestor::div/following-sibling::div/span/text()").extract_first()
                ra_cp_number = detail_page.xpath("//span[contains(text(),'RA/CP Number:')]/ancestor::div/following-sibling::div/span/text()").extract_first()
                location_address_string = str(detail_page.xpath("//span[contains(text(),'Location:')]/ancestor::div/following-sibling::div/span/text()").extract_first()).strip().replace('None','Alabama')
                permit_lic_status=detail_page.xpath("//span[contains(text(),'License status:')]/ancestor::div/following-sibling::div/span/text()").extract_first()
                coq_status= detail_page.xpath("//span[contains(text(),'COQ status:')]/ancestor::div/following-sibling::div/span/text()").extract_first()
                permit_lic_number = detail_page.xpath("//span[contains(text(),'License number:')]/ancestor::div/following-sibling::div/span/text()").extract_first()
                permit_lic_desc = detail_page.xpath("//span[contains(text(),'License description:')]/ancestor::div/following-sibling::div/span/text()").extract_first()
                permit_lic_eff_date = detail_page.xpath("//span[contains(text(),'Issue date:')]/ancestor::div/following-sibling::div/span/text()").extract_first()
                permit_lic_exp_date = detail_page.xpath("//span[contains(text(),'Expiration date:')]/ancestor::div/following-sibling::div/span/text()").extract_first()
                practice_type = detail_page.xpath("//span[contains(text(),'Practice Type')]/ancestor::div/following-sibling::div/span/text()").extract_first()
                school_name = detail_page.xpath("//span[contains(text(),'School Name:')]/ancestor::div/following-sibling::div/span/text()").extract_first()

                print("=========================================================",permit_lic_exp_date,lic_type,person_name)
                # self.state['items_count'] = self.state.get('items_count', 0) + 1
                il = ItemLoader(item=AlMedicalPersonLicensesSpiderItem(),response=response)
                il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
                il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                il.add_value('url', 'https://abme.igovsolution.com/online/Lookups/Individual_Lookup.aspx')
                il.add_value('sourceName', 'AL_Medical_Person_Licenses')
                il.add_value('person_name',person_name)
                il.add_value("physician's license",physician_lic)
                il.add_value('pa/crnp/cnm name',pa_cp_ra_name)
                il.add_value('ra/cp number',ra_cp_number)
                il.add_value('location_address_string',location_address_string)
                il.add_value('permit_lic_status',permit_lic_status)
                il.add_value('coq status', coq_status)
                il.add_value('permit_lic_no',permit_lic_number)
                il.add_value('permit_lic_desc',permit_lic_desc)
                il.add_value('permit_lic_eff_date',permit_lic_eff_date)
                il.add_value('permit_lic_exp_date',permit_lic_exp_date)
                il.add_value('practice type', practice_type)
                il.add_value('school name',school_name)
                il.add_value('permit_subtype',permit_subtype)
                il.add_value('permit_type', 'medical_license')
                yield il.load_item()

        total = aa['reccount']   #page_count
        page=response.meta['page']
        vid=response.meta['vid']
        param=response.meta['param']
        if total:
            page_no=(int(total)/20)+2

            if int(page_no) > int(page):
                headers={
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Content-Type': 'application/json; charset=UTF-8',
                # 'Host': 'abme.igovsolution.com',
                'Origin': 'https://abme.igovsolution.com',
                'Referer': 'https://abme.igovsolution.com/online/Lookups/Individual_Lookup.aspx',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest',
                # 'Cookie': 'ASP.NET_SessionId=bfmszuy441lnxiinwh0ivdzo; WWTHREADSID=ThisIsThEValue'
                }
                body=json.loads(json.dumps('{"lnumber":"","lname":"'+str(param)+'","fname":"","lictype":"-1","county":"-1","vid":"'+str(vid)+'","pageSize":20,"page":'+str(page)+',"sortby":"","sortexp":"","sdata":[]}'))
                print(body,'------------------body')


                yield scrapy.FormRequest(url='https://abme.igovsolution.com/online/JS_grd/Grid.svc/GetIndv_license',method='POST',headers=headers,body=body,callback=self.getIndv_license,meta={'page':int(page)+1,'vid':vid,'param':param})

            elif len(self.search_element) > 0:
                yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)