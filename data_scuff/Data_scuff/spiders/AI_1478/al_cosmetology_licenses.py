'''Created on 2019-Jul-17 15:43:24
TICKET NUMBER -AI_1478
@author: Suganya'''
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_1478.items import AlCosmetologyLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
import re
from scrapy.shell import inspect_response
from inline_requests import inline_requests
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils
from Data_scuff.utils.searchCriteria import SearchCriteria
class AlCosmetologyLicensesSpider(CommonSpider):
    name = '1478_al_cosmetology_licenses'
    allowed_domains = ['glsuite.us']
    start_urls = ['https://alboc.glsuite.us/GLSuiteWeb/Clients/ALBOC/public/VerificationSearch.aspx']    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1478_Licenses_Cosmetology_AL_CurationReady'),
        'JIRA_ID':'AI_1478',
        'HTTPCACHE_ENABLED':False,
        'CONCURRENT_REQUESTS': 1,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'DOWNLOAD_DELAY':4,
        # 'JOBDIR' : CustomSettings.getJobDirectory('al_cosmetology_licenses'),
        'TOP_HEADER':{'company_name': 'Salon Name','disciplinary action': 'Disciplinary Action','location_address_string': 'Address','permit_lic_desc': '','permit_lic_exp_date': 'License Expiration Date','permit_lic_no': 'License Number','permit_lic_status': 'License Status','permit_subtype': 'License Type','permit_type': '','person_name': 'Name','violation_type': ''},
        'FIELDS_TO_EXPORT':['person_name','company_name','location_address_string','permit_lic_no','permit_subtype','permit_lic_exp_date','permit_lic_status','disciplinary action','violation_type','permit_lic_desc','permit_type','url','sourceName',  'ingestion_timestamp'],
        'NULL_HEADERS':['disciplinary action']}
    def __init__(self, start=None, end=None,startnum=None,endnum=None, proxyserver=None, *a, **kw):
        super(AlCosmetologyLicensesSpider, self).__init__(start,end, proxyserver=None,*a, **kw)
        self.search_element = SearchCriteria.strRange(self.start,self.end)
    search_element = [] 
    search_element_a=[]
    check_first = True
    def parse(self, response):
        if self.check_first:
            self.check_first = False
            self.search_element_a = SearchCriteria.strRange(self.starta,self.enda)
            self.search_element1='*'+str(self.search_element.pop(0))
        if len(self.search_element_a) > 0:
            val = '*'+str(self.search_element_a.pop(0))
            form_data={'ctl00$ContentPlaceHolder1$txtLastName': str(self.search_element1),
            'ctl00$ContentPlaceHolder1$txtShopName': str(val),
            '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$btnLicenseeSubmit'}
            head={'Connection': 'keep-alive' ,
            'Host': 'alboc.glsuite.us',
            'Origin': 'https://alboc.glsuite.us','Upgrade-Insecure-Requests': '1' ,
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36' ,
            'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application         signed-exchange;v=b3',     
            'Referer': 'https://alboc.glsuite.us/GLSuiteWeb/Clients/ALBOC/public/VerificationSearch.aspx',
            'Accept-Encoding': 'gzip, deflate, br','Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8' }
            yield scrapy.FormRequest.from_response(response,headers=head,formdata=form_data,formid='aspnetForm',method="POST",dont_filter=True,callback=self.parse_details)
    @inline_requests   
    def parse_details(self, response):
        tr_list=response.xpath('//*[@id="ctl00_ContentPlaceHolder1_dtgResults"]//tr')[1:]
        for tr in tr_list:
            link=tr.xpath('td[10]/a/@href').extract_first()
            company_name=tr.xpath('td[4]/text()').extract_first()
            f_name=tr.xpath('td[1]/text()').extract_first()
            m_name=tr.xpath('td[2]/text()').extract_first()
            l_name=tr.xpath('td[3]/text()').extract_first()
            person_name=self.format_name(f_name,m_name,l_name)
            if company_name and len(company_name) > 2:
                company_name=company_name
            else:
                company_name=person_name
            if link:
                link_url='https://alboc.glsuite.us/GLSuiteWeb/Clients/ALBOC/public/'+str(link)
                parse_res=yield scrapy.Request(url=link_url,dont_filter=True)
                add=parse_res.xpath('//*[contains(text(),"City")]/following-sibling::td/span/text()').extract_first()
                state=parse_res.xpath('//*[contains(text(),"State")]/following-sibling::td/span/text()').extract_first()
                if add and state:
                    location_address_string=add+', '+state
                else:
                    location_address_string=state
                permit_lic_no=parse_res.xpath('//*[contains(text(),"License Number")]/following-sibling::td/span/text()').extract_first()
                permit_subtype=parse_res.xpath('//*[contains(text(),"License Type")]/following-sibling::td/span/text()').extract_first()
                permit_lic_exp_date=parse_res.xpath('//*[contains(text(),"License Expiration Date")]/following-sibling::td/span/text()').extract_first()
                permit_lic_status=parse_res.xpath('//*[contains(text(),"License Status")]/following-sibling::td/span/text()').extract_first()
                disciplinary_action=parse_res.xpath('//*[contains(text(),"Disciplinary Action")]/following-sibling::td/span/text()').extract_first()
                il = ItemLoader(item=AlCosmetologyLicensesSpiderItem(),response=response)
                # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
                il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                il.add_value('url', 'https://alboc.glsuite.us/GLSuiteWeb/Clients/ALBOC/public/VerificationSearch.aspx')
                il.add_value('sourceName', 'AL_Cosmetology_Licenses')
                il.add_value('permit_lic_exp_date',permit_lic_exp_date)
                il.add_value('permit_lic_status',permit_lic_status)
                il.add_value('person_name', person_name)
                il.add_value('violation_type', '')
                il.add_value('disciplinary action', disciplinary_action)
                il.add_value('permit_lic_desc', ('Cosmetology License for'+' '+str(company_name)) if company_name and len(company_name)>2 else 'Cosmetology License')
                il.add_value('permit_type', 'cosmetology_license')
                il.add_value('location_address_string', location_address_string if location_address_string and len(location_address_string) > 2 else 'AL')
                il.add_value('permit_lic_no', permit_lic_no)
                il.add_value('company_name', company_name)
                il.add_value('permit_subtype', permit_subtype)
                yield il.load_item()           
        pageee=response.xpath('//td[@colspan="10"]/span/following-sibling::a/@href').extract_first()
        if pageee:
            page_link=JavaScriptUtils.getValuesFromdoPost(pageee)
            page_data={'__EVENTTARGET':page_link['__EVENTTARGET'],'__EVENTARGUMENT':page_link['__EVENTARGUMENT'],'__VIEWSTATE':response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(), '__VIEWSTATEGENERATOR':response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),'__EVENTVALIDATION':response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),'__VIEWSTATEENCRYPTED':response.xpath('//*[@id="__VIEWSTATEENCRYPTED"]/@value').extract_first()}
            yield scrapy.FormRequest(url=response.url,method='POST',formdata=page_data,callback=self.parse_details,dont_filter=True)
        elif len(self.search_element_a)>0:
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)
        elif len(self.search_element)>0:
            self.check_first=True
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)           
    def format_name(self, f_name, m_name, l_name):
        return " ".join([y.strip() for y in [f_name, " ".join([i for i in [m_name, l_name] if i])] if y])