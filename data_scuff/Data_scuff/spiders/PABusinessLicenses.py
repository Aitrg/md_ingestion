import re

import scrapy
from scrapy.http import FormRequest
from Data_scuff.items.Items import PABusinessLicensesItem
from Data_scuff.spiders.__common import CommonSpider
from Data_scuff.utils.searchCriteria import SearchCriteria
from Data_scuff.utils.utils import Utils


class PABusinessLicensesSpider(CommonSpider):
    name = 'PABusinessLicenses1'
    allowed_domains = ['www.corporations.pa.gov']
    start_url = 'https://www.corporations.pa.gov/search/corpsearch'
    
    custom_settings = {
        'CSV_DELIMITER':'|',
        'DOWNLOAD_DELAY':0.1,
        'FILE_NAME':Utils.getRundateFileName('PA_SOS_Companies_Business_Licenses'),
        'TOP_HEADER':{'company_name':'Business Entity Name', 'name_type':'Name Type', 'location_address_string':'Address', 'permit_lic_no':'Entity Number', 'company_type':'Entity Type',
                            'status':'Status', 'permit_lic_status':'', 'citizenship':'Citizenship', 'creation_date':'Entity Creation Date',
                            'permit_lic_eff_date':'Effective Date', 'state_of_inc':'State of Inc', 'person_name':'Officers/Name',
                            'person_subtype':'Officers/Title', 'person_address_string':'Officers/Address', 'permit_type':'',
                            'url':'', 'sourceName':'', 'ingestion_timestamp':''},
        'FIELDS_TO_EXPORT':['company_name', 'name_type', 'location_address_string', 'permit_lic_no', 'company_type',
                            'status', 'permit_lic_status', 'citizenship', 'creation_date', 'permit_lic_eff_date',
                            'state_of_inc', 'person_name', 'person_subtype', 'person_address_string', 'permit_type',
                            'url', 'sourceName', 'ingestion_timestamp']
    
    }
    entity_name = []
    # def __init__(self):
        
    #     for alpha in range(65, 70):
    #         for alpha1 in range(65, 70):
    #             for alpha2 in range(65, 70):
    #                 for alpha3 in range(65, 70):
    #                     self.entity_name.append(chr(alpha)+chr(alpha1)+chr(alpha2)+chr(alpha3))
    #                     for alpha4 in range (65, 75):
    #                         self.entity_name.append(chr(alpha)+chr(alpha1)+chr(alpha2)+chr(alpha3)+chr(alpha4))

    def form_data(self, response):
        page_form_data = {
            "__LASTFOCUS": "",
            "__EVENTTARGET": "",
            "__EVENTARGUMENT":"",
            "__VIEWSTATE": "",
            "__VIEWSTATEGENERATOR": "",
            "__VIEWSTATEENCRYPTED": "",
            "__EVENTVALIDATION": "",
            "ctl00$MainContent$txtSearchTerms": "",
            "ctl00$MainContent$ddlSearchType": "1",
            "ctl00$MainContent$btnSearch": "Search"
            
        }
        return page_form_data
    
    def start_requests(self):
        yield scrapy.Request(self.start_url, callback=self.on_search)

    def form_request(self, form_data, callback):
        req = FormRequest(url=self.start_url, formdata=form_data, callback=callback, dont_filter=True)
        return req

    def second_form_data(self, response):
        nextpage_form_data = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT":"",
            "__VIEWSTATE": "",
            "__VIEWSTATEGENERATOR": "",
            "__VIEWSTATEENCRYPTED": "",
            "__EVENTVALIDATION":"",
            "gvResults_length": "",
        }
        return nextpage_form_data

    def on_search(self, response):
        form_data = self.form_data(response)
        for form_data in [ {"ctl00$MainContent$txtSearchTerms":key} for key in SearchCriteria.rangeAAAAtoZZZZ()]:
            # form_data["ctl00$MainContent$txtSearchTerms"] ='AAAA'  
            form_data["__VIEWSTATE"] = response.xpath("//input[@id='__VIEWSTATE']/@value").extract_first()
            form_data["__VIEWSTATEGENERATOR"] = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").extract_first()   
            form_data["__EVENTVALIDATION"] = response.xpath("//input[@id='__EVENTVALIDATION']/@value").extract_first()  
            yield scrapy.FormRequest.from_response(response, url=self.form_url, callback=self.second_page_crawl, formdata=form_data)
    
    def second_page_crawl(self, response):
        
        links = re.findall(r'ctl00.*\"', response.text)
        link_count = 0
        count = 0
        form_data = self.second_form_data(response)
        for link in links :
            link_val = link.split(',')
            if(link_val[0].replace('&quot;', '').endswith('BEName')):
                count = count + 1
                form_data["__EVENTTARGET"] = link_val[0].replace('&quot;', '')
                form_data["__VIEWSTATE"] = response.xpath("//input[@id='__VIEWSTATE']/@value").extract_first()
                form_data["__VIEWSTATEGENERATOR"] = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").extract().pop()
                form_data["__EVENTVALIDATION"] = response.xpath("//input[@id='__EVENTVALIDATION']/@value").extract().pop()
                form_data["gvResults_length"] = "-1" 
                yield self.form_request(form_data, self.second_page_crawl1)
    
    def second_page_crawl1(self, response):
        name = response.xpath("normalize-space(//div[@class='row']/div[2]/table/tbody/tr[contains(.,'Name')]/td/text())").extract()
        title = response.xpath("normalize-space(//div[@class='row']/div[2]/table/tbody/tr[contains(.,'Title')]/td/text())").extract()
        address = response.xpath("normalize-space(//div[@class='row']/div[2]/table/tbody/tr[contains(.,'Address')]/td/text())").extract()
        
        if name:
            for row in range(len(name)):
                item = PABusinessLicensesItem()   
                item["company_name"] = response.xpath("normalize-space(//tr/td[1]/span[@id='lbEntityName']/text())").extract_first() 
                item["name_type"] = response.xpath("normalize-space(//tr/td[2]/span[@id='lbNameType']/text())").extract_first() 
                item["location_address_string"] = response.xpath("normalize-space(//tr[9]/td[2]/text())").extract_first()
                item["permit_lic_no"] = response.xpath("normalize-space(//tr[2]/td[2]/text())").extract_first()
                item["company_type"] = response.xpath("normalize-space(//tr[3]/td[2]/text())").extract_first() 
                item["status"] = response.xpath("normalize-space(//tr[4]/td[2]/text())").extract_first() 
                item["permit_lic_status"] = 'Active'
                item["citizenship"] = response.xpath("normalize-space(//tr[5]/td[2]/text())").extract_first() 
                item["creation_date"] = response.xpath("normalize-space(//tr[6]/td[2]/text())").extract_first() 
                item["permit_lic_eff_date"] = response.xpath("normalize-space(//tr[7]/td[2]/text())").extract_first() 
                item["state_of_inc"] = response.xpath("normalize-space(//tr[8]/td[2]/text())").extract_first()
                item["person_name"] = name[row]
                item["person_subtype"] = title[row]
                item["person_address_string"] = address[row].replace(u'\xa0', u' ')
                item["permit_type"] = 'business_license' 
                item["url"] = 'https://www.corporations.pa.gov/search/corpsearch'
                item["sourceName"] = 'PA_SOS_Companies_Business_Licenses'
                item["ingestion_timestamp"] = self.getingestion_timestamp()
                yield item
            
        else:
            item = PABusinessLicensesItem()   
            item["company_name"] = response.xpath("normalize-space(//tr/td[1]/span[@id='lbEntityName']/text())").extract_first() 
            item["name_type"] = response.xpath("normalize-space(//tr/td[2]/span[@id='lbNameType']/text())").extract_first() 
            item["location_address_string"] = response.xpath("normalize-space(//tr[9]/td[2]/text())").extract_first()
            item["permit_lic_no"] = response.xpath("normalize-space(//tr[2]/td[2]/text())").extract_first()
            item["company_type"] = response.xpath("normalize-space(//tr[3]/td[2]/text())").extract_first() 
            item["status"] = response.xpath("normalize-space(//tr[4]/td[2]/text())").extract_first() 
            item["permit_lic_status"] = 'Active'
            item["citizenship"] = response.xpath("normalize-space(//tr[5]/td[2]/text())").extract_first() 
            item["creation_date"] = response.xpath("normalize-space(//tr[6]/td[2]/text())").extract_first() 
            item["permit_lic_eff_date"] = response.xpath("normalize-space(//tr[7]/td[2]/text())").extract_first() 
            item["state_of_inc"] = response.xpath("normalize-space(//tr[8]/td[2]/text())").extract_first()
            item["person_name"] = ''
            item["person_subtype"] = ''
            item["person_address_string"] = ''
            item["permit_type"] = 'business_license' 
            item["url"] = 'https://www.corporations.pa.gov/search/corpsearch'
            item["sourceName"] = 'PA_SOS_Companies_Business_Licenses'
            item["ingestion_timestamp"] = self.getingestion_timestamp()
            yield item
        
