'''
Created on 02-Jun-2018

@author: srinivasan
'''
import string

import scrapy

from Data_scuff.items.Items import MarylandItems
from Data_scuff.spiders.__common import CommonSpider


class Maryland(CommonSpider):
    
    name = "maryland"
    allowed_domains = ["egov.maryland.gov"]
    URL = "https://egov.maryland.gov"
    
    """custom_settings = {
        'ITEM_PIPELINES' : CustomSettings.appenAndgetItemPipelinevalues({
            'Data_scuff.pipeline.common.DropDuplicateUrlPipeline': 100,
            })
        }
    """
    start_urls = ["https://egov.maryland.gov/BusinessExpress/EntitySearch", ]
    form_url = start_urls[0]
    search_keys = [i for i in string.ascii_uppercase + string.digits]
    mapping_dict = {"Department ID Number:":"permit_lic_no", "Business Name:":"company_name",
                "Principal Office:":"location_address_string", "Owner:":"mixed_name", "Resident Agent:":"mixed_name",
                "Status:":"permit_lic_status", "Business Type:":"company_type", "Business Code:":"business_code",
                "Good Standing:":"good_standing", "Date of Formation/ Registration:":"creation_date", "State of Formation:":"hq_indicator",
                "Stock Status:":"public_private_indicator", "Close Status:":"close_status",
                "Renewal Date:":"permit_lic_eff_date", "Expiration Date:":"permit_lic_exp_date"}

    def __init__(self, *args, **kwargs):
        super(Maryland, self).__init__(*args, **kwargs)
    
    def parse(self, response):
        form_data = self.__getFormData(response)
        for key in self.search_keys:
            form_data["BusinessName"] = key
            yield self.form_request(form_data, self.on_search)
    
    def __getFormData(self, response):
        return {
            "__RequestVerificationToken": response.xpath("//input[@name='__RequestVerificationToken']/@value").extract_first(),
            "SearchAction": "Search",
            "ReturnUrl": "",
            "BusinessName": "",
            "DepartmentId": "",
            "SearchType": "name"
        }
    
    def on_search(self, response):
        rows = response.xpath("//table[@id='tblBusSearch']/tbody/tr")
        for row in rows:
            tds = row.xpath("td")
            url = tds[3].xpath("a[@class='btn']/@href").extract_first()
            url = response.urljoin(url)
            yield scrapy.Request(url=url, callback=self.on_detail_page)
    
    def on_detail_page(self, response):
        item = MarylandItems()
        item["url"] = response.url
        details = response.xpath("//div[@class='fp_formItemGroup']/div[contains(@class, 'fp_formItem') or contains(@class, 'fb_formItem fp_NoFormItem')]")
        for detail in details:
            label = detail.xpath("span[contains(@class, 'fp_formItemLabel')]/strong/text()").extract_first()
            rawdata = detail.xpath("span[contains(@class, 'fp_formItemData')]/text()").extract_first()
            data = rawdata.strip() if rawdata else rawdata
            if label == "Owner:":
                item["mixed_name"] = data
                item["mixed_subtype"] = "owner"
            elif label == "Resident Agent:":
                item["mixed_name"] = data
                item["mixed_subtype"] = "agent"
            else:
                val = self.mapping_dict.get(label)
                if val:
                    item[val] = data
        return item
