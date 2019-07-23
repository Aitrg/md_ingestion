# -*- coding: utf-8 -*-
'''
Created on 11-Jun-2018

@author: srinivasan
'''

import scrapy
from scrapy.http.request.form import FormRequest
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import replace_escape_chars

from Data_scuff.items.Items import NyauthorityItem
from Data_scuff.spiders.__common import CommonSpider
from Data_scuff.utils.utils import Utils


class NyauthoritySpider(CommonSpider):
    
    name = 'nyauthority'
    allowed_domains = ['tran.sla.ny.gov']
    start_urls = ['https://www.tran.sla.ny.gov/JSP/query/PublicQueryPrincipalSearchPage.jsp']
    
    custom_settings = {
        'CSV_DELIMITER':'|',
        'FILE_NAME':Utils.getRundateFileName('License_Vehicle Inspection Stations_NY_CurationReady'),
        'TOP_HEADER':{'company_name': 'Premises Name', 'mixed_name': "Principal's Name", 'mixed_subtype': '', 'dba_name': 'Trade Name',
                       'zone': 'Zone', 'county': 'County', 'location_address_string': 'Address/zone', 'licence_class': 'License Class',
                       'licence_type_code': 'License Type Code', 'permity_subtype': 'License Type', 'permit_lic_exp_date': 'Expiration Date',
                        'permit_licence_status': 'License Status', 'permit_lic_no': 'Serial Number', 'credit_group': 'Credit Group',
                        'permit_lic_eff_date': 'Filing Date', 'permit_applied_date': 'Effective Date', 'permit_type': ' ',
                         'url': ' ', 'source_name': ' ', 'ingestion_timestamp': ' '},
        'FIELDS_TO_EXPORT':['company_name', 'mixed_name', 'mixed_subtype', 'dba_name', 'zone', 'county',
                            'location_address_string', 'licence_class', 'licence_type_code', 'permity_subtype', 'permit_lic_exp_date',
                            'permit_licence_status', 'permit_lic_no', 'credit_group', 'permit_lic_eff_date',
                            'permit_applied_date', 'permit_type', 'url', 'source_name', 'ingestion_timestamp']
        }
    
    def parse(self, response):
        yield FormRequest.from_response(response, formname="principal",
                                        formdata=self.__getFormData(),
                                         callback=self.on_search,
                                         errback=self.handle_form_error)
    
    def __getFormData(self):
        return {
        'validated' : 'true',
        'principalName' : 'abbr'
        }
    
    def on_search(self, response):
        for row in response.xpath("//div[2]/table/tr"):
            tds = row.xpath("td")
            if len(tds) > 1:
                licence_class = tds[2].xpath('text()').extract_first()
                if licence_class:
                    url = response.urljoin(tds[0].xpath("a/@href").extract_first())
                    location_address = ' '.join(str(e).strip() for e in tds[1].xpath("text()").extract())
                    icence_type_code = tds[3].xpath('text()').extract_first()
                    yield scrapy.Request(url=url, callback=self.on_detail_page, dont_filter=True,
                                          meta={"address": location_address, "lic_class": licence_class,
                                                "lic_code": icence_type_code})
    
    def on_detail_page(self, response):
        self.logger.info("started to extracting CSV data from {}".format(response.url))
        il = ItemLoader(item=NyauthorityItem(), response=response)
        il.default_output_processor = MapCompose(lambda v: v.strip(), replace_escape_chars)
        il.add_xpath('company_name', '//table[5]/tr[2]/td[3]/text()')
        il.add_xpath('mixed_name', '//table[5]/tr[1]/td[3]/text()')
        il.add_xpath('dba_name', '//table[5]/tr[3]/td[3]/text()')
        il.add_xpath('zone', '//table[5]/tr[4]/td[3]/text()')
        il.add_xpath('county', '//table[5]/tr[8]/td[3]/text()')
        il.add_xpath('permity_subtype', '//table[3]/tr[2]/td[3]/text()')
        il.add_xpath('permit_lic_exp_date', '//table[3]/tr[7]/td[3]/text()')
        il.add_xpath('permit_lic_no', '//table[3]/tr[1]/td[3]/text()')
        il.add_xpath('credit_group', '//table[3]/tr[4]/td[3]/text()')
        il.add_xpath('permit_lic_eff_date', '//table[3]/tr[6]/td[3]/text()')
        il.add_xpath('permit_applied_date', '//table[3]/tr[5]/td[3]/text()')
        il.add_xpath('permit_licence_status', '//table[3]/tr[3]/td[3]/text()',
                     MapCompose(lambda v: v.replace('License is', "").strip()))
        il.add_value('url', response.url)
        il.add_value('mixed_subtype', 'principal')
        il.add_value('permit_type', 'liquor_license')
        il.add_value('source_name', 'NY_Liquor_Licenses')
        il.add_value('ingestion_timestamp', self.getingestion_timestamp())
        il.add_value('location_address_string', response.meta['address'])
        il.add_value('licence_class', response.meta['lic_class'])
        il.add_value('licence_type_code', response.meta['lic_code'])
        return il.load_item()
