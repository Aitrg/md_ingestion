# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-26 05:23:50
TICKET NUMBER -AI_1415
@author: ait
'''

from scrapy.spiders import CSVFeedSpider
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1415.items import IlChampagneUrbanaBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CustomSettings
from Data_scuff.spiders.__common import DataFormatterMixin,LookupDatareaderMixin
from Data_scuff.utils.utils import Utils
import re
import scrapy


class IlChampagneUrbanaBuildingPermitsSpider(CSVFeedSpider,LookupDatareaderMixin, DataFormatterMixin):
    name = '1415_il_champagne_urbana_building_permits'
    allowed_domains = ['urbanaillinois.us']
    start_urls = ['https://data.urbanaillinois.us/api/views/9rzq-mqbh/rows.csv?accessType=DOWNLOAD']
    # headers = ['id', 'name', 'description', 'image_link']
    # delimiter = '\t'
    count = 1
   
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1415_Permits_Buildings_IN_Champagne_Urbana_CurationReady'),
        'JIRA_ID':'AI_1415',
        # 'JOBDIR' : CustomSettings.getJobDirectory('il_champagne_urbana_building_permits'),
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        'TOP_HEADER':{                        'dba_name': '',
                         'improvement type': 'Improvement Type',
                         'issue month': 'Issue Month',
                         'issue year': 'Issue Year',
                         'latitude': 'Latitude',
                         'location_address_string': 'Address',
                         'longitude': 'Longitude',
                         'mixed_name': 'Applicant',
                         'occupancy_subtype': 'Building Type',
                         'parcel number': 'Parcel Number',
                         'permit_lic_desc': '',
                         'permit_lic_eff_date': 'Issue Date',
                         'permit_lic_fee': 'Fee Amount',
                         'permit_lic_no': 'Permit Number',
                         'permit_lic_status': 'Permit Status',
                         'permit_lic_value': 'Estimated Cost',
                         'permit_subtype': 'Permit Type',
                         'permit_type': '',
                         'square_footage': 'Square Feet'},
        'FIELDS_TO_EXPORT':[
                         'permit_lic_no',
                         'permit_subtype',
                         'permit_lic_desc',
                         'permit_lic_eff_date',
                         'issue month',
                         'issue year',
                         'location_address_string',
                         'parcel number',
                         'improvement type',
                         'occupancy_subtype',
                         'permit_lic_value',
                         'permit_lic_fee',
                         'square_footage',
                         'mixed_name',
                         'dba_name',
                         'permit_lic_status',
                         'latitude',
                         'longitude',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp',
                         ],
        'NULL_HEADERS':['longitude', 'parcel number', 'issue year', 'issue month', 'latitude', 'improvement type']
        }

    # Do any aadaptations you need here
    #def adapt_response(self, response):
    #    return response
    def is_float_num(self,o):
        try:
            float(o)
            return True
        except:
            return False

    def parse_row(self, response, row):
        il = ItemLoader(item=IlChampagneUrbanaBuildingPermitsSpiderItem())
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('permit_lic_no', row['Permit Number'])
        il.add_value('permit_subtype', row['Permit Type'])
        il.add_value('permit_lic_desc', row['Permit Type'])
        il.add_value('permit_lic_eff_date', row['Issue Date'])
        il.add_value('issue month', row['Issue Month'])
        il.add_value('issue year', row['Issue Year'])
        # print(self.count)
        # print(len(row["Address"]))
        # print(row["Address"])
        # self.count+=1
        co_ordinates=row['Permit Address - Mapped']
        # addr = co_ordinates[len(row['Address'])-1:co_ordinates.rfind("\n")].split(",")
        # print(addr[0]+" "+addr[1])
        # print(co_ordinates[len(row['Address'])-1:co_ordinates.rfind("(")])
        if ('(' in  co_ordinates):
            addr = co_ordinates[:co_ordinates.rfind("\n")]
        else:
            addr = co_ordinates
        # print(addr)
        # addr = co_ordinates.split('(')[0]
        addr_formated = self.format_address(address = "",city = addr, _zip = '').replace("\n",",",1)
        # addr_formated = 
        # print(addr_formated)
        # addr = row['Address']+',IL'
        il.add_value('location_address_string',addr_formated)
        # print(co_ordinates.split('(')[0]+)
        il.add_value('parcel number', row['Parcel Number'])
        il.add_value('improvement type', row['Improvement Type'])
        il.add_value('occupancy_subtype', row['Building Type'])
        il.add_value('permit_lic_value', '$'+row['Estimated Cost'])
        il.add_value('permit_lic_fee', '$'+row['Fee Amount'])
        il.add_value('square_footage', row['Square Feet'])
        dba_name = self._getDBA(row['Applicant'])
        il.add_value('mixed_name', dba_name[0])
        # if self.count == 641 or self.count == 762:
        #     print("____________________DBA Name",self.count)
        #     print(dba_name)
        # self.count+=1
        il.add_value('dba_name', dba_name[1])
        il.add_value('permit_lic_status', row['Permit Status'])
        
        a = co_ordinates[co_ordinates.rfind('(')+1:co_ordinates.rfind(')')].split(',')
        # print("_____________________________Co-Ordinates ",self.count)
        # print(a[0]+" "+a[1])
        
        # try:
        #     print(float(a[0])," ",float(a[1]))
        #     print(a[0].isdigit()," ",a[1].isdigit())
        #     il.add_value('latitude', float(a[0]))
        #     il.add_value('longitude', float(a[1]))
        # except:
        #     il.add_value('latitude', " ")
        #     il.add_value('longitude', " ")
        #     print("This is not float")
        #     print("__________________________Not float")
        # self.count+=1
        
        if (self.is_float_num(o=a[0]) and self.is_float_num(o=a[1])):        
            il.add_value('latitude', a[0])
            il.add_value('longitude', a[1])
            print(float(a[0])," ",float(a[1]))
        else:
            il.add_value('latitude', "")
            il.add_value('longitude', '')
            print("This is not float")

        il.add_value('permit_type', 'building_permit')
        il.add_value('sourceName', 'IL_Champagne_Urbana_Building_Permits')
        il.add_value('url', 'https://data.urbanaillinois.us/Buildings/Building-Permits-Issued/x5kj-g5aj')
		# il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('ingestion_timestamp', '20190624_114324')
        yield il.load_item()
        # print("_____________________________________",row['Permit Number'])


