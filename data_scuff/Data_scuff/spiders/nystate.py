'''
Created on 05-Jun-2018

@author: srinivasan
'''

from scrapy.loader import ItemLoader
from scrapy.spiders.feed import CSVFeedSpider

from Data_scuff.items.Items import NyAlbanyStateItems
from Data_scuff.utils.utils import Utils


class NyAlbanyStateSpider(CSVFeedSpider):
    
    name = 'data_ny_albany_gov'
    allowed_domains = ['data.ny.gov']
    start_urls = ['https://data.ny.gov/api/views/kb9s-4gzd/rows.csv']
    delimiter = ','
    quotechar = "'"
    custom_settings = {
        'CSV_DELIMITER':'|',
        'FILE_NAME':Utils.getRundateFileName('Permits_Buildings_NY_Albany_CurationReady'),
        'TOP_HEADER':{'permit_lic_no': 'Permit Number', 'permit_lic_eff_date': 'Date', "application_number":"Application Number",
                      'location_address_string': 'Address', 'person_name': 'Owner', 'person_subtype': '', 'contractor_name': 'Contractor',
                      'permit_lic_value': 'Estimated Cost', 'permit_lic_fee': 'Fee', 'permit_lic_desc': 'Description of Work',
                       'longitude': 'Longitude', 'latitude': 'Latitude', 'permit_type': '', 'url': '',
                       'sourceName': '', 'ingestion_timestamp': ''},
        'FIELDS_TO_EXPORT':['permit_lic_no', 'permit_lic_eff_date', 'application_number', 'location_address_string', 'person_name', 'person_subtype',
                            'contractor_name', 'permit_lic_value', 'permit_lic_fee', 'permit_lic_desc', 'longitude', 'latitude',
                            'permit_type', 'url', 'sourceName', 'ingestion_timestamp']
        }

    """
    Parsing the row from csv file
    """

    def parse_row(self, response, row):
        self.logger.info("started to extracting CSV data from {}".format(response.url))
        il = ItemLoader(item=NyAlbanyStateItems())
        lat, lng = map(str, row['Location'].splitlines()[-1].strip('()').split(','))
        il.add_value('permit_lic_no', row['Permit Number'])
        il.add_value('permit_lic_eff_date', row['Date'])
        il.add_value('application_number', row['Application Number'])
        il.add_value('location_address_string', row['Address'])
        il.add_value('person_name', row['Owner'])
        il.add_value('person_subtype', "Owner")
        il.add_value('contractor_name', row['Contractor'])
        il.add_value('permit_lic_value', row['Estimated Cost'])
        il.add_value('permit_lic_fee', row['Fee'])
        il.add_value('permit_lic_desc', row['Description of Work'])
        il.add_value('longitude', lng)
        il.add_value('latitude', lat)
        il.add_value('permit_type', "building_permits")
        il.add_value('url', response.url)
        il.add_value('sourceName', "NY_Albany_Building_Permits")
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        return  il.load_item()