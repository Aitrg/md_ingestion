# -*- coding: utf-8 -*-

'''
Created on 2018-Sep-10 15:45:49
TICKET NUMBER -AI_362
@author: lenovo
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import scrapy
from Data_scuff.spiders.AI_362.items import IlYellowPagesSpiderItem
from Data_scuff.spiders.__common import CommonSpider, CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.shell import inspect_response
from inline_requests import inline_requests
import re
import time

class IlYellowPagesSpider(CommonSpider):
    name = '362_il_yellow_pages_0_50'
    allowed_domains = ['google.com','yellowpages.com','zipcodestogo.com']
    start_urls = ['https://www.google.com/']
    # start_urls = ['https://www.yellowpages.com/']
    a = None
    b = None
    # global_state = input("Enter the state: ")
    handle_httpstatus_list = [403]
    allState =  {
            'AK':'Alaska',
            'AL':'Alabama',
            'AR':'Arkansas',
            'AZ':'Arizona',
            'CA':'California',
            'CO':'Colorado',
            'CT':'Connecticut',
            'DE':'Delaware',
            'FL':'Florida',
            'GA':'Georgia',
            'HI':'Hawaii',
            'IA':'Iowa',
            'ID':'Idaho',
            'IL':'Illinois',
            'IN':'Indiana',
            'KS':'Kansas',
            'KY':'Kentucky',
            'LA':'Louisiana',
            'MA':'Massachusetts',
            'MD':'Maryland',
            'ME':'Maine',
            'MI':'Michigan',
            'MN':'Minnesota',
            'MO':'Missouri',
            'MS':'Mississippi',
            'MT':'Montana',
            'NC':'North Carolina',
            'ND':'North Dakota',
            'NE':'Nebraska',
            'NH':'New Hampshire',
            'NJ':'New Jersey',
            'NM':'New Mexico',
            'NV':'Nevada',
            'NY':'New York',
            'OH':'Ohio',
            'OK':'Oklahoma',
            'OR':'Oregon',
            'PA':'Pennsylvania',
            'PR':'Puerto Rico',
            'RI':'Rhode Island',
            'SC':'South Carolina',
            'SD':'South Dakota',
            'TN':'Tennessee',
            'TX':'Texas',
            'UT':'Utah',
            'VA':'Virginia',
            'VT':'Vermont',
            'WA':'Washington',
            'WI':'Wisconsin',
            'WV':'West Virginia',
            'WY':'Wyoming'
        }
    custom_settings = {
        # 'ITEM_PIPELINES':CustomSettings.appenAndgetItemPipelinevalues({'Data_scuff.pipeline.common.DropDuplicateItemPipeline':1}),
        # 'FILE_NAME':Utils.getRundateFileName(allState[global_state.title()]+'YellowPagesSpider'),
        'FILE_NAME':Utils.getRundateFileName('StateYellowPagesSpider'),
        'JIRA_ID':'AI_362',
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        'RANDOM_PROXY_DISABLED':True,
        'DOWNLOAD_DELAY':5,
        'CONCURRENT_REQUESTS':1,
        'DOWNLOADER_MIDDLEWARES' : CustomSettings.appenDownloadMiddlewarevalues({
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
            'Data_scuff.middleware.common.TooManyRequestsRetryMiddleware': 543,
            }),
        'TOP_HEADER':{   'rating': 'BBB Rating',
                         'brands': 'Brands',
                         'category ': 'Category ',
                         'yp_id':'yp_id',
                         'city + state': 'City + State',
                         'company_email': 'Email Business',
                         'company_name': 'Business Name',
                         'company_phone': 'Phone Number',
                         'company_subtype': 'Categories',
                         'company_website': 'Website Link/Other Link',
                         'general info/ biography': 'General Info/ Biography',
                         'hours_of_operation': 'Hours',
                         'languages spoken': 'Languages Spoken',
                         'location_address_string': 'Address',
                         'ops_desc': 'Services/Products',
                         'other info': 'Other Info',
                         'years in business': 'Years in Business',
                         'sourceName':'sourceName',
                         'url':'url',
                         'ingestion_timestamp':'ingestion_timestamp'},
        'FIELDS_TO_EXPORT':['category ',
                         'yp_id',
                         'city + state',
                         'company_name',
                         'location_address_string',
                         'company_phone',
                         'years in business',
                         'general info/ biography',
                         'languages spoken',
                         'rating',
                         'hours_of_operation',
                         'ops_desc',
                         'brands',
                         'company_subtype',
                         'other info',
                         'company_email',
                         'company_website',
                         'sourceName',
                         'url',
                         'ingestion_timestamp'],
        'NULL_HEADERS':['category ', 'city + state', 'years in business', 'general info/ biography', 'languages spoken', 'brands', 'other info']
        }

    check_first = True
    city_list = [] 
    # category_full_list =['Plumbers', 'Locksmiths', 'Dentists', 'Doctors', 'Veterinarians', 'Attorneys', 'Auto Repair', 'Auto Insurance', 'Restaurants']
    category_full_list = ['Restaurant/Caterers/Food establishment', 'Tech Comp', 'Accountants', 'Cosmetology', 'Lawyers/Legal', 'Medical (ALL)/Dental/Veterinary', 'Blood Banks', 'Kidney Dialysis Centers/Medical Labs', 'Physical Therapy', 'Studio/Photography', 'Wedding/Event Planner', 'Financial (Advisors/Trust)', 'Title/Title Insurance', 'Athletics & Drafting', 'Consulting Services', 'Engineering/Surveyors/Architects', 'Tech Programming/Graphic Design', 'PR Firms', 'Clothing/Clothing Rental/Bridal', 'Luggage/Leather Goods', 'Shoe Stores/Repair/Tailor', 'Camera Repair/Cell Phone Repair', 'Candy Stores', 'Cosmetics/Beauty Stores', 'Food/Health Supplement Stores', 'Musical Instrument/Supplies', 'Optical Goods', 'Tocacco/Vaping/E-Cigs', 'Video/Comp Games/Console store', 'Comp/Electronics repair', 'Bicycle store', 'Book store', 'Electronics stores', 'Florist/Gift/Novelty/Hobby/Craft/Art Supply', 'Mailing/Packaging', 'Model/Toy/Game Store']
    category_list =[]
    cityy = ''
    categoryy = ''
    def parse(self, response):
        url = 'https://www.zipcodestogo.com/'+self.allState.get(self.start)+'/'
        yield scrapy.Request(url=url, callback = self.getCity, dont_filter=True)


    def getCity(self, response):
        if len(self.city_list) == 0:
            self.city_list = list(set(response.xpath("//*[@id='leftCol']/table/tr/td/table/tr/td[2]/text()").extract()))

        if len(self.category_list) == 0:
            self.category_list = self.category_full_list.copy()
            self.cityy = self.city_list.pop(0)

        if len(self.category_list) > 0:
            self.categoryy = self.category_list.pop(0)

            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'}
            urll = 'https://www.yellowpages.com/search?search_terms={}&geo_location_terms={}+{}'.format(self.categoryy, self.cityy, self.start)

            yield scrapy.Request(url=urll, callback=self.get_detail_list, dont_filter=True, headers=headers)



    # @inline_requests
    def get_detail_list(self, response):
        # inspect_response(response,self)
        links_list = response.xpath('//*[@class="info"]/h2[@class="n"]/a[@class="business-name"]/@href').extract()
        
        # links_list = ['/anchorage-ak/mip/roto-rooter-468119931?lid=1001991436715', '/culver-city-ca/mip/dave-blair-plumbing-481152576?lid=1001926175069', '/baldwin-park-ca/mip/advanced-plumbing-solutions-468103022?lid=1001884526903', '/arcadia-ca/mip/western-supreme-rooter-2586159?lid=1001692726701', '/culver-city-ca/mip/dave-blair-plumbing-481152576?lid=1001926175069', '/fairbanks-ak/mip/glacier-point-services-inc-8703381','/nationwide/mip/titan-wheel-accessories-533506296?lid=1002003454969', '/dell-rapids-sd/mip/robin-hattervig-513983371']
        
        for linkk in links_list:
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36'}
            urll = 'https://www.yellowpages.com'+linkk
            # scrap_data_res = yield scrapy.Request(url=urll, dont_filter=True, meta={'cookiejar':linkk}, headers=headers)
            yield scrapy.Request(url=urll, dont_filter=True, meta={'cookiejar':linkk}, headers=headers, callback=self.scrap_data)

        if "Next" in response.xpath('//*[@class="next ajax-page"]/text()').extract():
            cookiejarr = response.xpath('//*[@class="next ajax-page"]/@href').extract()[0]
            yield scrapy.Request(url = 'https://www.yellowpages.com'+response.xpath('//*[@class="next ajax-page"]/@href').extract_first(), callback=self.get_detail_list, dont_filter=True, meta={'cookiejar':cookiejarr})
        elif len(self.city_list) > 0:
            self.crawler.engine.pause()
            time.sleep(10)
            self.crawler.engine.unpause()
            yield scrapy.Request(url='https://www.yellowpages.com/', callback = self.getCity, dont_filter=True)

    def scrap_data(self,response):
        scrap_data_res = response
        if 'upgrade-browser' not in scrap_data_res.url or "yellowpages.com" not in (scrap_data_res.url).split("=")[-1] or 'There seems to have been an issue with your search.' not in scrap_data_res.text:

            try:
                company_name = scrap_data_res.xpath('//*[@class="sales-info"]//text()').extract_first()
                # address = scrap_data_res.xpath('//*[@class="address"]//text()').extract_first()
                try:
                    address = ''.join(scrap_data_res.xpath('//*[@class="address"]/span/text()').extract())
                except:
                    address = self.start

                company_phone_val = scrap_data_res.xpath('//*[@class="phone"]//text()').extract_first()

                company_phone_list = scrap_data_res.xpath('//*[@class="extra-phones"]//text()').extract()
                company_phone_val2 = "; ".join(company_phone_list) if company_phone_list and len(company_phone_list) > 0 else ''

                business_desc_val = scrap_data_res.xpath('//*[@class="general-info"]/text()').extract_first()

                if not business_desc_val:
                    business_desc_val = scrap_data_res.xpath('//*[contains(text(),"Biography")]/following::dd/text()').extract_first()

                brands_list = scrap_data_res.xpath('//*[@class="brands"]//text()').extract()
                brands_val = "; ".join(brands_list) if brands_list and len(brands_list) > 0 else ''

                category_list = scrap_data_res.xpath('//*[@class="categories"]//text()').extract()
                category_val = ", ".join(category_list) if category_list and len(category_list) > 0 else ''

                abc = scrap_data_res.xpath('//*[contains(text(),"Services/Products")]/following::dd[1]/text()').extract()
                if abc and len(abc) > 0:
                    services_products = "; ".join(abc).replace("|", ";").replace("||",";")
                else:
                    services_products_list = scrap_data_res.xpath('//*[contains(text(),"Services/Products")]/following::dd[1]/ul/li/text()').extract()
                    services_products =  "; ".join(services_products_list) if services_products_list and len(services_products_list) > 0 else ''

                hour_list = scrap_data_res.xpath("//time/@datetime").extract()
                hours_val = "; ".join(hour_list) if hour_list and len(hour_list) > 0 else ''

                years_in_bus = scrap_data_res.xpath('//*[@class="number"]/text()').extract_first()
                languages_spoken = scrap_data_res.xpath('//*[@class="languages"]/text()').extract_first()
                bbb_rating = scrap_data_res.xpath('//*[@class="bbb-rating"]/a/span/text()').extract_first()

                a_list = scrap_data_res.xpath('//*[@class="other-information"]/p/strong/text()').extract()
                b_list = scrap_data_res.xpath('//*[@class="other-information"]/p/text()').extract()
                if a_list and b_list:
                    a = ("=+".join(a_list)).split("=+")
                    b = ("=+".join(b_list).replace("\xa0","").strip(":")).split("=+")
                    
                    other_info = ("; ".join([_a+":"+_b.replace(":","") for _a,_b in zip(a,b)])).strip(":")
                else:
                    other_info_list = scrap_data_res.xpath('//*[@class="other-information"]/p/text()').extract()
                    if other_info_list and len(other_info_list):
                        other_info = (other_info_list[0].replace("\xa0","").strip(":")).strip(":")
                    else:
                        other_info = ''

                company_email = scrap_data_res.xpath('//*[@class="email-business"]/@href').extract_first()
                company_website = scrap_data_res.xpath('//*[@class="secondary-btn website-link"]/@href').extract_first()

                il = ItemLoader(item=IlYellowPagesSpiderItem(),response=response)
                il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                il.add_value('sourceName', self.start+'_Yellow_Pages ')

                urll = response.url
                il.add_value('url', urll)

                if '?lid=' in urll:
                    lid_val = urll.split('=')[1]
                    if '&' in lid_val:
                        lid_val = lid_val.split('&')[0]
                    il.add_value('yp_id', lid_val)
                else:
                    ind=lambda data:(data.rfind("="))+1 if '=' in data else (data.rfind('-'))+1
                    il.add_value('yp_id', urll[ind(urll):])
                
                il.add_value('category ', self.categoryy)
                il.add_value('city + state', self.cityy+', '+self.start)
                il.add_value('company_name', company_name)
                il.add_value('location_address_string', address if address and len(address) > 2 else self.start)

                Company_phone_og = "; ".join([re.sub('\s+', ' ', i) for i in [company_phone_val, company_phone_val2] if i])
                il.add_value('company_phone', Company_phone_og.rstrip("; ").lstrip(";"))

                il.add_value('years in business', years_in_bus)
                il.add_value('general info/ biography', re.sub('\s+', ' ', business_desc_val) if business_desc_val else '')
                il.add_value('languages spoken', languages_spoken)
                il.add_value('rating', bbb_rating if bbb_rating and len(bbb_rating)>0 else '')
                il.add_value('hours_of_operation', hours_val.replace("Mo","Mon").replace("Tu","Tue").replace("We","Wed").replace("Th","Thu").replace("Fr","Fri").replace("Su","Sun").replace("Sa","Sat") if hours_val and len(hours_val) > 2 else '')
                il.add_value('ops_desc', re.sub('\s+', ' ', services_products).rstrip(';').lstrip(';') if services_products else '')
                il.add_value('brands', re.sub('\s+', ' ', brands_val).rstrip(';').lstrip(';') if brands_val else '')
                if category_val:
                   category_val = category_val.replace(',,',', ').replace(', ,',', ').replace(', , ',', ')
                else:
                    category_val = ''
                il.add_value('company_subtype', re.sub('\s+', ' ', category_val).replace('categouries','').replace('categories',''))

                il.add_value('other info', other_info)
                il.add_value('company_email', company_email.replace('mailto:service@westernrooter.com', '') if company_email else '')
                il.add_value('company_website', company_website)
                
                yield il.load_item()
            except:
                print('-------------------------except')
                pass

        # if "Next" in response.xpath('//*[@class="next ajax-page"]/text()').extract():
        #     cookiejarr = response.xpath('//*[@class="next ajax-page"]/@href').extract()[0]
        #     yield scrapy.Request(url = 'https://www.yellowpages.com'+response.xpath('//*[@class="next ajax-page"]/@href').extract_first(), callback=self.get_detail_list, dont_filter=True, meta={'cookiejar':cookiejarr})
        # elif len(self.city_list) > 0:
        #     yield scrapy.Request(url='https://www.yellowpages.com/', callback = self.getCity, dont_filter=True)

