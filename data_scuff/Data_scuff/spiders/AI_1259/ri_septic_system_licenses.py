

'''
scrapy  crawl 1259_ri_septic_system_licenses -a start=1 -a end=40


Created on 2019-May-15 12:09:29
TICKET NUMBER -AI_1259
@author: MS-Muhil
'''
import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_1259.items import RiSepticSystemLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider, CustomSettings
from Data_scuff.utils.utils import Utils
from inline_requests import  inline_requests
from nltk.corpus.reader import lin
import re

class RiSepticSystemLicensesSpider(CommonSpider):
    name = '1259_ri_septic_system_licenses'
    allowed_domains = ['ri.gov']
    start_urls = ['https://www.ri.gov/DEM/isdssearch/']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1259_Licenses_RI_Septic_System_CurationReady'),
        'JIRA_ID':'AI_1259',
        'DOWNLOAD_DELAY':.2,
        
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('ri_septic_system_licenses'),
        'TOP_HEADER':{                        'city/town': 'City/Town',
                         'company_name': 'Corp Owner',
                         'dba_name': '',
                         'location_address_string': 'Location',
                         'lot': 'lot',
                         'permit_lic_desc': '',
                         'permit_lic_no': 'Application number',
                         'permit_type': '',
                         'person_name': 'Designer Name',
                         'plat': 'Plat',
                         'sublot': 'sublot',
                        'total flow': 'Total Flow'
                         },
        'FIELDS_TO_EXPORT':['permit_lic_no', 'city/town', 'location_address_string', 'plat', 'lot', 'sublot', 'company_name', 'dba_name', 'person_name', 'total flow', 'permit_lic_desc', 'permit_type', 'url', 'sourceName', 'ingestion_timestamp'],
        'NULL_HEADERS':['city/town', 'plat', 'lot', 'sublot','total flow' ]
        }

    # @inline_requests
    def parse(self, response):
        self.rem_lis=lambda lis:[dd.replace('[\n\t\r]','').strip() for dd in lis]
        print('start = ',self.start,'  end = ',self.end)
        option = response.xpath("//select[@id='town']/option/@value").extract()[int(self.start):int(self.end)]
        print('======>city : ',option)
        self.pos=0
        self.city=option
        self.url = 'https://www.ri.gov/DEM/isdssearch/index.php?town='
        yield scrapy.Request(url=self.url + self.city[self.pos], dont_filter=True, meta={'city':self.city[self.pos]},callback=self.parse_data)
    @inline_requests
    def parse_data(self, response):
        print('============> ',response.meta['city'],' : ',response.xpath('//p/text()').extract())
        rem_esc=lambda data: re.sub('\s+',' ',data.replace('[\n\t\r]','').strip()) if data else ''
        view_links = response.xpath("//td/div/a/@href").extract()
        permit_lic=response.xpath("//td/a/text()").extract()
        if response.xpath("//td/div/a/@href").extract():
            pass
            for link,permit_lic_no in zip(view_links,permit_lic):
                main_res=yield scrapy.Request(url='https://www.ri.gov/DEM/isdssearch/'+link.strip())
                location_address_string=rem_esc(main_res.xpath("//em[contains(text(),'Location')]/following::text()").extract_first())
                plat_lot=rem_esc(''.join(main_res.xpath('//em[contains(text(),"Plat")]/following::text()').extract()[:2]))
                Owner_name=rem_esc(main_res.xpath("//em[contains(text(),'Owner Name')]/following::text()").extract_first())
                corp_owner=rem_esc(main_res.xpath("//em[contains(text(),'Corp Owner')]/following::text()").extract_first())
                designer=rem_esc(main_res.xpath("//em[contains(text(),'Designer')]/following::text()").extract_first())
                total=rem_esc(main_res.xpath("//em[contains(text(),'Total')]/following::text()").extract_first())
                plat=''
                lot=''
                sublot=''
                if plat_lot:
                    if 'Plat' in plat_lot and 'Lot' in plat_lot and 'Sublot' in plat_lot :
                        plat=re.search('Plat.*Lot',plat_lot).group()[4:-3].strip()
                        lot= re.search('Lot.*Sublot',plat_lot).group()[3:-6].strip()
                        sublot=re.search('Sublot.*',plat_lot).group()[6:].strip()
                    elif 'Plat' in plat_lot and 'Lot' in plat_lot :
                        plat=re.search('Plat.*Lot',plat_lot).group()[4:-3].strip()
                        lot= re.search('Lot.*',plat_lot).group()[3:].strip()
                    elif 'Plat' in plat_lot and 'Sublot' in plat_lot :
                        plat=re.search('Plat.*Sublot',plat_lot).group()[4:-6].strip()
                        lot= re.search('Sublot.*',plat_lot).group()[6:].strip()
                    elif 'plat' in plat_lot.lower():
                        plat=re.search('Plat.*',plat_lot).group()[4:].strip()
                il = ItemLoader(item=RiSepticSystemLicensesSpiderItem(),response=response)
                il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                il.add_value('sourceName', 'RI_Septic_System_Licenses')
                il.add_value('url', 'https://www.ri.gov/DEM/isdssearch/')
                il.add_value('permit_lic_no', permit_lic_no)
                il.add_value('city/town', response.meta['city'])
                il.add_value('location_address_string', location_address_string.strip()+", RI")
                il.add_value('plat', (plat.upper().strip())[:-1] if plat.endswith('&') else plat.upper())
                il.add_value('lot', (lot.upper().strip())[:-1] if lot.endswith('&') else lot.upper())
                il.add_value('sublot', (sublot.upper().strip())[:-1] if sublot.endswith('&') else sublot.upper())
                company_name=corp_owner if corp_owner.strip() else Owner_name if Owner_name.strip() else designer if designer.strip() else ''
                com_name=self._getDBA(company_name)
                designer_dba=self._getDBA(designer)
                permit_lic_desc='Septic System Licenses'
                if com_name[0]:
                    permit_lic_desc+=" For "+com_name[0]
                il.add_value('company_name', com_name[0] if com_name[0].strip() else designer_dba[0])
                il.add_value('dba_name', com_name[1] if com_name[1] else designer_dba[1])
                il.add_value('person_name', designer)
                il.add_value('total flow','' if 'Not available'  in total else total)
                il.add_value('permit_lic_desc', permit_lic_desc)
                il.add_value('permit_type', 'utility_license')
                yield il.load_item()
        elif response.xpath('//h2/text()').extract_first():
                permit_lic_no=(response.xpath('//h2/text()').extract_first()).split('#')[1]
                main_res=response
                location_address_string=rem_esc(main_res.xpath("//em[contains(text(),'Location')]/following::text()").extract_first())
                plat_lot=rem_esc(''.join(main_res.xpath('//em[contains(text(),"Plat")]/following::text()').extract()[:2]))
                Owner_name=rem_esc(main_res.xpath("//em[contains(text(),'Owner Name')]/following::text()").extract_first())
                corp_owner=rem_esc(main_res.xpath("//em[contains(text(),'Corp Owner')]/following::text()").extract_first())
                designer=rem_esc(main_res.xpath("//em[contains(text(),'Designer')]/following::text()").extract_first())
                total=rem_esc(main_res.xpath("//em[contains(text(),'Total')]/following::text()").extract_first())
                plat=''
                lot=''
                sublot=''
                if plat_lot:
                    if 'Plat' in plat_lot and 'Lot' in plat_lot and 'Sublot' in plat_lot :
                        plat=re.search('Plat.*Lot',plat_lot).group()[4:-3].strip()
                        lot= re.search('Lot.*Sublot',plat_lot).group()[3:-6].strip()
                        sublot=re.search('Sublot.*',plat_lot).group()[6:].strip()
                    elif 'Plat' in plat_lot and 'Lot' in plat_lot :
                        plat=re.search('Plat.*Lot',plat_lot).group()[4:-3].strip()
                        lot= re.search('Lot.*',plat_lot).group()[3:].strip()
                    elif 'Plat' in plat_lot and 'Sublot' in plat_lot :
                        plat=re.search('Plat.*Sublot',plat_lot).group()[4:-6].strip()
                        lot= re.search('Sublot.*',plat_lot).group()[6:].strip()
                    elif 'plat' in plat_lot.lower():
                        plat=re.search('Plat.*',plat_lot).group()[4:].strip()
                il = ItemLoader(item=RiSepticSystemLicensesSpiderItem(),response=response)
                il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                il.add_value('sourceName', 'RI_Septic_System_Licenses')
                il.add_value('url', 'https://www.ri.gov/DEM/isdssearch/')
                il.add_value('permit_lic_no', permit_lic_no)
                il.add_value('city/town', response.meta['city'])
                il.add_value('location_address_string', location_address_string.strip()+", RI")
                il.add_value('plat', (plat.upper().strip())[:-1] if plat.endswith('&') else plat.upper())
                il.add_value('lot', (lot.upper().strip())[:-1] if lot.endswith('&') else lot.upper())
                il.add_value('sublot', (sublot.upper().strip())[:-1] if sublot.endswith('&') else sublot.upper())
                company_name=corp_owner if corp_owner.strip() else Owner_name if Owner_name.strip() else designer if designer.strip() else ''
                com_name=self._getDBA(company_name)
                designer_dba=self._getDBA(designer)

                permit_lic_desc='Septic System Licenses'
                if com_name[0]:
                    permit_lic_desc+=" For "+com_name[0]
                il.add_value('company_name', com_name[0] if com_name[0].strip() else designer_dba[0])
                il.add_value('dba_name',com_name[1] if com_name[1] else designer_dba[1])
                il.add_value('person_name', designer_dba[0])
                il.add_value('total flow','' if 'Not available'  in total else total)
                il.add_value('permit_lic_desc', permit_lic_desc)
                il.add_value('permit_type', 'utility_license')
                yield il.load_item()

        if response.xpath('//span[@class="nextButton"]/a/@href').extract_first():
             yield scrapy.Request(url='https://www.ri.gov/DEM/isdssearch/index.php'+response.xpath('//span/a/@href').extract_first(),  meta={'city':response.meta['city']},callback=self.parse_data)

        elif len(self.city)> self.pos+1:
            self.pos+=1
            print('====pos',self.pos)
            yield scrapy.Request(url=self.url + self.city[self.pos], dont_filter=True, meta={'city':self.city[self.pos]},callback=self.parse_data)
    # @inline_requests
    def save_csv(self,response,main_res,permit_lic_no):
        location_address_string=rem_esc(main_res.xpath("//em[contains(text(),'Location')]/following::text()").extract_first())
        plat_lot=rem_esc(''.join(main_res.xpath('//em[contains(text(),"Plat")]/following::text()').extract()[:2]))
        Owner_name=rem_esc(main_res.xpath("//em[contains(text(),'Owner Name')]/following::text()").extract_first())
        corp_owner=rem_esc(main_res.xpath("//em[contains(text(),'Corp Owner')]/following::text()").extract_first())
        designer=rem_esc(main_res.xpath("//em[contains(text(),'Designer')]/following::text()").extract_first())
        total=rem_esc(main_res.xpath("//em[contains(text(),'Total')]/following::text()").extract_first())
        plat=''
        lot=''
        sublot=''
        if plat_lot:
            if 'Plat' in plat_lot and 'Lot' in plat_lot and 'Sublot' in plat_lot :
                plat=re.search('Plat.*Lot',plat_lot).group()[4:-3].strip()
                lot= re.search('Lot.*Sublot',plat_lot).group()[3:-6].strip()
                sublot=re.search('Sublot.*',plat_lot).group()[6:].strip()
            elif 'Plat' in plat_lot and 'Lot' in plat_lot :
                plat=re.search('Plat.*Lot',plat_lot).group()[4:-3].strip()
                lot= re.search('Lot.*',plat_lot).group()[3:].strip()
            elif 'Plat' in plat_lot and 'Sublot' in plat_lot :
                plat=re.search('Plat.*Sublot',plat_lot).group()[4:-6].strip()
                lot= re.search('Sublot.*',plat_lot).group()[6:].strip()
            elif 'plat' in plat_lot.lower():
                plat=re.search('Plat.*',plat_lot).group()[4:].strip()
        il = ItemLoader(item=RiSepticSystemLicensesSpiderItem(),response=response)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'RI_Septic_System_Licenses')
        il.add_value('url', 'https://www.ri.gov/DEM/isdssearch/')
        il.add_value('permit_lic_no', permit_lic_no)
        il.add_value('city/town', response.meta['city'])
        il.add_value('location_address_string', location_address_string.strip()+", RI")
        il.add_value('plat', (plat.upper().strip())[:-1] if plat.endswith('&') else plat.upper())
        il.add_value('lot', (lot.upper().strip())[:-1] if lot.endswith('&') else lot.upper())
        il.add_value('sublot', (sublot.upper().strip())[:-1] if sublot.endswith('&') else sublot.upper())
        company_name=corp_owner if corp_owner.strip() else Owner_name if Owner_name.strip() else designer if designer.strip() else ''
        com_name=self._getDBA(company_name)
        designer_dba=self._getDBA(designer)
        permit_lic_desc='Septic System Licenses'
        if com_name[0]:
            permit_lic_desc+=" For "+com_name[0]
        il.add_value('company_name', com_name[0] if company[0].strip() else designer[0])
        il.add_value('dba_name', com_name[1] if com_name[1] else designer[1])
        il.add_value('person_name', designer[0])
        il.add_value('total flow','' if 'Not available'  in total else total)
        il.add_value('permit_lic_desc', permit_lic_desc)
        il.add_value('permit_type', 'utility_license')
        yield il.load_item()
