'''
Created on 12-May-2018

@author: srinivasan
'''
import datetime
from dateutil.parser import parse
import json
import logging
import os
import re
from scrapy.conf import settings
from scrapy.http.request import Request
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.spiders.crawl import CrawlSpider
from time import strftime, gmtime
from urllib.parse import urljoin

from scrapy.http.request.form import FormRequest
from scrapy.selector.unified import SelectorList, Selector
from twisted.internet.error import TimeoutError, TCPTimedOutError, DNSLookupError

from Data_scuff.extensions.exception.seleniumexception import SeleniumExtensionsException
from Data_scuff.extensions.solver.captchacoder import CaptchaCoderApi
from Data_scuff.selenium.action import Action, SelectValues
from Data_scuff.selenium.driver import Driver
from Data_scuff.utils.fileutils import FileUtils
from Data_scuff.utils.utils import Utils
import pandas as pd

logger = logging.getLogger(__name__)


class CustomSettings:
    
    @staticmethod
    def appenAndgetItemPipelinevalues(custom_dict):
        itemPipe = settings.getdict('ITEM_PIPELINES')
        itemPipe.update(custom_dict)
        return itemPipe
    
    @staticmethod
    def appenDownloadMiddlewarevalues(custom_dict):
        download = settings.getdict('DOWNLOADER_MIDDLEWARES')
        download.update(custom_dict)
        return download
    
    @staticmethod
    def appendSpiderDownloadMiddlewarevalues(custom_dict):
        download = settings.getdict('SPIDER_MIDDLEWARES')
        download.update(custom_dict)
        return download
    
    @staticmethod
    def getCustom_settings_WithHeader(custom_settings):
        req_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                     settings['NEWSPIDER_MODULE'].split(".")[1],
                     custom_settings['JIRA_ID'])
        logger.info("spider Path: %s", req_dir)
        import glob
        os.chdir(req_dir)
        if glob.glob("*.xlsx") or glob.glob("*.xls"):
            headers = Utils.getExcelHeaders(os.path.join(req_dir, glob.glob("*.xlsx")[0]))
            custom_settings['TOP_HEADER'] = headers['top_header']
            custom_settings['FIELDS_TO_EXPORT'] = headers['feed_expo']
            custom_settings['NULL_HEADERS'] = headers['null_header']
            return custom_settings
        else:
            raise Exception('rquirement Excel File is missing in path: {}'.format(req_dir))
    
    @staticmethod
    def getJobDirectory(foldernm):
        job_dir = settings.get('JOB_DIR_PAUSE_RESUME',
                                os.path.join(settings.get('BASE_DIR', None), "jobDir"))
        return os.path.join(job_dir, foldernm)


class LookupDatareaderMixin:
    
    """
    Read and convert the CSV file into dictionary
    """

    def readcsv(self, path, sep=','):
        return self.__convertKeyValuePairtoDictionary(
            pd.read_csv(path, sep=sep))
    
    """
    Read and convert the EXCEL file into dictionary
    """

    def readExcel(self, path):
        return self.__convertKeyValuePairtoDictionary(
            pd.read_excel(path))

    """
    Read and convert the JSON file into dictionary
    """

    def readJson(self, path):
        return  self.__convertKeyValuePairtoDictionary(
            pd.read_json(path))

    """
    Read and convert the TABLE file into dictionary
    """

    def readtable(self, url, index_pos=0):
        return  self.__convertKeyValuePairtoDictionary(
            pd.read_html(url)[index_pos])
    
    def __convertKeyValuePairtoDictionary(self, df):
        return dict(zip(df.ix[:, 0], df.ix[:, 1]))


class DataFormatterMixin:
    """
    Format the address based on the three argument
    """

    def format_address(self, address, city, _zip):

        return ", ".join([y.strip() for y in 
            [address, " ".join([i for i in 
                                [city, _zip] if i])] if y])
    
    """
    Format the address based on the four argument
    """

    def format__address_4(self, address, city, state, _zip):
        return ", ".join([y.strip() for y in 
            [address, city, " ".join([i for i in 
                                [state, _zip] if i])] if y])
    
    """
    Format the address based on the four argument
    """

    def format__address(self, address1, address2, city, _zip):
        return ", ".join([ y.strip() for y in [address1, address2,
                 " ".join([i.strip() for i in [city, _zip] if i])] if y])
    
    """
    Format the address
    """

    def format__address_5(self, address1, address2, city, state, _zip):
        return ", ".join([ y.strip() for y in [address1, address2, city,
                 " ".join([i.strip() for i in [state, _zip] if i])] if y])
    
    """
    join values based on the join keys
    """

    def join_string(self, join_key=" ", *args):
        return join_key.join(*args)
    
    def format__address5(self, address1, address2, city, state, _zip):
        return ", ".join([ y.strip() for y in [address1, address2, city,
                 " ".join([i.strip() for i in [state, _zip] if i])] if y])
    
    """
    combine first name and last name with prefix
    """

    def format_name(self, **kwargs):
        name = ''
        if "fullname" in kwargs and kwargs.get('fullname'):
            name = ".".join([kwargs.get('prefix', ''),
                             kwargs.get('fullname').strip()])
        else:
            val = [kwargs.get('prefix', ''),
                              " ".join([v.strip() for v in [kwargs.get('firstname', ''),
                                                    kwargs.get('lastname', '')] if v])]
            if len(val) >= 2:
                name = ".".join(val)
        return name
    
    def format_date(self, date):
        if date and len(str(date)) > 6:
            try:
                dt = parse(str(date))
                date = dt.strftime('%m/%d/%Y')
            except:
                return date
        else:
            date = ''

        return date

    def state_list(self, varr):
        listt = [' WA ',' OR ',' CA ',' NV ',' ID ',' MT ',' UT ',' AZ ',' NM ',' CO ',' WY ',' ND ',' SD ',' NE ',' KS ',' OK ',' TX ',' MN ',' IA ',' MO ',' AR ',' LA ',' WI ',' IL ',' MS ',' AL ',' TN ',' KY ',' OH ',' MI ',' IN ',' GA ',' FL ',' SC ',' NC ',' VA ',' DC ',' PA ',' NY ',' VT ',' ME ',' NH ',' MA ',' RI ',' CT ',' NJ ',' DE ',' MD ',' WV ',' HI ',' AK ', ',WA ',',OR ',',CA ',',NV ',',ID ',',MT ',',UT ',',AZ ',',NM ',',CO ',',WY ',',ND ',',SD ',',NE ',',KS ',',OK ',',TX ',',MN ',',IA ',',MO ',',AR ',',LA ',',WI ',',IL ',',MS ',',AL ',',TN ',',KY ',',OH ',',MI ',',IN ',',GA ',',FL ',',SC ',',NC ',',VA ',',DC ',',PA ',',NY ',',VT ',',ME ',',NH ',',MA ',',RI ',',CT ',',NJ ',',DE ',',MD ',',WV ',',HI ',',AK ', ' WA,',' OR,',' CA,',' NV,',' ID,',' MT,',' UT,',' AZ,',' NM,',' CO,',' WY,',' ND,',' SD,',' NE,',' KS,',' OK,',' TX,',' MN,',' IA,',' MO,',' AR,',' LA,',' WI,',' IL,',' MS,',' AL,',' TN,',' KY,',' OH,',' MI,',' IN,',' GA,',' FL,',' SC,',' NC,',' VA,',' DC,',' PA,',' NY,',' VT,',' ME,',' NH,',' MA,',' RI,',' CT,',' NJ,',' DE,',' MD,',' WV,',' HI,',' AK,',   ',WA,',',OR,',',CA,',',NV,',',ID,',',MT,',',UT,',',AZ,',',NM,',',CO,',',WY,',',ND,',',SD,',',NE,',',KS,',',OK,',',TX,',',MN,',',IA,',',MO,',',AR,',',LA,',',WI,',',IL,',',MS,',',AL,',',TN,',',KY,',',OH,',',MI,',',IN,',',GA,',',FL,',',SC,',',NC,',',VA,',',DC,',',PA,',',NY,',',VT,',',ME,',',NH,',',MA,',',RI,',',CT,',',NJ,',',DE,',',MD,',',WV,',',HI,',',AK,']

        for i in listt:
            if i in varr:
                return True

        return False

    def _getDBA(self, person_name):
        if person_name:
            person_name = re.sub(r' Dba | DBA |D/B/A|d/b/a| DBA|DBA | dba | d b a |DBA:|dba:| dba |doing business|doingbusinessas|doing business as|doingas| A K A | A K A | AKA | also known as', ' dba ', person_name, flags=re.IGNORECASE)
            if re.search(' dba ', person_name, flags=re.IGNORECASE):
                name = person_name.split('dba')[0]
                dba_name = person_name.split('dba')[1]
                return name, dba_name
        return (person_name, '')
    
    def _getLicense_mapping(self, permit_value, default_value=''):
        key = permit_value.lower()
        map_dict = {'accountants':'accountancy_license', 'accounting firms': 'accountancy_license', 'cpas':'accountancy_license', 'accountancy':'accountancy_license', 'tax preparer':'accountancy_license', 'acupuncture professional':'acupuncture_license', 'real estate/home appraiser':'appraiser_license', 'real estate':'appraiser_license', 'home appraiser':'appraiser_license', 'real estate appraisers':'appraiser_license', 'architects':'architecture_license', 'architecture firms':'architecture_license', 'architect & interior designer':'architecture_license',  'interior license':'architecture_license', 'design firm':'architecture_license', 'asbestos related':'asbestos_contractor_license', 'athletic trainers':'athletic_trainer_license', 'auction house':'auction_license',  'auctioneer':'auction_license', 'audiology':'audiology_pathology_license', 'pathology':'audiology_pathology_license', 'hearing aid dispenser':'audiology_pathology_license', 'speech pathology and audiology':'audiology_pathology_license', 'hearing aid dealers':'audiology_pathology_license', 'speech-language pathologist':'audiology_pathology_license', 'secondhand dealer auto':'auto_dealer_license', 'secondhand dealer general':'auto_dealer_license', 'automotive repair':'auto_repair_license', 'barbers':'barber_license', 'barber instructors':'barber_license', 'barber schools':'barber_license', 'general/building contractors':'building_contractor_license', 'building contractors':'building_contractor_license', 'general':'building_contractor_license', 'residential/general contractors':'building_contractor_license', 'residential':'building_contractor_license', 'general contractors':'building_contractor_license', 'building related permits':'building_permit', 'car Wash':'business_license', 'commercial lessor':'business_license', 'dealer in products for the disabled':'business_license', 'process server individual':'business_license', 'process serving agency':'business_license', 'cemetery':'cemetery_funeral_license', 'funeral home':'cemetery_funeral_license', 'embalmer':'cemetery_funeral_license', 'chemical dependency professional':'chemical_license', 'child care centers':'child_care_license', 'chiropractor':'chiropractor_license', 'chiropractic examiner':'chiropractor_license', 'chiropractors':'chiropractor_license', 'naprapathy':'chiropractor_license', 'electronic cigarette retail dealer':'cigarette_tobacco_license', 'tobacco retail dealer':'cigarette_tobacco_license', 'clinical laboratory - technologists':'clinical_license', 'technicians':'clinical_license', 'restricted':'clinical_license', 'cytotechnologist':'clinical_license', 'histological technician':'clinical_license', 'home improvement contractor':'contractor_license', 'home improvement salesperson':'contractor_license', 'residential builders':'contractor_license', 'lead':'contractor_license', 'handyman':'contractor_license', 'facilities':'controlled_substance_license', 'professionals that handle controlled substance':'controlled_substance_license', 'cs precursor':'controlled_substance_license', 'cosmetologists':'cosmetology_license', 'hairstylists':'cosmetology_license', 'cosmetology':'cosmetology_license', 'behavior analyst':'counselor_license', 'therapist':'counselor_license', 'mental health practituoners':'counselor_license', 'occupational therapy':'counselor_license', 'psychology':'counselor_license', 'counselors':'counselor_license', 'psychology':'counselor_license', 'substance use disorder':'counselor_license', 'court reporters':'court_reporter_license', 'debt collection agency':'debt_collector_license', 'lien recovery fund member':'debt_collector_license', 'dentists':'dental_license', 'dentistry':'dental_license', 'dental assistants':'dental_technician_license', 'dental technicians':'dental_technician_license', 'dental hygeinists':'dental_technician_license', 'dietician':'dietician_license', 'schools':'educational_license', 'postsecondary schools':'educational_license', 'electrical contractors':'electrical_contractor_license', 'electrician':'electrical_contractor_license', 'electronic & home appliance service dealer':'electronics_license', 'electronics store':'electronics_license', 'electronics store':'electronics_license', 'electronic & home appliance service dealer':'electronics_license', 'elevator contractors':'elevator_contractor_license', 'elevator mechanic':'elevator_contractor_license', 'engineers':'engineer_surveyor_license', 'surveyors':'engineer_surveyor_license', 'engineers and land surveyors':'engineer_surveyor_license', 'professional engineers':'engineer_surveyor_license', 'professional surveyors':'engineer_surveyor_license', 'environmental health practitioner':'environmental_license', 'environmental health scientist':'environmental_license', 'financial sector':'financial_license', 'sidewalk cafe':'food_vendor_license', 'temporary street fair vendor permit':'food_vendor_license', 'foresters':'forester_license', 'amusement arcade':'gaming_license', 'amusement device - permanent':'gaming_license', 'amusement device - portable':'gaming_license', 'Amusement device - temporary':'gaming_license', 'bingo game operator':'gaming_license', 'games of chance':'gaming_license', 'gaming cafe':'gaming_license', 'pool or billiard room':'gaming_license', 'carnival/amusement':'gaming_license', 'amusement':'gaming_license', 'carnival':'gaming_license', 'garage':'garage_license', 'garage and parking lot':'garage_license', 'parking lot':'garage_license', 'geologists':'geology_license', 'sightseeing guide':'guide_license', 'hunting':'hunting_license', 'hunters':'hunting_license', 'hunting guides/outfitters':'hunting_license', 'hunting guides':'hunting_license', 'outfitters':'hunting_license', 'heating':'hvac_contractor_license', 'ventilation and air conditining contractors':'hvac_contractor_license', 'building inspector':'inspector_license', 'contract work inspector':'inspector_license', 'plumbing inspector':'inspector_license', 'electrical inspector':'inspector_license', 'insurance agents':'insurance_license', 'brokers':'insurance_license', 'agencies':'insurance_license', 'landscape contractor':'landscape_license', 'landscape architects':'landscape_license', 'industrial laundry':'laundry_license', 'industrial laundry delivery':'laundry_license', 'retail laundry':'laundry_license', 'lawyers':'lawyer_license', 'para legal':'legal_license', 'locksmith':'locksmith_license', 'locksmith apprentice':'locksmith_license', 'hotels':'lodging_license', 'motels':'lodging_license', 'manufactured home':'manufactured_home_license', 'factory built housing':'manufactured_home_license', 'marijuana related facilities':'marijuana_license', 'doctors':'medical_license', 'physicians':'medical_license', 'specialists':'medical_license', 'perfusion':'medical_license', 'corporate health professionals':'medical_license','mortuary science':'medical_license', 'osteopathic medicine & surgery':'medical_license', 'respiratory care':'medical_license', 'health systems facilities':'medical_license', 'medication aide - certified':'medical_license', 'naturopathic':'medical_license', 'osteopathic physician':'medical_license', 'radiology':'medical_license', 'pathologists':'medical_license', 'medical technician':'medical_technician_license', 'medical assistant':'medical_technician_license', 'physicians assistants':'medical_technician_license', 'nursing homes':'nursing_home_license', 'nursing home administrators':'nursing_home_license', 'nursing related':'nursing_license', 'nursing':'nursing_license', 'nurse aides':'nursing_license', 'direct-entry midwife':'nursing_license', 'opticians':'optician_license', 'optometrist':'optometry_license', 'optometry technician':'optometry_license', 'optometry':'optometry_license', 'professional service corp':'pes_license', 'personnel agency':'pes_license', 'professional employer org':'pes_license', 'pest control':'pest_control_license', 'termite control':'pest_control_license', 'fumigation':'pest_control_license', 'pharmacists':'pharmacy_license', 'pharmacy':'pharmacy_license', 'plumbing contractors':'plumbing_contractor_license', 'podiatrist':'podiatry_license', 'podiatry':'podiatry_license', 'podiatric medicine & surgery':'podiatry_license', 'real estate':'real_estate_license', 'general vendor':'retail_license', 'general vendor distributor':'retail_license', 'watch guard':'security_license', 'patrol agencies':'security_license', 'armored car carrier':'security_license', 'armored guard':'security_license', 'private detective and Sec. Agency':'security_license', 'investment/securities':'security_license', 'investment':'security_license', 'securities':'security_license', 'professional investigator':'security_license', 'security alarm':'security_license', 'security alarm systems':'security_license', 'security guard':'security_license', 'unarmed combat':'security_license', 'adoption agency':'social_service_license', 'foster care agency':'social_service_license', 'residential crisis nursaries':'social_service_license', 'adult care':'social_service_license', 'behavioral support home':'social_service_license', 'group home':'social_service_license', 'social work':'social_work_license', 'prof.coun./soc. Work':'social_work_license', 'prof.coun.':'social_work_license', 'soc. Work':'social_work_license', 'marriage and family therapy':'social_work_license', 'social workers':'social_work_license', 'pawnbroker':'specialty_business_license', 'pedicab business':'specialty_business_license', 'aquaculture':'specialty_business_license', 'fish propagators':'specialty_business_license', 'scale dealer/repairer':'specialty_business_license', 'scale dealer':'specialty_business_license', 'repairer':'specialty_business_license', 'scrap metal processor':'specialty_business_license', 'special sale':'specialty_business_license', 'going out of business':'specialty_business_license', 'liquidation':'specialty_business_license', 'ticket seller business':'specialty_business_license', 'ticket seller individual':'specialty_business_license', 'tele marketer':'specialty_business_license', 'sanitary license':'specialty_business_license', 'ski areas':'specialty_business_license', 'sanitarians':'specialty_business_license', 'body art':'specialty_business_license', 'furniture retailer':'specialty_business_license', 'dealer':'specialty_business_license', 'professional Fiduciary':'specialty_license', 'animal control operators':'specialty_license','athlete agents':'specialty_license', 'community association manager':'specialty_license', 'sex offender provider':'specialty_license', 'detection of deception trainee':'specialty_license', 'trainer':'specialty_license', 'examiner':'specialty_license', 'utility foreman':'specialty_license', 'librarian':'specialty_license', 'lactation consultants':'specialty_license', 'collection practices':'specialty_license', 'polygraph examiners':'specialty_license', 'deception detection':'specialty_license', 'medical language interpreter':'specialty_license', 'sign language interpreters':'specialty_license', 'online internet facilitator':'specialty_license', 'private probation provider':'specialty_license', 'utility construction':'specialty_license', 'booting company':'specialty_transportation_license', 'horse drawn cab driver':'specialty_transportation_license', 'horse drawn cab owner':'specialty_transportation_license', 'pedicab driver':'specialty_transportation_license', 'household carriers':'specialty_transportation_license', 'employment agency':'staffing_license', 'storage warehouse':'storage_license', 'underground storage tanks':'storage_license', 'teachers':'teacher_license', 'physical therapy/ massage therapy/Occupational Thrapy':'therapy_license', 'massage therapy':'therapy_license', 'massage therapist':'therapy_license', 'occupational therapists':'therapy_license', 'physical therapy':'therapy_license', 'tow truck company':'tow_license', 'tow truck driver':'tow_license', 'tow truck exemption':'tow_license', 'sightseeing bus':'transportation_license', 'transportation company':'transportation_license', 'tree experts':'tree_care_license', 'water/ wastewater board':'utility_license', 'water':'utility_license', 'wastewater board':'utility_license', 'vehicle inspection stations':'Vehicle_Inspection_license', 'vehicle repair shops':'Vehilcle_Repair_License', 'newsstand':'vendor_license','stoop line stand':'vendor_license', 'veterinarian':'veterinary_license', 'veterinary technician':'veterinary_license', 'veterinary medicine':'veterinary_license', 'waste facilities storage and processing':'waste_facility_license', 'medical waste producers':'waste_facility_license', 'waste':'waste_transporter_license', 'kitchen grease transporters':'waste_transporter_license'}
        return map_dict.get(key, str(default_value))


class CommonSpider(CrawlSpider, CustomSettings, LookupDatareaderMixin, DataFormatterMixin):

    def __init__(self, start=None, end=None, proxyserver=None, *a, **kw):
        
        super(CommonSpider, self).__init__(*a, **kw)
        self.start = start
        self.end = end
        self.proxyserver = proxyserver
    
    name = "commonSpider"
    URL = None
    form_url = None
    allowed_domains = []
    driverObj = None
    blocked_domains = []
     
    """
    get the current ingestion time
    """

    def getingestion_timestamp(self):
        return strftime("%Y-%m-%d_%H%M%S", gmtime())
    
    """
    get the selenium dirver
    """

    def getSeleniumDriver(self, drivertype=None, executable_path=None, run_headless=False,
                  load_images=True, proxy_string=None, **kwargs):
        download_path = os.path.join(self.settings.get('SELENIUM_DOWNLOAD_PATH', '/tmp'), self.name)
        if not FileUtils.isExist(download_path):
            FileUtils.createDir(download_path)
        kwargs['download_path'] = download_path
        self.driverObj = Driver(drivertype)
        driver = None
        try:
            driver = self.driverObj.getDriver(executable_path, run_headless,
                                                   load_images, proxy_string, **kwargs)
        except Exception as e:
            raise SeleniumExtensionsException("problem to get selenium driver-{}", e)
        return driver
    
    """
    send form request
    """

    def form_request(self, form_data, callback):
        req = FormRequest(url=self.form_url, method='POST', formdata=form_data, dont_filter=True,
                          callback=callback, errback=self.handle_form_error)
        return req

    """
    kill the driver class
    """

    def killDriver(self):
        if self.driverObj:
            self.driverObj.shutdownDriver()
    
    """
    get Action
    """

    def getAction(self):
        action = Action()
        return action

    """
    get all select values
    """

    def getSelectValues(self, driver, element_locator):
        return SelectValues.getOptionValues(driver, element_locator)

    """
    handle the form error
    """

    def handle_form_error(self, failure):
        # log all failures
        self.logger.error(repr(failure))

        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)
    
    """
    Return the raw content
    """

    def _raw_content(self, response):
        return response.body_as_unicode()
    
    """
    read the json response to string
    """

    def jsonResponse(self, response):
        return json.loads(response.body_as_unicode())

    """
    Handle 302 redirect
    """

    def dontRedirectResponse(self, url, callback):
        return Request(url, meta={
                  'dont_redirect': True,
                  'handle_httpstatus_list': [302]
              }, callback=callback)

    """
    Clean the Html Content
    """

    def cleanHtml(self, rawHtml):
        patterns = [re.compile("<head>.*?</head>"), re.compile("<footer>.*?</footer>")]
        for i in patterns:
            rawHtml = i.sub("", rawHtml)
        return rawHtml
    
    """
    Join the prefix in all domain urls
    """

    def url_join_list(self, prefix, urls):
        urls = urls if isinstance(urls, list) else [urls]
        return [ urljoin(prefix, l) for l in urls if l]
    
    """
    Extract Data From html
    """

    def extractData(self, body, xpath):
        if isinstance(body, str):
            return Selector(text=body).xpath(xpath).extract()
        return Selector(response=body).xpath(xpath).extract()
    
    """
    get all the selector based on the xpathvalues
    """

    def xpath(self, selector, xpathValue):
        if isinstance(xpathValue, dict):
            operator = xpathValue['operator']
            if operator == 'and':
                return self.__clean_selector_null([selector.xpath(exp) for exp in 
                                                   xpathValue['value'] if selector.xpath(exp)])
            elif operator == 'or':
                return self.__xpath_or(selector, xpathValue['value'])
        elif isinstance(xpathValue, list):
            return self.__xpath_or(selector, xpathValue)
        else:
            return selector.xpath(xpathValue)
        return []
    
    """
    Clean select if it's null Element
    """

    def __clean_selector_null(self, selList):
        return SelectorList([ item for item in selList 
                             if len(item.extract()) > 0])

    def __xpath_or(self, selector, xpathList):
        for exp in xpathList:
            result = self.__clean_selector_null(selector.xpath(exp))
            if result:
                return result
    
    def getcaptchaCoder(self, site_key=None):
        kwargs = {}
        if site_key:
            kwargs['site_key'] = site_key
        return CaptchaCoderApi(self.settings.get('CAPTCHA_CODER_BASE_URL'),
                               self.settings.get('CAPTCHA_CODER_API_KEY'), **kwargs)
        