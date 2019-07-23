'''
Created on 16-Jun-2018

@author: srinivasan
'''
import time

from Data_scuff.selenium.driver import DriverType
from Data_scuff.spiders.__common import CommonSpider


class NCHDInspectionSpider(CommonSpider):
    
    name = 'NCHDInspectionSpider'
    allowed_domains = ['healthspace.com']
    start_urls = ['http://healthspace.com/clients/colorado/nchd/web.nsf/module_facilities.xsp?module=Food']
    
    def parse(self, response):
        self.logger.info("-------------------ulr {}".format(response.url))
        driver = self.getSeleniumDriver(DriverType.CHROME, run_headless=False, load_images=False)
        driver.get(response.url) 
        elems = driver.find_elements_by_xpath('//form/div[3]/div/div[2]/div/div/div[2]/div/div/div/table/tbody/tr')
        for elsem in elems:
            elsem.click() 
            time.sleep(20)
            driver.back()
        driver.close()
    
