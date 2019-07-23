'''
Created on 17-May-2018

@author: srinivasan
'''
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.select import Select


class Action:
    
    """
    Click element
    """

    def click_on_element(self, driver, element_locator):
        element = driver.find_element(*element_locator)
        element.click()
    
    """
    Check the element is present
    """

    def element_is_present(self, driver, element_locator, waiting_time=2):
        try:
            WebDriverWait(driver, waiting_time).until(
            EC.presence_of_element_located(element_locator))
            return True
        except TimeoutException:
            return False
    
    """
    wait the element is present 
    """

    def wait_for_element_to_be_present(self, driver, element_locator, waiting_time=2):
        try:
            WebDriverWait(driver, waiting_time).until(
            EC.presence_of_element_located(element_locator))
        except TimeoutException:
                raise TimeoutException(
                    'Timeout waiting for {} presense'.format(element_locator[1]))
    
    """
    Wait for clickable element is present
    """

    def  wait_for_element_to_be_clickable(self, driver, element_locator, waiting_time=2):
        try:
            WebDriverWait(driver, waiting_time).until(
            EC.element_to_be_clickable(element_locator))
            self.click_on_element(driver, element_locator)
        except TimeoutException:
            raise TimeoutException(
                'Timeout waiting for {} element to be clickable'.format(element_locator[1]))

    """
    Populate the value in text box
    """

    def populate_text_field(self, driver, element_locator, text):
        input_element = driver.find_element(*element_locator)
        input_element.clear()
        input_element.send_keys(text)

    """
    Scroll the current page
    """

    def scroll(self, driver, scroll_element=None):
        if scroll_element:
            driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight;', scroll_element)
        else:
            driver.execute_script('document.body.scrollTop = document.body.scrollHeight;')
            
    """
    select option values
    """

    def select_option_vales(self, driver, element_locator, text):
        Select(driver.find_element(*element_locator)).select_by_value(text).click()


class SelectValues:
    
    @staticmethod
    def getOptionValues(driver, element_locator):
        return [i.text() for i in Select(driver.find_element(*element_locator)).options()]
        
