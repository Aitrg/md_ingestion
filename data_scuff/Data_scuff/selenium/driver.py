'''
Created on 17-May-2018

@author: srinivasan
'''
from abc import ABC, abstractmethod
import enum
import logging
import os
import re

from pyvirtualdisplay.display import Display
from selenium import webdriver

from Data_scuff import settings
from Data_scuff.utils import utils
from Data_scuff.config import getDriverPath

logger = logging.getLogger(__name__)


class DriverType(enum.Enum):
    
    FIREFOX = "firefox"
    CHROME = "chrome"


class Driver:
    
    def __init__(self, drivertype):
        self.drivertype = drivertype
        self.driver = None
    
    """
    get the newly created web driver object
    """

    def getDriver(self, executable_path=None, run_headless=False,
                  load_images=True, proxy_string=None, **kwargs):
        if not executable_path:
            import platform
            if platform.system().lower() in 'windows':
                if  self.drivertype == DriverType.FIREFOX.name:
                    executable_path = getDriverPath('Firefox-Webdriver-exe', 'geckodriver.exe')
                else:
                    executable_path = getDriverPath('Chrome-Webdriver-exe', 'chromedriver.exe')
                if not executable_path:
                    executable_path = settings.DRIVER_EXECUTABLE_PATH
                if not executable_path:
                    raise Exception("Please set the execute able path in Windows Machine")
        else:
            if  self.drivertype == DriverType.FIREFOX.name:
                pass
            else:
                pass
        if self.drivertype == DriverType.FIREFOX.name:
            self.driver = self._FireFoxDriver().getDriver(executable_path, run_headless, load_images, proxy_string, **kwargs)
        else:
            self.driver = self._ChromeDriver().getDriver(executable_path, run_headless, load_images, proxy_string, **kwargs)
        return self.driver
    
    """
    Shutdown the web driver object
    """

    def shutdownDriver(self):
        if self.driver:
            self.driver.quit()
        try:
            """
            kill the virtual display
            """
            self.driver.display.sendstop()
        except:
            pass

    """
    Abstract Driver class
    """
    
    class _AbstractDriver(ABC):
        
        @abstractmethod
        def getDriver(self, executable_path=None, run_headless=False,
                      load_images=True, proxy_string=None, **kwargs):
            pass
        
        """
        Validate the Proxy url information
        """
    
        def _validate_proxy_string(self, proxy_string):
            if proxy_string:
                val_ip = re.match('^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$', proxy_string)
                if not val_ip:
                    if proxy_string.startswith('http://'):
                        proxy_string = proxy_string.split('http://')[1]
                    elif proxy_string.startswith('https://'):
                        proxy_string = proxy_string.split('https://')[1]
                    elif '://' in proxy_string:
                        proxy_string = proxy_string.split('://')[1]
                    chunks = proxy_string.split(':')
                    if len(chunks) == 2:
                        if re.match('^\d+$', chunks[1]):
                            if utils.Utils.is_valid_url('http://' + proxy_string):
                                valid = True
                else:
                    proxy_string = val_ip.group()
                    valid = True
                if not valid:
                    logger.info("not a valid proxy url")
                    proxy_string = None
    
            return proxy_string
    
    class _ChromeDriver(_AbstractDriver):
        
        """
        get the chrome driver
        """
    
        def getDriver(self, executable_path=None, run_headless=False,
                      load_images=True, proxy_string=None, **kwargs):
            chrome_options = self.__getChromeOptions(
                self._validate_proxy_string(proxy_string),
                kwargs.get('download_path', '/tmp'))
            if run_headless:
                chrome_options.add_argument('headless')
            if not load_images:
                prefs = {'profile.managed_default_content_settings.images': 2}
                chrome_options.add_experimental_option('prefs', prefs)
            if executable_path:
                driver = webdriver.Chrome(chrome_options=chrome_options,
                                          executable_path=executable_path)
            else:
                driver = webdriver.Chrome(chrome_options=chrome_options)
            return driver
        
        """
        get the chrome options
        """
    
        def __getChromeOptions(self, proxy_string, downloads_path='/tmp'):
            chrome_options = webdriver.ChromeOptions()
            prefs = {
                "download.default_directory": downloads_path,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "credentials_enable_service": False,
                "profile": {
                    "password_manager_enabled": False
                }
            }
            chrome_options.add_experimental_option("prefs", prefs)
            chrome_options.add_argument("--test-type")
            chrome_options.add_argument("--no-first-run")
            chrome_options.add_argument("--ignore-certificate-errors")
            chrome_options.add_argument("--allow-file-access-from-files")
            chrome_options.add_argument("--allow-insecure-localhost")
            chrome_options.add_argument("--allow-running-insecure-content")
            chrome_options.add_argument("--disable-infobars")
            chrome_options.add_argument("--disable-save-password-bubble")
            chrome_options.add_argument("--disable-single-click-autofill")
            chrome_options.add_argument("--disable-translate")
            chrome_options.add_argument("--disable-web-security")
            if proxy_string:
                chrome_options.add_argument('--proxy-server=%s' % proxy_string)
            if settings.START_CHROME_IN_FULL_SCREEN_MODE:
                chrome_options.add_argument("--start-maximized")
                chrome_options.add_argument("--kiosk")
            return chrome_options
    
    class _FireFoxDriver(_AbstractDriver):
        
        def getDriver(self, executable_path=None, run_headless=False,
            load_images=True, proxy_string=None, **kwargs):
            firefox_profile = self.__getFireFoxOptions(
                self._validate_proxy_string(proxy_string))
            if run_headless:
                display = Display(visible=0, size=(1024, 768))
                display.start()
            else:
                display = None
            if not load_images:
                firefox_profile.add_extension(os.path.dirname(
                    os.path.realpath(__file__)) + 
                    '/disableimageload/quickjava-2.1.2-fx.xpi')
                firefox_profile.set_preference(
                    'thatoneguydotnet.QuickJava.curVersion', '2.1.2.1')
                firefox_profile.set_preference(
                    'thatoneguydotnet.QuickJava.startupStatus.Images', 2)
                firefox_profile.set_preference(
                    'thatoneguydotnet.QuickJava.startupStatus.AnimatedImage', 2)
            if executable_path:
                driver = webdriver.Firefox(
                    firefox_profile, executable_path=executable_path)
            else:
                driver = webdriver.Firefox(firefox_profile)
            driver.display = display
            return driver
    
        """
        get the FireFox options
        """
    
        def __getFireFoxOptions(self, proxy_string, downloads_path='/tmp'):
            profile = webdriver.FirefoxProfile()
            profile.set_preference("reader.parse-on-load.enabled", False)
            profile.set_preference("pdfjs.disabled", True)
            if proxy_string:
                proxy_server = proxy_string.split(':')[0]
                proxy_port = proxy_string.split(':')[1]
                profile.set_preference("network.proxy.type", 1)
                profile.set_preference("network.proxy.http", proxy_server)
                profile.set_preference("network.proxy.http_port", int(proxy_port))
                profile.set_preference("network.proxy.ssl", proxy_server)
                profile.set_preference("network.proxy.ssl_port", int(proxy_port))
            profile.set_preference(
                "security.mixed_content.block_active_content", False)
            profile.set_preference(
                "browser.download.manager.showAlertOnComplete", False)
            profile.set_preference("browser.download.panel.shown", False)
            profile.set_preference(
                "browser.download.animateNotifications", False)
            profile.set_preference("browser.download.dir", downloads_path)
            profile.set_preference("browser.download.folderList", 2)
            profile.set_preference(
                "browser.helperApps.neverAsk.saveToDisk",
                ("application/pdf, application/zip, application/octet-stream, "
                 "text/csv, text/xml, application/xml, text/plain, "
                 "text/octet-stream, "
                 "application/"
                 "vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
            return profile
