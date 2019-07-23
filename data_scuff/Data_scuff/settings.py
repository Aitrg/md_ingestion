# -*- coding: utf-8 -*-
import os
from os.path import abspath, join, dirname

from Data_scuff.config import getUserAgentList

# Scrapy settings for Data_scuff project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html
BOT_NAME = 'Data_scuff'
BASE_DIR = os.path.dirname(os.path.dirname(__file__))

# SCHEDULER = 'Data_scuff.core.db_schedulers.Scheduler'

SPIDER_MODULES = ['Data_scuff.spiders']
NEWSPIDER_MODULE = 'Data_scuff.spiders'

# scheduler
DEPTH_PRIORITY = 1
SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.FifoMemoryQueue'
DUPEFILTER_CLASS = "Data_scuff.extensions.filter.BLOOMRFPDupeFilter"

# chrome screen status
START_CHROME_IN_FULL_SCREEN_MODE = False
DRIVER_EXECUTABLE_PATH = '/usr/local/bin/chromedriver'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# temp file path
TEMP_FILE_PATH = None

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 10

COMMANDS_MODULE = 'Data_scuff.commands'
# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3

# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 10
CONCURRENT_REQUESTS_PER_IP = 10

# Disable cookies (enabled by default)
COOKIES_ENABLED = False
# RANDOM_PROXY_DISABLED = True

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
   'Accept-Language': 'en',
   'X-JAVASCRIPT-ENABLED': 'True' 
 }

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    'Data_scuff.middleware.spider.optional.AddExtraColumnsMiddleware':1,
    'Data_scuff.middleware.spider.optional.RecrawlMiddleware':2,
    'Data_scuff.middleware.spider.common.CollectFailedUrlMiddleWare': 543,
    'Data_scuff.middleware.spider.common.AddparameterProxyMiddleWare':1000
}

# Retry many times since proxies often fail
RETRY_TIMES = 10

# Retry on most error codes since proxies fail for different reasons
RETRY_HTTP_CODES = [500, 503, 504, 400, 403, 404, 408]

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware' : None,
    'Data_scuff.middleware.captcha.CaptchaMiddleware':99,
    'Data_scuff.middleware.common.BlockDomainMiddleWare': 11,
    'Data_scuff.middleware.rotateuseragent.RotateUserAgentMiddleware': 998,
    'Data_scuff.middleware.common.DelayedRequestMiddleware':999,
    'Data_scuff.middleware.requestTracking.RequestResponseTracking':1000,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
    'Data_scuff.middleware.radomproxy.RandomProxy': 350,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 400
}

DOWNLOAD_HANDLERS = {
    'http': 'Data_scuff.extensions.http.CustomHttpDownloadHandler',
    'https': 'Data_scuff.extensions.http.CustomHttpDownloadHandler'
}

REACTOR_THREADPOOL_MAXSIZE = 30

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    # 'Data_scuff.extensions.ext.perodicstatsSender.PerodicStatsSender': 500,
    # 'Data_scuff.extensions.ext.statsmail.StatsMailSend':1000,
    'Data_scuff.extensions.ext.stats.ColumnStatsExtension':10,
    'Data_scuff.extensions.common.SpiderOpenCloseLogging':1,
    'Data_scuff.extensions.common.ClearDownloadPath':500,
    'Data_scuff.extensions.ext.stats.SaveStatisticsinJobDirectory':999,
    'Data_scuff.extensions.ext.error.CollectSpiderError':2
 }

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html

ITEM_PIPELINES = {
    'Data_scuff.pipeline.data.DataCorrectionPipeline':1,
    'Data_scuff.pipeline.data.DataReplacePipeline':2,
    'Data_scuff.pipeline.data.AddCommonColumnPipeline':3,
    'Data_scuff.pipeline.data.DataValidatorPipeline':4,
    'Data_scuff.pipeline.common.DropDuplicateItemPipeline': 998,
    'Data_scuff.pipeline.file.ChunkWiseCsvFileWriter': 999,
    # 'Data_scuff.pipeline.statsmailsend.StatsMailSendPipeline':1000,
 }

# chunk file size is 100MB
CHUNK_FILE_SIZE = 100000000

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 2
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 100
# Enable showing throttling stats for every response received:
AUTOTHROTTLE_DEBUG = False

# viswa disabled 18AUG18
# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 0
HTTPCACHE_DIR = os.path.join(BASE_DIR, 'cache', 'httpcache')
HTTPCACHE_IGNORE_HTTP_CODES = []
HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
HTTPCACHE_GZIP = True

# Error codes
ERROR_CODES = [400, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 426, 428]

# LOG_LEVEL = 'ERROR'
# LOG_FILE = os.path.join(BASE_DIR, "logs", "log.txt")

CSV_DELIMITER = '|'

# Final File Stored Directory
STORAGE_DIR = os.path.join(BASE_DIR, "storage")

# Cache Directory
CACHE_DIR = os.path.join(BASE_DIR, "cache")

# list domain to be blocked
BLOCK_DOMAINS = []

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT_CHOICES = getUserAgentList()

# Custom Template directory
CUSTOM_TEMPLATES_DIR = abspath(join(dirname(__file__), 'templates'))

# proxy
RANDOM_PROXY_DISABLED = False
PROXY_LIST = abspath(join(dirname(__file__), 'configs', 'proxy'))

# Proxy mode
# 0 = Every requests have different proxy
# 1 = Take only one proxy from the list and assign it to every requests
PROXY_MODE = 0

# SMTP CONFIGURATION
MAIL_HOST = 'host.aitrg.com'
MAIL_PORT = 465
MAIL_FROM = 'support@aitrg.com'
MAIL_PASS = 'ait123'
MAIL_USER = 'support@aitrg.com'
MAIL_TLS = True
MAIL_SSL = True

# Notification Emails
JOB_NOTIFICATION_EMAILS = ['kavya.d@aitrg.com']
STATUSMAILER_COMPRESSION = 'gzip'

# # periodic mail enabled
PERODICSTATS_MAIL_ENABLED = True
PERODICSTATS_NOTIFY_MAIL = ['kavya.d@aitrg.com']
PERODICSTATS_CHECK_INTERVAL_MIN = 240

# # column Statistic Report
COLUMNSTATS_ENABLED = True
COLUMNSTATS_SKIP_COLUMNS = []

MEMUSAGE_CHECK_INTERVAL_SECONDS = 120.0
MEMUSAGE_ENABLED = True
MEMUSAGE_LIMIT_MB = 700
MEMUSAGE_NOTIFY_MAIL = ['kavya.d@aitrg.com']
MEMUSAGE_WARNING_MB = 600

# pause and resume job directory
JOB_DIR_PAUSE_RESUME = os.path.join(BASE_DIR, "jobDir")

# selenium Download Path
SELENIUM_DOWNLOAD_PATH = os.path.join(BASE_DIR, "download")
CLEAN_DOWNLOAD_PATH_ENABLED = True

# Captcha Coder
CAPTCHA_CODER_BASE_URL = 'http://api.captchacoder.com/imagepost.ashx'
CAPTCHA_CODER_API_KEY = 'JZOCTSQ69BKLQXKPQEZTG6ORNNHBCH8YOCMHX5J8'
CAPTCHA_CODER_MAX_RETRY = 5

# enable Depth stats
DEPTH_STATS_VERBOSE = True
URLLENGTH_LIMIT = 50000

# Request Tracking
REQUEST_TRACKING_PATH = os.path.join(BASE_DIR, "responseTracking")
