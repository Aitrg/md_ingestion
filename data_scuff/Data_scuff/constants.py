'''
Created on 02-Jun-2018

@author: srinivasan
'''

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT_SHORT = "%Y-%m-%d"


class RegularExpConstant:
    import re
    URL = re.compile(r'^https?://[^/]+(/.*)?')
    DOMAIN_FROM_URL = re.compile(
        r'^https?://([\w\-]+\.)*([\w\-]+)\.(\w{2,3}\.\w{2}|\w{2,4})/?'
    )
    HTML_TAGS = re.compile(r'<[^>]+>')
    NON_ENG_CHARS = re.compile(r'[^A-Za-z\s]+')
    NON_PRINTABLE_CHARS = re.compile('[\x00-\x01](?![\x00-\x0f])')
    EMAIL = re.compile(r'[a-zA-Z0-9_\.+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-\.]+')
