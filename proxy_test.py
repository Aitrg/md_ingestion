import requests

# proxies = {
#     'http': 'http://lum-customer-americanitresource-zone-static:vgzy7zg1xoim@38.131.152.83',
#     'https': 'https://lum-customer-americanitresource-zone-static:vgzy7zg1xoim@38.131.152.83',
#     'http': 'http://lum-customer-americanitresource-zone-static:vgzy7zg1xoim@38.131.153.175',
#     'https': 'https://lum-customer-americanitresource-zone-static:vgzy7zg1xoim@38.131.153.175',
# }

proxies = {
    'http': 'http://srejepplcd:38afdb5573de6e875a72@5.79.66.2:13200',
    'https': 'https://srejepplcd:38afdb5573de6e875a72@5.79.66.2:13200'
}

# s = requests.Session()
# s.proxies = proxies
r = requests.get('https://www.nvsos.gov/SOSDocumentPrep/AnonymousAccess/Search.aspx', proxies=proxies)
print(r.text)


# http://data827:cubes38@149.115.188.207:8888


# lum-customer-americanitresource-zone-static

# vgzy7zg1xoim


# 38.131.152.83

# import random
# username = 'lum-customer-americanitresource-zone-static'
# password = 'vgzy7zg1xoim'
# port = 22225
# session_id = random.random()
# super_proxy_url = ('http://%s-session-%s:%s@zproxy.lum-superproxy.io:%d' %
#     (username, session_id, password, port))
# proxies = {
#     'http': super_proxy_url,
#     'https': super_proxy_url
# }

# r = requests.get('http://www.showmemyip.com/', proxies=proxies)
# print(r.text)

# opener = requests.build_opener(proxy_handler)
# print('Performing request')
# print(opener.open('http://www.showmemyip.com/').read())
