'''
Created on 05-Feb-2019

@author: srinivasan
'''
from Data_scuff.extensions.solver.captchacoder import CaptchaCoderApi
import requests
kwargs = {}
cc = CaptchaCoderApi('http://api.captchacoder.com/imagepost.ashx',
                     'PI6FF36UY6E83PRYVQYDPM4D81VPXWME3KBWGUOQ', **kwargs)
print(cc.balance_query())

d = requests.get('https://azbop.igovsolution.com/online/Captcha.aspx').content
with open('/home/srinivasan/Desktop/1up/picture_out.gif', 'wb') as f:
    f.write(d)
