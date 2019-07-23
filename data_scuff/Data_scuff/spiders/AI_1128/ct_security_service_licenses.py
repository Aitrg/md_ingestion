# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-12 05:39:09
TICKET NUMBER -AI_1128
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1128.items import CtSecurityServiceLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
import tabula
import pandas as pd
import re
import numpy as np
from PyPDF2 import PdfFileReader

class CtSecurityServiceLicensesSpider(CommonSpider):
    name = 'ai_1128_ct_security_service_licenses'
    allowed_domains = ['ct.gov']
    start_urls = ['https://portal.ct.gov/-/media/DESPP/files/bondsmanpdf.pdf?la=en']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1128_Licenses_Security_Service_CT_CurationReady'),
        'JIRA_ID':'AI_1128',
        'HTTPCACHE_ENABLED': False,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        # 'JOBDIR' : CustomSettings.getJobDirectory('ct_security_service_licenses'),
        'TOP_HEADER':{                        'bail limit': 'Bail Limit',
                         'company_name': '',
                         'dba_name': '',
                         'location_address_string': 'Address',
                         'permit_lic_desc': '',
                         'permit_lic_exp_date': 'Exp Date',
                         'permit_lic_no': 'Lic No.',
                         'permit_lic_status': 'Status',
                         'permit_subtype': 'Type',
                         'permit_type': '',
                         'person_name': 'Agent/Instructors',
                         'person_phone': 'Phone',
                         'person_subtype': ''},
        'FIELDS_TO_EXPORT':[                        
                         'permit_subtype',
                         'company_name',
                         'person_name',
                         'dba_name',
                         'person_subtype',
                         'permit_lic_no',
                         'bail limit',
                         'permit_lic_status',
                         'permit_lic_exp_date',
                         'location_address_string',
                         'person_phone',
                         'permit_lic_desc',
                         'permit_type',
                         'url',
                         'sourceName',
                         'ingestion_timestamp'],
                         
        'NULL_HEADERS':['bail limit']
        }

    def _getPersonName(self, mixed_name):
        if mixed_name!=None and ',' in mixed_name:
            diff = mixed_name.split(',')
            fir_name, sec_name = diff[0], diff[1]
            mixed_name = ' '.join([sec_name, fir_name])
        elif mixed_name==None:
            mixed_name =''
        return mixed_name.strip()

    def parse(self, response):

        file1='https://portal.ct.gov/-/media/DESPP/files/bondsmanpdf.pdf?la=en'
        file2='https://portal.ct.gov/-/media/DESPP/SLFU/bea/BEALIST82017pdf.pdf?la=en'
        file7='https://portal.ct.gov/-/media/DESPP/SLFU/bondsman/BondsmanFirearmInstructorspdf.pdf?la=en'
        
        #------->OCR format convert and give file path
        file3='/home/ait-python/Downloads/pdf (2)/Licensed-Private-Detectives-and-Security-Companies.pdf'
        file4='/home/ait-python/Downloads/pdf (2)/bluecardinstructorpdf.pdf'
        file5='/home/ait-python/Downloads/pdf (2)/beainstructor032018pdf.pdf'
        file6='/home/ait-python/Downloads/pdf (2)/Public-Security-Instructors.pdf'
        #--------->
        if file1:
            df = tabula.read_pdf(file1,
            pages ='1',
            area = [110.635,11.385,595.485,768.735],
            columns=[170.775,218.295,279.675,329.175,395.505,526.185,636.075,668.745,700.355,764.775],
            silent=True,
            # stream=True,
            # multiple_tables=True,
            guess = False,
            encoding='utf-8',
            pandas_option = {'header':None})
            for _, row in df.fillna('').iterrows():
                row=row.tolist()
                old_name=str(row[0]).split(',')
                name=old_name[1]+' '+old_name[0]
                print(name)
                permit_lic_no=row[1]
                bail_limit=str(row[2])
                permit_lic_status=row[3]
                permit_lic_exp_date=row[4]
                address=str(row[5])
                city=str(row[6])
                state=str(row[7])
                zippp=str(row[8])
                print("===============",zippp)                
                location_address_string=self.format__address_4(address,city,state,zippp)
                phone=str(row[9]).replace('.0','')
                permit_subtype='Bail Bondsman'
                company_name=name
                il = ItemLoader(item=CtSecurityServiceLicensesSpiderItem())
                il.add_value('url', 'https://portal.ct.gov/-/media/DESPP/files/bondsmanpdf.pdf?la=en')
                il.add_value('sourceName', 'CT_Security_Service_Licenses')
                il.add_value('permit_subtype',permit_subtype)
                il.add_value('company_name',name)
                il.add_value('person_name',name)
                il.add_value('dba_name', '')
                il.add_value('person_subtype', 'Agent')
                il.add_value('permit_lic_no',permit_lic_no)
                il.add_value('bail limit',bail_limit)
                il.add_value('permit_lic_status', permit_lic_status)
                il.add_value('permit_lic_exp_date',permit_lic_exp_date)
                il.add_value('location_address_string',location_address_string)
                il.add_value('person_phone',phone)
                il.add_value('permit_lic_desc',permit_subtype)
                il.add_value('permit_type', 'security_license')
                yield il.load_item()

        if file2:
            df = tabula.read_pdf(file2,
            pages ='all',
            area = [30.175,42.57,584.595,773.19],
            columns=[182.16,233.64,295.02,369.27,487.08,600.93,642.51,681.12,757.35],
            # spreadsheet=True,
            silent=True,
            stream=True,
            # multiple_tables=True,
            # guess = True,
            encoding='ISO-8859-1',
            pandas_option = {'header':None})

            for _, row in df.fillna('').iterrows():
                row=row.tolist()     
                old_name=str(row[0]).split(',')
                name=old_name[1]+' '+old_name[0]
                # print(old_name)
                permit_lic_no=str(row[1])
                permit_lic_status=str(row[2])
                permit_lic_exp_date=str(row[3])
                address=str(row[4])
                city=str(row[5])
                state=str(row[6])
                zippp=str(row[7])
                phone=str(row[8]).replace('.0','')
                location_address_string=self.format__address_4(address,city,state,zippp)
                permit_subtype='Bail enforcement Agents'
                il = ItemLoader(item=CtSecurityServiceLicensesSpiderItem())
                il.add_value('url', 'https://portal.ct.gov/-/media/DESPP/files/bondsmanpdf.pdf?la=en')
                il.add_value('sourceName', 'CT_Security_Service_Licenses')
                il.add_value('permit_subtype',permit_subtype)
                il.add_value('company_name',name)
                il.add_value('person_name',name)
                il.add_value('dba_name', '')
                il.add_value('person_subtype', 'Agent')
                il.add_value('permit_lic_no',permit_lic_no)
                il.add_value('bail limit','')
                il.add_value('permit_lic_status', permit_lic_status)
                il.add_value('permit_lic_exp_date',permit_lic_exp_date)
                il.add_value('location_address_string',location_address_string)
                il.add_value('person_phone',phone)
                il.add_value('permit_lic_desc', 'Bail enforcement Agents')
                il.add_value('permit_type', 'security_license')
                yield il.load_item()

        if file3:

            def __extractData(self,response):
                def rolling_group(val):
                    if pd.notnull(val): 
                    # if pd.notnull(val) and '/' in val and not 'st' in val:
                        rolling_group.group += 1
                    return rolling_group.group
                rolling_group.group = 0  

                def joinFunc(g, column):
                    col = g[column]
                    joiner = "/"
                    s = joiner.join([str(each) for each in col if pd.notnull(each)])
                    s = re.sub("(?<=&)" + joiner, " ", s)  
                    s = re.sub("(?<=-)" + joiner, " ", s)  
                    s = re.sub(joiner * 2, joiner, s)
                    return s

                def getDf(file3, area):
                    return tabula.read_pdf(file3,
                        pages ='all',
                        area = [45.045,29.7,582.615,781.11],
                        columns=[217.8,268.29,326.7,375.16,532.62,617.76,670.23,699.93,779.13],
                        guess = False,
                        encoding='ISO-8859-1',
                        pandas_option = {'header':None})
                df = getDf(file3,[45.045,29.7,582.615,781.11])

                df.columns = ['company','lic_no','status','exp_date','street','city','state','zip','phone'] 
                groups = df.groupby(df['status'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                final_df = groups.apply(groupFunct).fillna('')
                yield final_df.to_dict('records')

            for col in __extractData(self,response):
                # print(col)
                for row in col:
                    company_name=str(row['company'])
                    permit_lic_no=row['lic_no']
                    permit_lic_status=row['status']
                    permit_lic_exp_date=row['exp_date']
                    address=str(row['street'])
                    city=str(row['city'])
                    state=str(row['state'])
                    zippp=str(row['zip'])
                    location_address_string=self.format__address_4(address,city,state,zippp)
                    permit_subtype='Private Investigators/Security Companies'
                    phone=str(row['phone']).replace('.0','')
                    il = ItemLoader(item=CtSecurityServiceLicensesSpiderItem())
                    il.add_value('url', 'https://portal.ct.gov/-/media/DESPP/files/bondsmanpdf.pdf?la=en')
                    il.add_value('sourceName', 'CT_Security_Service_Licenses')
                    il.add_value('permit_subtype',permit_subtype)
                    il.add_value('company_name',self._getDBA(company_name)[0])
                    il.add_value('person_name','')
                    il.add_value('dba_name',self._getDBA(company_name)[1])
                    il.add_value('person_subtype', '')
                    il.add_value('permit_lic_no',permit_lic_no)
                    il.add_value('bail limit','')
                    il.add_value('permit_lic_status', permit_lic_status)
                    il.add_value('permit_lic_exp_date',permit_lic_exp_date)
                    il.add_value('location_address_string',location_address_string)
                    il.add_value('person_phone',phone)
                    il.add_value('permit_lic_desc',permit_subtype)
                    il.add_value('permit_type', 'security_license')
                    yield il.load_item()

        if file4:
            def __extractData(self, pdflink):
                def rolling_group(val):
                    if pd.notnull(val): 
                        rolling_group.group += 1
                    return rolling_group.group
                rolling_group.group = 0

                def joinFunc(g, column):
                    col = g[column]
                    joiner = "/"
                    s = joiner.join([str(each) for each in col if pd.notnull(each)])
                    s = re.sub("(?<=-)" + joiner, " ", s)  
                    s = re.sub(joiner * 2, joiner, s)
                    return s
                def getDF(area,column):
                    df = tabula.read_pdf(file4,
                        pages= 'all',
                        silent = True,
                        guess=False,
                        columns=column,
                        area=area,  
                        encoding='ISO-8859-1',
                        pandas_options={'header': None, 'error_bad_lines': False, 'warn_bad_lines': False}
                        ).replace('\r', ' ', regex=True).dropna(how='all')
                    return df
                df = getDF([70.763,34.425,756.203,589.815],[212.67,470.475,589.815])
                df.columns=['a','b','c']
                
                groups = df.groupby(df['c'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                final_df = groups.apply(groupFunct).fillna('')
                
                return final_df.to_dict('records')

            for col in __extractData(self, file4):
                mixed_name=self._getPersonName(col['a'])
                company=col['b'].split('/')
                if len(company)>2:
                    company_add1=company[1]
                    company_add2=company[2]
                    location_address_string=company_add1+', '+company_add2
                    c_name=company[0]
                    print("==============",c_name)
                    phone=col['c']
                    permit_subtype='Bail Firearms instructors'

                    il = ItemLoader(item=CtSecurityServiceLicensesSpiderItem())
                    il.add_value('url', 'https://portal.ct.gov/-/media/DESPP/files/bondsmanpdf.pdf?la=en')
                    il.add_value('sourceName', 'CT_Security_Service_Licenses')
                    il.add_value('permit_subtype',permit_subtype)
                    il.add_value('company_name',c_name)
                    il.add_value('person_name',mixed_name)
                    il.add_value('dba_name', '')
                    il.add_value('person_subtype', '')
                    il.add_value('permit_lic_no','')
                    il.add_value('bail limit','')
                    il.add_value('permit_lic_status', '')
                    il.add_value('permit_lic_exp_date','')
                    il.add_value('location_address_string',location_address_string)
                    il.add_value('person_phone',phone)
                    il.add_value('permit_lic_desc',permit_subtype)
                    il.add_value('permit_type', 'security_license')
                    yield il.load_item()


        if file5:
            def __extractData(pdflink):
                def rolling_group(val):
                    if pd.notnull(val): 
                        rolling_group.group += 1
                    return rolling_group.group
                rolling_group.group = 0

                def joinFunc(g, column):
                    col = g[column]
                    joiner = "/"
                    s = joiner.join([str(each) for each in col if pd.notnull(each)])
                    s = re.sub("(?<=-)" + joiner, " ", s)  
                    s = re.sub(joiner * 2, joiner, s)
                    return s
                def getDF(area,column):
                    df = tabula.read_pdf(file5,
                        pages= 'all',
                        silent = True,
                        guess=False,
                        columns=column,
                        area=area,  
                        encoding='ISO-8859-1',
                        pandas_options={'header': 'infer', 'error_bad_lines': False, 'warn_bad_lines': False}
                        ).replace('\r', ' ', regex=True).dropna(how='all')
                    return df
                df = getDF([70.785,23.76,581.625,713.79],[183.15,269.28,446.49,672.21])
                df.columns=['a','b','c','d']

                groups = df.groupby(df['a'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')

            for col in __extractData(file5):
                person_name=col['a']
                mail=col['c'].split('/')
                if len(mail)==1:
                    phone=mail[0]
                else:
                    phone=mail[1]
                add=col['d'].split('/')
                company_name=add[0]
                location_address_string=add[1]+', '+add[2]
                permit_subtype='Bail enforcement instructors'
                il = ItemLoader(item=CtSecurityServiceLicensesSpiderItem())
                il.add_value('url', 'https://portal.ct.gov/-/media/DESPP/files/bondsmanpdf.pdf?la=en')
                il.add_value('sourceName', 'CT_Security_Service_Licenses')
                il.add_value('permit_subtype',permit_subtype)
                il.add_value('company_name',company_name)
                il.add_value('person_name',person_name)
                il.add_value('dba_name', '')
                il.add_value('person_subtype', 'Instructors')
                il.add_value('permit_lic_no','')
                il.add_value('bail limit','')
                il.add_value('permit_lic_status', '')
                il.add_value('permit_lic_exp_date','')
                il.add_value('location_address_string',location_address_string)
                il.add_value('person_phone',phone)
                il.add_value('permit_lic_desc',permit_subtype)
                il.add_value('permit_type', 'security_license')
                yield il.load_item()


        if file6:
            def __extractData(pdflink):
                def rolling_group(val):
                    if pd.notnull(val): 
                        rolling_group.group += 1
                    return rolling_group.group
                rolling_group.group = 0

                def joinFunc(g, column):
                    col = g[column]
                    joiner = "/"
                    s = joiner.join([str(each) for each in col if pd.notnull(each)])
                    s = re.sub("(?<=-)" + joiner, " ", s)  
                    s = re.sub(joiner * 2, joiner, s)
                    return s
                def getDF(area,column):
                    df = tabula.read_pdf(file6,
                        pages= 'all',
                        silent = True,
                        guess=False,
                        columns=column,
                        area=area,  
                        encoding='ISO-8859-1',
                        pandas_options={'header': 'infer', 'error_bad_lines': False, 'warn_bad_lines': False}
                        ).replace('\r', ' ', regex=True).dropna(how='all')
                    return df
                df = getDF([73.755,42.57,572.715,770.22],[156.42,218.79,253.44,306.9,398.97,489.06,682.11,765.27])
                df.columns=['a','b','c','d','e','f','g','h']

                groups = df.groupby(df['h'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')

            for col in __extractData(file6):
                person_name=col['a'].split('/')
                f_name=person_name[0].split(',')

                permit_lic_no=person_name[1]

                new_lic_no=re.search(r'\d\d-\d*',permit_lic_no)
                lic_no=''
                if new_lic_no:
                    lic_no=new_lic_no.group()

                if len(f_name)==1:
                    name=f_name[0]
                else:
                    name=f_name[1]+' '+f_name[0]

                permit_lic_status=col['f'].split('/')[0].replace('0','')
                location=col['g'].split('/')
                location_address_string=location[0]+', '+location[1]+' '+location[2]
                phone=col['h']

                permit_subtype='Public Security Instructors'
                il = ItemLoader(item=CtSecurityServiceLicensesSpiderItem())
                il.add_value('url', 'https://portal.ct.gov/-/media/DESPP/files/bondsmanpdf.pdf?la=en')
                il.add_value('sourceName', 'CT_Security_Service_Licenses')
                il.add_value('permit_subtype',permit_subtype)
                il.add_value('company_name',name)
                il.add_value('person_name',name)
                il.add_value('dba_name', '')
                il.add_value('person_subtype', 'Instructors')
                il.add_value('permit_lic_no',lic_no)
                il.add_value('bail limit','')
                il.add_value('permit_lic_status',permit_lic_status)
                il.add_value('permit_lic_exp_date','')
                il.add_value('location_address_string',location_address_string)
                il.add_value('person_phone',phone)
                il.add_value('permit_lic_desc',permit_subtype)
                il.add_value('permit_type', 'security_license')
                yield il.load_item()

        if file7:
            self.check_val=False
            def _extractpdf(self, response):
                def rolling_group(val):
                    match = re.search(r'(\([\d]{3}\))[\s][\d]{3}-[\d]{4}', val)
                    if pd.notnull(val) and match:
                        self.check_val = True
                    elif self.check_val:
                        rolling_group.group += 1
                        self.check_val = False
                    return rolling_group.group
                rolling_group.group = 0

                df = tabula.read_pdf(file7,
                        pages='all',
                        guess=False,
                        columns = [263.16,573.75],
                        encoding='ISO-8859-1',
                        pandas_options = {'header':None}
                        ).fillna('')

                df = df.drop([i for i in range(11)])
                Series =df[0].append(df[1]).reset_index(drop=True)
                df = Series.to_frame(name=None)
                groups = df.groupby(df[0].apply(rolling_group), as_index=False)

                for i in groups: 
                    x= pd.DataFrame(i[1]).reset_index(drop=True)
                    if (x.apply(len).values[0])>1:
                        x.replace('', np.nan, inplace=True)
                        df1 = x.apply(lambda x: pd.Series(x.dropna().values))
                        df1[0] = df1.apply(lambda x:x[0] if not x[0].isdigit() and len(x)!=0 else np.nan, axis = 1) 
                        df1[1] = x[0][0]
                        df1[3] = x[0][1]
                        df1[0] = df1.apply(lambda x:x[0] if not x[0]==x[1] else np.nan, axis = 1) #dba name
                        df1[2] = df1.apply(lambda x:x[0] if str(x[0]).startswith('(') else np.nan, axis = 1)
                        df1[0] = df1.apply(lambda x:x[0] if x[0]!=x[2] else np.nan, axis = 1) 
                        df1[0] = df1.apply(lambda x:x[0] if x[0]!=x[3] else np.nan, axis = 1)
                        df1[4] = df1[0].str.cat(sep=', ')
                        df1 = df1.drop([0], axis=1)
                        df1[2].fillna(method= 'bfill',inplace = True)
                        df1.drop_duplicates(subset =1, inplace = True)
                        df1=df1.fillna('')
                        df1.columns = ['person_name','company_name', 'company_phone', 'location']
                        final_df = df1.to_dict('records')
                        # print(final_df)
                        yield final_df

            for col in _extractpdf(self,response):
                # print(col)
                for row in col:
                    print('________________', row)
                    permit_subtype='Bail Firearms instructors'
                    il = ItemLoader(item=CtSecurityServiceLicensesSpiderItem())
                    il.add_value('url', 'https://portal.ct.gov/-/media/DESPP/files/bondsmanpdf.pdf?la=en')
                    il.add_value('sourceName', 'CT_Security_Service_Licenses')
                    il.add_value('permit_subtype',permit_subtype)
                    il.add_value('company_name',row['company_name'])
                    il.add_value('person_name',row['person_name'])
                    il.add_value('dba_name', '')
                    il.add_value('person_subtype', 'Instructors')
                    il.add_value('permit_lic_no','')
                    il.add_value('bail limit','')
                    il.add_value('permit_lic_status','')
                    il.add_value('permit_lic_exp_date','')
                    il.add_value('location_address_string',row['location'])
                    il.add_value('person_phone',row['company_phone'])
                    il.add_value('permit_lic_desc',permit_subtype)
                    il.add_value('permit_type', 'security_license')
                    yield il.load_item()
 












