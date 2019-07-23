# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-18 08:58:35
TICKET NUMBER -AI_1381
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1381.items import IlKankakeeFoodInspectionsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
from inline_requests import inline_requests
import re


class IlKankakeeFoodInspectionsSpider(CommonSpider):
    name = '1381_il_kankakee_food_inspections'
    allowed_domains = ['kankakeehealth.org']
    start_urls = ['http://www.kankakeehealth.org/environmental-health/food-sanitation/food_inspections.html']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1381_Inspections_Food_IL_Kankakee_CurationReady'),
        'JIRA_ID':'AI_1381',
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        'DOWNLOAD_DELAY':2,
        'CONCURRENT REQUESTS':1,
        # 'JOBDIR' : CustomSettings.getJobDirectory('IlKankakeeFoodInspectionsSpider'),
        'TOP_HEADER':{   'abate_date': '',
                         'abate_status': 'Compliance Status.1',
                         'company_name': 'Establishment',
                         'dba_name': 'Permit Holder',
                         'inspection_date': '',
                         'inspection_pass_fail': 'Inspection Result',
                         'inspection_subtype': 'Purpose of Inspection',
                         'inspection_type': '',
                         'inspector_comments': 'Remarks',
                         'location_address_string': 'Street Address',
                         'permit_lic_desc': '',
                         'permit_lic_no': 'License/Permit #',
                         'permit_type': '',
                         'risk category': 'Risk Category',
                         'temperature observations-item/location': 'TEMPERATURE OBSERVATIONS-Item/Location',
                         'violation category': 'Violation Category',
                         'violation_date': '',
                         'violation_description': 'OBSERVATIONS AND CORRECTIVE ACTIONS or Deficiencies/Remarks/Corrections',
                         'violation_rule': 'Compliance Status',
                         'violation_rule_id': 'Item Number',
                         'violation_subtype': '',
                         'violation_type': ''},
        'FIELDS_TO_EXPORT':[
                         'company_name',
                         'permit_lic_no',
                         'location_address_string',
                         'dba_name',
                         'risk category',
                         'inspection_date',
                         'inspection_subtype',
                         'inspection_pass_fail',
                         'inspector_comments',
                         'inspection_type',
                         'violation_date',
                         'violation_rule_id',
                         'violation_rule',
                         'abate_date',
                         'abate_status',
                         'violation category',
                         'violation_subtype',
                         'temperature observations-item/location',
                         'violation_description',
                         'violation_type',
                         'permit_lic_desc',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp',
                         ],
        'NULL_HEADERS':['temperature observations-item/location', 'violation category', 'risk category']
        }
    def __init__(self, start=None, end=None,startnum=None,endnum=None, proxyserver=None, *a, **kw):
        super(IlKankakeeFoodInspectionsSpider, self).__init__(start,end, proxyserver=None,*a, **kw)
        import csv
        import os
        current_file_path = os.path.abspath(os.path.dirname(__file__))+'/AI_1381_permit_no_list_{}_{}.csv'.format(self.start, self.end)
        self.csv = open(current_file_path, "w") 
        columnTitleRow = "permit_lic_no\n"
        self.csv.write(columnTitleRow)
    def parse(self, response):
        yield scrapy.Request(url='http://il.healthinspections.us/kankakee/',dont_filter=True, callback=self.parse_link)
    def parse_link(self, response):
        link=response.xpath('//div/table//tr/td[3]/table[1]//tr[2]/td/div[4]/a/@href').extract_first()
        url_join='http://il.healthinspections.us/kankakee/'+str(link)
        yield scrapy.Request(url=url_join,dont_filter=True, callback=self.parse_get)
    slink=[]
    def parse_get(self, response):
        meta={}
        if len(self.slink) == 0:
            self.slink=response.xpath('//*[@id="innersearchbox"]/table//tr/td/a/@href').extract()
        if len(self.slink) > 0:
            search_link=self.slink.pop(0)
            print('========================================>',search_link)
            link_join='http://il.healthinspections.us/kankakee/'+str(search_link)
            yield scrapy.Request(url=link_join,dont_filter=True, callback=self.parse_get1,meta={'page':'1'})
    @inline_requests
    def parse_get1(self, response):
        meta=response.meta
        val_link=response.xpath('//div/table//tr[1]/td[1]/a/@href').extract()
        for val in val_link:
            if 'estab.cfm' in val:
                val_join='http://il.healthinspections.us/kankakee/'+str(val)
                # val_join=''
                parse_get2=yield scrapy.Request(url=val_join,dont_filter=True)
                table=parse_get2.xpath('//div/table//tr[1]/td[1]/div')[1:]
                for i in table:
                    company_name=i.xpath('div[1]/b/text()').extract_first()
                    meta['company_name'] = self._getDBA(company_name)[0]
                    csv_row = str(company_name) + "\n"
                    meta['permit_lic_desc']='Restaurant License for '+str(company_name)
                    address=i.xpath('div[2]/text()').extract()
                    clean_tags = re.compile('<.*?>')
                    meta['location_address_string']=','.join(re.sub('\s+', ' ', re.sub(clean_tags, ' ', desc)) for desc in address)
                    date=i.xpath('div[3]/text()').extract()
                    meta['inspection_date']=''.join(re.sub('\s+', ' ', re.sub(clean_tags, ' ', desc)) for desc in date)
                    ins_link=i.xpath('div/a/@href').extract()
                    for ins in ins_link:
                        if ins:
                            ins_join='http://il.healthinspections.us/'+str(ins.replace('../','/'))
                            parse_get3=yield scrapy.Request(url=ins_join,dont_filter=True)
                            method2=parse_get3.xpath("//*[contains(text(),'Establishment #')]/u/text()").extract_first()
                            met=parse_get3.xpath("//*[contains(text(),'License/Permit #')]/ancestor::td/text()").extract()
                            method1=''.join(str(self.data_clean(desc)) for desc in met)
                            meta['dba_name']=''
                            check=0
                            if method1:
                                check=1
                                meta['permit_lic_no']=method1
                                dba=parse_get3.xpath("//*[contains(text(),'Permit Holder')]/ancestor::td/text()").extract()
                                ris=parse_get3.xpath("//*[contains(text(),'Risk Category')]/ancestor::td/text()").extract()
                                meta['risk']=''.join(str(self.data_clean(desc)) for desc in ris)
                                inspection_sub=parse_get3.xpath("//*[contains(text(),'Purpose of Inspection')]/ancestor::td/text()").extract()
                                meta['inspection_subtype']=''.join(str(self.data_clean(desc)) for desc in inspection_sub)
                                meta['inspection_pass_fail']=''
                                correcive_action=parse_get3.xpath('//*[contains(text(), "OBSERVATIONS AND CORRECTIVE ACTIONS")]/following::table')[1:]
                                check_val=value2=check_val1=des=''
                                dict1={}
                                dict2={}
                                for cor in correcive_action:
                                    descrip= []
                                    vio_comment=''
                                    value=self.data_clean(cor.xpath('tr/td[1]/text()').extract_first())
                                    if value and len(value)>0 and 'Inspection' not in value and ':' not in value:
                                        des=cor.xpath('tr/td[2]').extract()
                                        vio_comment1 = ''.join(str(self.data_clean(desc)) for desc in des)
                                        vio_comment=vio_comment1
                                        if value:
                                            check_val=value
                                            if check_val in dict2.keys():
                                                old_desc_list = dict2[check_val]
                                                old_desc_list.append(vio_comment)
                                                descrip = old_desc_list
                                                dict2[check_val]=descrip
                                            else:
                                                descrip.append(vio_comment)
                                                dict2[check_val]=descrip
                                    else:
                                        pass
                                inspector_comments=parse_get3.xpath('//*[contains(text(), "Inspection Comments")]/following::td[1]/text()').extract()
                                meta['inspector_comments'] = ''.join(str(self.data_clean(desc)) for desc in inspector_comments)
                                meta['inspection_type']='health_inspection'
                                table_path=parse_get3.xpath("//*[contains(text(), 'TEMPERATURE OBSERVATIONS')]/following::table//tr/td/text()").extract()
                                tem = ' '.join(str(self.data_clean(desc)) for desc in table_path)
                                if tem and 'Item Number' in tem:
                                    meta['temperature']=tem.split('Item Number')[0]
                                else:
                                    meta['temperature']=''
                                food=parse_get3.xpath('//div[1]/div/table[4]//tr/td/table//tr')
                                good=parse_get3.xpath('//div[1]/div/table[7]//tr/td/table//tr')
                                rule_list=[]
                                check_yield=0
                                if food:
                                    for food1 in food:
                                        meta['dba_name']=''.join(str(self.data_clean(desc)) for desc in dba)

                                        out=food1.xpath('td[3]/span[@style="padding-left:4px;padding-right:4px;border:solid 1px red;-webkit-border-radius: 25px;-moz-border-radius: 25px;border-radius: 25px;"]/text()').extract_first()
                                        if out and 'OUT' in out:
                                            check_yield=1
                                            meta['rule_id']=self.data_clean(food1.xpath('td[1]/text()').extract_first())
                                            rule_list.append(meta['rule_id'])
                                            meta['rule']=food1.xpath('td[6]/text()').extract_first()
                                            meta['violation_description']=''
                                            if meta['rule_id'] in dict2.keys():
                                                vio_Des=dict2[meta['rule_id']]
                                            else:
                                                vio_Des=''
                                            for vio_Des1 in vio_Des:
                                                meta['violation_description']=vio_Des1
                                                meta['violation_subtype']='Critical'
                                                meta['violation_type']='health_violation'
                                                meta['violation_category']='FOODBORNE ILLNESS RISK FACTORS AND PUBLIC HEALTH INTERVENTIONS'
                                                meta['violation_date']=meta['inspection_date']
                                                meta['abate_status']=meta['abate_date']=''
                                                if 'Correct By:' in meta['violation_description']:
                                                    meta['abate_status']='corrected on-site during inspection'
                                                    meta['abate_date']=self.format_date(meta['violation_description'].split('(Correct By:')[1].split(')')[0])
                                                else:
                                                    meta['abate_status']=meta['abate_date']=''
                                                yield self.save_to_csv(response,**meta)
                                                meta['dba_name']=''
                                                dba_namee = self._getDBA(company_name)[1]
                                                if dba_namee and len(dba_namee) > 3:
                                                    meta['dba_name']=dba_namee
                                                    yield self.save_to_csv(response,**meta)

                                if good:
                                    meta['rule_id']=meta['rule']=meta['violation_description']=meta['violation_type']=meta['violation_subtype']=meta['abate_status']=meta['abate_date']=''
                                    for good1 in good:
                                        meta['dba_name']=''.join(str(self.data_clean(desc)) for desc in dba)
                                        x=good1.xpath('td[2]/text()').extract_first()
                                        if x and 'X' in x:
                                            check_yield=1
                                            meta['rule_id']=self.data_clean(good1.xpath('td[1]/text()').extract_first())
                                            rule_list.append(meta['rule_id'])
                                            meta['rule']=good1.xpath('td[3]/text()').extract_first()
                                            meta['violation_description']=''
                                            if meta['rule_id'] in dict2.keys():
                                                vio_Des=dict2[meta['rule_id']]
                                            else:
                                                vio_Des=''
                                            for vio_Des1 in vio_Des:
                                                meta['violation_description']=vio_Des1
                                                meta['violation_category']='GOOD RETAIL PRACTICES'
                                                meta['violation_subtype']='Non Critical'
                                                meta['violation_type']='health_violation'
                                                meta['violation_date']=meta['inspection_date']
                                                meta['abate_status']=meta['abate_date']=''
                                                if 'Correct By:' in meta['violation_description']:
                                                    meta['abate_status']='corrected on-site during inspection'
                                                    meta['abate_date']=self.format_date(meta['violation_description'].split('(Correct By:')[1].split(')')[0])
                                                yield self.save_to_csv(response,**meta)
                                                meta['dba_name']=''
                                                dba_namee = self._getDBA(company_name)[1]
                                                if dba_namee and len(dba_namee) > 3:
                                                    meta['dba_name']=dba_namee
                                                    yield self.save_to_csv(response,**meta)
                                                
                                for rule in dict2.keys():
                                    if rule in rule_list:
                                        pass
                                    else:
                                        meta['dba_name']=''.join(str(self.data_clean(desc)) for desc in dba)
                                        check_yield=1
                                        meta['rule_id']=rule
                                        meta['violation_description']=dict2[rule]
                                        meta['violation_subtype']=''
                                        meta['violation_type']='health_violation'
                                        meta['violation_date']=meta['inspection_date']
                                        meta['violation_category']=meta['abate_status']=meta['abate_date']=''
                                        meta['rule']=''
                                        yield self.save_to_csv(response,**meta)
                                        meta['dba_name']=''
                                        dba_namee = self._getDBA(company_name)[1]
                                        if dba_namee and len(dba_namee) > 3:
                                            meta['dba_name']=dba_namee
                                            yield self.save_to_csv(response,**meta)
                                if check_yield==0:
                                    meta['dba_name']=''.join(str(self.data_clean(desc)) for desc in dba)
                                    print('==================check_yield')
                                    meta['rule_id']=''
                                    meta['violation_description']=''
                                    meta['violation_subtype']=''
                                    meta['violation_type']=''
                                    meta['violation_date']=''
                                    meta['violation_category']=meta['abate_status']=meta['abate_date']=''
                                    meta['rule']=''
                                    yield self.save_to_csv(response,**meta)
                                    meta['dba_name']=''
                                    dba_namee = self._getDBA(company_name)[1]
                                    if dba_namee and len(dba_namee) > 3:
                                        meta['dba_name']=dba_namee
                                        yield self.save_to_csv(response,**meta)
                            if method2:
                                check=1
                                meta['permit_lic_no']=method2
                                meta['inspection_subtype']=parse_get3.xpath("//table[1]//tr[1]/td/table[1]//tr/td[3]/font/strong/b[contains(text(),'X')]/following::text()").extract_first()
                                check_val=value2=check_val1=des=''
                                dict1={}
                                dict2={}
                                correcive_action=parse_get3.xpath('//*[contains(text(), "Remarks and Recommendations for Corrections")]/ancestor::table//tr')[2:]
                                for cor in correcive_action:
                                    descrip= []
                                    value=self.data_clean(cor.xpath('td[1]/text()').extract_first())
                                    if value and len(value)>0:
                                        des=cor.xpath('td[2]').extract()
                                        add=cor.xpath('td[3]/text()').extract_first()
                                        vio_com = ''.join(str(self.data_clean(desc)) for desc in des)
                                        vio_comment=str(vio_com)+str(add)
                                    else:
                                        pass
                                    if value:
                                        check_val=value
                                        if check_val in dict2.keys():
                                            old_desc_list = dict2[check_val]
                                            old_desc_list.append(vio_comment)
                                            descrip = old_desc_list
                                            dict2[check_val]=descrip
                                        else:
                                            descrip.append(vio_comment)
                                            dict2[check_val]=descrip
                                meta['risk']=meta['inspection_pass_fail']=meta['temperature']=meta['violation_date']=meta['rule']=meta['rule_id']=meta['abate_date']=meta['abate_status']=meta['violation_category']=meta['violation_subtype']=meta['violation_description']=meta['violation_type']=''
                                inspector_comments=parse_get3.xpath("//*[contains(text(),'General Comments')]/following::tr/td[@style='border:2px solid black; padding:3px']/text()").extract()
                                meta['inspector_comments'] = ' '.join(str(self.data_clean(desc)) for desc in inspector_comments)
                                meta['inspection_type']='health_inspection'
                                vio_tab=parse_get3.xpath('//table[1]//tr[1]/td/table[4]//tr')
                                for vio in vio_tab:
                                    rule=vio.xpath('//*[@class="checkX"]/preceding::td[1]/following-sibling::td[3]/text()').extract()
                                    rule_id=vio.xpath('//*[@class="checkX"]/preceding::td[1]//text()').extract()
                                vio_dict = dict(zip(rule_id, rule))
                                if len(vio_dict)>0:
                                    for n in vio_dict.keys():
                                        meta['dba_name']=parse_get3.xpath('//table[1]//tr[1]/td/table[3]//tr[1]/td[1]/font/strong[contains(text(),"Owner or Operator")]/following::td/text()').extract_first()
                                        if n in dict2.keys():
                                            vio_Des=dict2[n]
                                            meta['rule']=vio_dict[n]
                                            meta['rule_id']=n
                                        else:
                                            meta['violation_description']=''
                                            meta['violation_subtype']=''
                                            meta['violation_type']='health_violation'
                                            meta['violation_date']=meta['inspection_date']
                                            meta['violation_category']=meta['abate_status']=meta['abate_date']=''
                                            meta['rule']=vio_dict[n]
                                            meta['rule_id']=n
                                            yield self.save_to_csv(response,**meta)
                                            meta['dba_name']=''
                                            dba_namee = self._getDBA(company_name)[1]
                                            if dba_namee and len(dba_namee) > 3:
                                                meta['dba_name']=dba_namee
                                                yield self.save_to_csv(response,**meta)
                                        for vio_Des1 in vio_Des:
                                            meta['dba_name']=parse_get3.xpath('//table[1]//tr[1]/td/table[3]//tr[1]/td[1]/font/strong[contains(text(),"Owner or Operator")]/following::td/text()').extract_first()
                                            meta['violation_description']=vio_Des1
                                            meta['violation_subtype']=''
                                            meta['violation_type']='health_violation'
                                            meta['violation_date']=meta['inspection_date']
                                            meta['violation_category']=''
                                            meta['abate_status']=meta['abate_date']=''
                                            if 'Onsite' in meta['violation_description']:
                                                meta['abate_status']='corrected on-site during inspection'
                                                meta['abate_date']=meta['inspection_date']
                                            else:
                                                meta['abate_status']=meta['abate_date']=''
                                            meta['violation_description']=meta['violation_description'].replace('Immediate/Onsite','').replace('Next Inspection','').replace('NEXT INSPECTION','') if meta['violation_description'] and len(meta['violation_description'])>2 else ''
                                            yield self.save_to_csv(response,**meta)
                                            meta['dba_name']=''
                                            dba_namee = self._getDBA(company_name)[1]
                                            if dba_namee and len(dba_namee) > 3:
                                                meta['dba_name']=dba_namee
                                                yield self.save_to_csv(response,**meta)
                                    for m in dict2.keys():
                                        if m in vio_dict.keys():
                                            pass
                                        else:
                                            meta['dba_name']=parse_get3.xpath('//table[1]//tr[1]/td/table[3]//tr[1]/td[1]/font/strong[contains(text(),"Owner or Operator")]/following::td/text()').extract_first()
                                            meta['rule_id']=m
                                            meta['violation_description']=dict2[m]
                                            meta['violation_description']=meta['violation_description'].replace('Immediate/Onsite','').replace('Next Inspection','').replace('NEXT INSPECTION','') if meta['violation_description'] and len(meta['violation_description'])>2 else ''
                                            meta['violation_subtype']=''
                                            meta['violation_type']='health_violation'
                                            meta['violation_date']=meta['inspection_date']
                                            meta['violation_category']=meta['abate_status']=meta['abate_date']=''
                                            meta['rule']=''
                                            yield self.save_to_csv(response,**meta)
                                            meta['dba_name']=''
                                            dba_namee = self._getDBA(company_name)[1]
                                            if dba_namee and len(dba_namee) > 3:
                                                meta['dba_name']=dba_namee
                                                yield self.save_to_csv(response,**meta)
                                else:
                                    meta['dba_name']=parse_get3.xpath('//table[1]//tr[1]/td/table[3]//tr[1]/td[1]/font/strong[contains(text(),"Owner or Operator")]/following::td/text()').extract_first()
                                    meta['violation_date']=meta['rule']=meta['rule_id']=meta['abate_date']=meta['abate_status']=meta['violation_category']=meta['violation_subtype']=meta['violation_description']=meta['violation_type']=''
                                    yield self.save_to_csv(response,**meta)
                                    meta['dba_name']=''
                                    dba_namee = self._getDBA(company_name)[1]
                                    if dba_namee and len(dba_namee) > 3:
                                        meta['dba_name']=dba_namee
                                        yield self.save_to_csv(response,**meta)


                            if check==0:
                                break


        page_val = response.meta['page']
        next_pagee = response.xpath('//table//tr/td/a[@class="buttN"]/b[contains(text(), "'+(str(page_val))+'")]/following::a/@href').extract_first()
        print(next_pagee, 'next_pagee@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@',page_val)
        if next_pagee and 'start=' in next_pagee:
            main_url='http://il.healthinspections.us/kankakee/'+str(next_pagee)
            yield scrapy.Request(url=main_url, callback=self.parse_get1, dont_filter=True,meta={'page':int(response.meta['page'])+1})
        if len(self.slink) > 0:
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse_get, dont_filter=True)

    def data_clean(self, value):
        if value:
            try:
                clean_tags = re.compile('<.*?>')
                desc_list = re.sub('\s+', ' ', re.sub(clean_tags, ' ', value))
                desc_list_rep = desc_list.replace('&amp;', '&')
                return desc_list_rep.strip()
            except:
                return ''
        else:
            return ''


    def save_to_csv(self, response,**meta):
        il = ItemLoader(item=IlKankakeeFoodInspectionsSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'IL_Kankakee_Food_Inspections')
        il.add_value('url', 'http://www.kankakeehealth.org/environmental-health/food-sanitation/food_inspections.html')
        il.add_value('violation_date', meta['violation_date'])
        il.add_value('permit_lic_no', meta['permit_lic_no'])
        il.add_value('location_address_string', meta['location_address_string'])
        il.add_value('inspector_comments', meta['inspector_comments'])
        il.add_value('inspection_date', meta['inspection_date'])
        il.add_value('company_name',meta['company_name'])
        il.add_value('violation_rule_id', meta['rule_id'])
        il.add_value('violation_subtype', meta['violation_subtype'])
        il.add_value('inspection_pass_fail', meta['inspection_pass_fail'])
        il.add_value('violation category', meta['violation_category'])
        il.add_value('dba_name', meta['dba_name'])
        il.add_value('inspection_type', meta['inspection_type'])
        il.add_value('violation_description', meta['violation_description'])
        il.add_value('risk category', meta['risk'])
        il.add_value('abate_date', meta['abate_date'])
        il.add_value('abate_status', meta['abate_status'])
        il.add_value('temperature observations-item/location', meta['temperature'])
        il.add_value('inspection_subtype', meta['inspection_subtype'])
        il.add_value('violation_rule', meta['rule'])
        il.add_value('permit_lic_desc', meta['permit_lic_desc'])
        il.add_value('permit_type', 'restaurant_license')
        il.add_value('violation_type', meta['violation_type'])
        return il.load_item()