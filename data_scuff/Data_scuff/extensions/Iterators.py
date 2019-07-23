'''
Created on 27-Jun-2018

@author: srinivasan
'''
import logging

import pandas as pd
import io

logger = logging.getLogger(__name__)


def exceliter(response, header, skip_rows, sheet_name, dropna_thresh, isEmpty_lineHeader):
    logger.info("started to reading data from excel file")
    if isEmpty_lineHeader:
        df = pd.read_excel(io.BytesIO(response.body),
                   header=None).replace('\r', ' ',
                    regex=True).replace('***', ' ').dropna(how='all').dropna(thresh=dropna_thresh).fillna('')
        df.columns = df.iloc[0]
        df = df[1:].astype(str)
    else:
        df = pd.read_excel(io.BytesIO(response.body),
                           header=header, skiprows=skip_rows
                           ).dropna(how='all').fillna('')
        df = df.astype(str)
        
    df.rename(columns=lambda x: x.replace('\n', '').replace('\r', ''), inplace=True)
    from collections import OrderedDict
    for row in df.to_dict('records'):
        yield dict(OrderedDict(row))
    

def pcsviter(response, delimiter=None, headers=None, encoding=None, quotechar=None):
    df = pd.read_csv(io.BytesIO(response.body),
                     sep=delimiter, error_bad_lines=False, index_col=False, dtype='str')
    df = df.astype(str)
    from collections import OrderedDict
    for row in df.to_dict('records'):
        yield dict(OrderedDict(row))
