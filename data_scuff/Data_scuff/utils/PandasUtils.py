'''
Created on 12-May-2018

@author: srinivasan
'''
import pandas as pd


class PandasUtils:
    
    """
    solve the UnicodeEncodeError in data frame
    """

    @staticmethod
    def clean_str_cols(df, encoding='ascii'):
        df = df.copy()
        for col, dtype in df.dtypes.items():
            if dtype.kind == 'O':  
                df[col] = df[col].astype('str')
                df[col] = df[col].str.encode(encoding, errors='ignore').str.decode(encoding)
        return df 
    
    """
    Trim white space in left and right
    """

    @staticmethod
    def trim_df(df):
        df_obj = df.select_dtypes(['object'])
        df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
        return df
    
    """
    Clean the column name from dataFrame
    """

    @staticmethod
    def clean_col_names(df):
        df.rename(index=str, columns={c:''.join(e for e in c if e.isalnum()) for c in df.columns}, inplace=True)
        return df
    
    """
    drop the following columns from data frame
    """

    @staticmethod
    def drop_cols(df, columns):
        df.drop(columns, inplace=True, axis=1)
        return df
    
    @staticmethod
    def readExcel(path, multiplesheet=True):
        dfs = pd.read_excel(path, sheetname=None)
        if not multiplesheet:
            return dfs[0]
        return dfs
    
    @staticmethod
    def convertlistDictToDf(val):
        return pd.DataFrame(val)
