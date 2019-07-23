'''
Created on 17-Feb-2019

@author: srinivasan
'''
# Import pandas 
"""import pandas as pd 

df = pd.read_csv(
    "/home/srinivasan/eclipse-workspace/data_scuff/responseTracking/AI_813_2/trackingRequest_20190217.csv",
    sep='|')
df["status"].fillna(0.0, inplace=True)
request = df.loc[df['type'] == 'request'].reset_index(drop=True)[['unique_id', ]]
response = df.loc[df['type'] == 'response'].reset_index(drop=True)[['unique_id', 'status']]
df = pd.merge(request, response, how='left', on=['unique_id'])
df["status"].fillna(0.0, inplace=True)
res = df.groupby(['status']).size().reset_index(name='counts')
print(res)"""
