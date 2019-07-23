'''
Created on 17-Feb-2019

@author: srinivasan
'''
"""import pandas as pd 

df = pd.read_csv(
    "/home/srinivasan/eclipse-workspace/data_scuff/responseTracking/AI_813_2/trackingRequest_20190218.csv",
    sep='|')
request = df.loc[(df['type'] == 'request') 
                  & (df['callback'] == 'getInspections')
                  ].reset_index(drop=True)
            
            
print(request.shape[0])            
                  
response = df.loc[(df['type'] == 'response') 
                  & (df['callback'] == 'getInspections')
                  ].reset_index(drop=True)[['unique_id', 'status']]
print(response.shape[0])
data = pd.read_csv(
    "/home/srinivasan/eclipse-workspace/data_scuff/storage/AI_813_2/AI-813_2_Permits_Buildings_MN_Anoka_MultiCity_CurationReady_20190218_v1.csv",
    sep='|',
    skiprows=1).drop_duplicates(
        subset=['unique_id'], keep='first')[['unique_id']]
print(data.shape[0])
df = pd.merge(request, data, on=['unique_id'], how="outer", indicator=True)
left = df[df['_merge'] == 'left_only'].reset_index(drop=True)
final_df = pd.merge(left, response, on=['unique_id'], how="left") 
print(final_df.shape[0])"""
