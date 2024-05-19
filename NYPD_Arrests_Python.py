#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import json

nyc_arrests = pd.DataFrame()
temp_df = pd.DataFrame()

# 'GET' request
for offset in range(0, 230000, 50000):
    url = 'https://data.cityofnewyork.us/resource/uip8-fykc.json?$limit=50000&$offset='+str(offset)
    response = requests.get(url)
    data = response.json()
    temp_df = pd.DataFrame()
    temp_df = pd.json_normalize(data)
    
    nyc_arrests = nyc_arrests._append(temp_df, ignore_index=True)

# check number of rows and columns in the dataframe
nyc_arrests.shape


# In[2]:


nyc_arrests.tail(5)


# In[3]:


# print column names
print(nyc_arrests.columns.tolist())
nyc_arrests['geocoded_column.type'].unique()


# In[4]:


# Drop redundant columns from the dataframe
nyc_arrests.drop(nyc_arrests.loc[:, ':@computed_region_f5dn_yrer':'geocoded_column.coordinates'].columns, inplace=True, axis=1)
print(nyc_arrests.columns.tolist())


# In[5]:


# return indices for empty entries/values
nyc_arrests[nyc_arrests['arrest_key'] == ''].index

# check for NULL values
import numpy as np
np.where(pd.isnull(nyc_arrests['arrest_key']))

# check if all the values are numeric
pd.to_numeric(nyc_arrests['arrest_key'], errors='coerce').notnull().all()

# check if all the values are 9 digits long
nyc_arrests[nyc_arrests['arrest_key'].apply(lambda x: len(str(x)) != 9)]


# In[6]:


# check for NULL values
import numpy as np
np.where(pd.isnull(nyc_arrests['arrest_date']))

# check if all values are datetime
nyc_arrests['arrest_date'].astype(str).apply(lambda x: pd.to_datetime(x, errors='coerce')).notna().all()


# In[7]:


# check if all the values are numeric
pd.to_numeric(nyc_arrests['pd_cd'], errors='coerce').notnull().all()

# find unique values
nyc_arrests['pd_cd'].unique()

# find number of rows with 'nan' entries 
nyc_arrests['pd_cd'].isnull().sum()

# check if all the values are 3 digits long
temp = nyc_arrests[nyc_arrests['pd_cd'].apply(lambda x: len(str(x)) != 3)]

temp['pd_cd'].value_counts()


# In[8]:


# find unique values
nyc_arrests['pd_desc'].unique()


# In[9]:


# check if all the values are numeric
pd.to_numeric(nyc_arrests['ky_cd'], errors='coerce').notnull().all()

# find unique values
nyc_arrests['ky_cd'].unique()
nyc_arrests['ky_cd'].isnull().sum()
nyc_arrests.loc[nyc_arrests['ky_cd'].isnull()]

# check if all the values are 3 digits long
nyc_arrests[nyc_arrests['ky_cd'].apply(lambda x: len(str(x)) != 3)]


# In[10]:


# find unique values
nyc_arrests['ofns_desc'].unique()


# In[11]:


# find unique values
nyc_arrests['law_cat_cd'].unique()
nyc_arrests.loc[nyc_arrests['law_cat_cd'] == '9']
nyc_arrests.loc[nyc_arrests['law_cat_cd'] == 'I']


# In[12]:


# find unique values
nyc_arrests['arrest_boro'].unique()


# In[44]:


# check if all the values are numeric
pd.to_numeric(nyc_arrests['arrest_precinct'], errors='coerce').notnull().all()


# In[45]:


# check if all the values are numeric
pd.to_numeric(nyc_arrests['jurisdiction_code'], errors='coerce').notnull().all()


# In[46]:


# find unique values
nyc_arrests['age_group'].unique()


# In[47]:


# find unique values
nyc_arrests['perp_sex'].unique()


# In[48]:


# find unique values
nyc_arrests['perp_race'].unique()


# In[16]:


# calculate min & max on 'latitude'
print(nyc_arrests['latitude'].agg(['min', 'max']))

# calculate min & max on 'longitude'
print(nyc_arrests['longitude'].agg(['min', 'max']))

# locate rows with longitude value equal to 0
nyc_arrests.loc[nyc_arrests['longitude'] == '0.0']

#locate rows with latitude value equal to 0
nyc_arrests.loc[nyc_arrests['latitude'] == '0.0']

temp=nyc_arrests.drop([41238,40573])

print(temp['longitude'].agg(['min', 'max']))
print(temp['latitude'].agg(['min', 'max']))


# In[13]:


#load the data into sql server using replace option
get_ipython().system('pip install pyodbc')
import sqlalchemy as sal
engine=sal.create_engine('mssql://RahityaGovindu/master?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
conn=engine.connect()


# In[14]:


#load data into sql using append option
nyc_arrests.to_sql('nyc_arrests_sql',con=conn,index=False,if_exists='append')

