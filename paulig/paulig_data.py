# -*- coding: utf-8 -*-
"""
Created on Sat Nov  7 11:31:22 2020

@author: Mikko
"""
import snowflake.connector
import pandas as pd

con = snowflake.connector.connect(
    user='dev_edw_junction_team_04',
    password='9oCVtEJzhkkFPh2GRtWH66nKVYX4ZR6K',
    account='paulig.west-europe.azure'
)

cur = con.cursor()

sql = """
select
*
from "DEV_EDW_JUNCTION"."JUNCTION_2020"."CAFE_POS_DATA"
where header_total >= 0
"""

cur.execute(sql)

df = cur.fetch_pandas_all()

df.to_csv('snowflake_data.csv', index=False)

#%%
sql2 = """
select
*
from "DEV_EDW_JUNCTION"."JUNCTION_2020"."WEBSHOP_DATA"
"""

cur.execute(sql2)

webshop = cur.fetch_pandas_all()

webshop.to_csv('webshop_data.csv', index=False)


#%% download weather data

from wwo_hist import retrieve_hist_data


frequency = 1
start_date = '2020-07-17'
end_date = '2020-11-07'
api_key = '3c3b738e215542218d4102520200711'
location_list = ['finland']
hist_weather_data = retrieve_hist_data(api_key,
                                location_list,
                                start_date,
                                end_date,
                                frequency,
                                location_label = False,
                                export_csv = True,
                                store_df = True)

hist_weather_data[0]

hist_weather_data[0].to_csv('weather_data.csv', index=False)


#%% paulig data distinct

sql3 = """
select distinct
HEADER_BOOKINGDATE,
HEADER_JOURNALTIME,
HEADER_TERMINAL,
HEADER_CASHIER,
HEADER_TOTAL
from "DEV_EDW_JUNCTION"."JUNCTION_2020"."CAFE_POS_DATA"
where header_total >= 0
"""

cur.execute(sql3)

asd = cur.fetch_pandas_all()

df.to_csv('snowflake_data_filtered.csv', index=False)