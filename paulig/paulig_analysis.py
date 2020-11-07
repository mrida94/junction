# -*- coding: utf-8 -*-
"""
Created on Sat Nov  7 01:16:02 2020

@author: Mikko
"""


# Imports
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
import numpy as np

df = pd.read_csv('snowflake_data_filtered.csv')
wdata = pd.read_csv('weather_data.csv')

#%% Format df

#Timestamp index
df.index = pd.to_datetime(df['HEADER_BOOKINGDATE'].astype(str) + ' ' + df['HEADER_JOURNALTIME'].astype(str))

# Resample to 1h
df2 = df[['HEADER_TOTAL']].resample('1h').sum()
# df2['HEADER_TOTAL'].plot()

# Remove outliers
df2.HEADER_TOTAL = df2.HEADER_TOTAL.clip(0,df2.HEADER_TOTAL.quantile(0.95))

# Add new fields
df2['hour'] = df2.index.hour
df2['weekday'] = df2.index.weekday
df2['dayname'] = df2.index.day_name()
# df2['day'] = df2.index.day
# df2['week'] = df2.index.week
# df2['month'] = df2.index.month

# Join with weather data
df2 = df2.join(wdata.set_index('date_time'))

# Plot
a = df2.groupby('hour').mean()['HEADER_TOTAL']
# a.plot(kind='bar')

# Keep rows when cafe is open 8-18 (weekends 10-16)
df3 = df2[df2.hour.isin(list(range(8,19)))]
df3 = df3[df3.HEADER_TOTAL > 0]

# Correlation
df3.groupby('hour').mean()['HEADER_TOTAL']

df_corr = df3.corr().reset_index()

#%% Create future data set (NO TARGET FOR THIS DATA)
wdata[wdata.date_time > '2020-10-27 18:00:00']

df_future = wdata[wdata.date_time > '2020-10-27 18:00:00'].set_index('date_time')
df_future.index = pd.to_datetime(df_future.index)

df_future['hour'] = df_future.index.hour
df_future['weekday'] = df_future.index.weekday
df_future['dayname'] = df_future.index.day_name()
#%% Train model

# Select features
features = ['hour', 'weekday', 'windspeedKmph', 'tempC', 'humidity']
target = 'HEADER_TOTAL'


X = df3[features]
y = df3[target]

X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0, shuffle=False, test_size=0.1)

reg = GradientBoostingRegressor(random_state=0)

reg.fit(X_train, y_train)

pred = reg.predict(X_test)

print(reg.score(X_test, y_test))
print(reg.score(X_train, y_train))

#%% Predict historical data

# Train with full dataset
reg.fit(X, y)

# Collect results
results = df3[features]
results['sales'] = y
results['sales_pred'] = reg.predict(X)

# Resample to hour
results2 = results.resample('1h').sum()

# results2.sales = results2.sales.replace(0, np.nan)
# results2.sales_pred = results2.sales_pred.replace(0, np.nan)

# Plot
results2[['sales', 'sales_pred']].plot()

# Join orifinal dataset (df2) with sales and prediction
results3 = df2[features].join(results[['sales', 'sales_pred']])

#%% Predict future data (No target for this data)

# Select features
X_f = df_future[features]

# Collect results
results_future = df_future[features]
results_future['sales'] = np.nan
results_future['sales_pred'] = reg.predict(X_f)
results_future['sales_pred'].loc[~results_future.hour.isin(list(range(8,19)))] = np.nan
results_future['sales_pred'].loc[(~results_future.hour.isin(list(range(10,17)))) & (results_future.weekday.isin([5,6]))] = np.nan

# Combine history and forecast
history_and_forecast = pd.concat([results3,results_future])

history_and_forecast[['sales', 'sales_pred']].plot()

#%% CREATE FAKE INVENTORY
inv_start = 500000
inv = [inv_start]
for n, i in enumerate(history_and_forecast['sales_pred']):

    if np.isnan(i):
        inv.append(inv[n])

    elif inv[n] < 0.3*inv_start:
        inv.append(inv_start)

    else:
        inv.append(inv[n]-i)

inv_percentage = np.array(inv)/inv_start*100

history_and_forecast['inventory_per'] = inv_percentage[0:-1]

history_and_forecast[['sales', 'inventory_per']].plot(subplots=True)

#%% Push data to bq


from google.cloud import bigquery

df_bq = history_and_forecast.reset_index()
df_bq = df_bq.rename(columns = {'index':'timestamp'})
df_bq.to_csv('paulig_model_results.csv', index=False)
# Construct a BigQuery client object.
bq_client = bigquery.Client()
project_id = 'bitcoin-293512'
destination_table = 'junction_demo.restaurant_sales_and_pred_inventory'
table_schema = [{'name': 'timestamp', 'type': 'TIMESTAMP'},
                {'name': 'hour', 'type': 'INT64'},
                {'name': 'weekday', 'type': 'INT64'},
                {'name': 'windspeedKmph', 'type': 'FLOAT'},
                {'name': 'tempC', 'type': 'FLOAT'},
                {'name': 'humidity', 'type': 'FLOAT'},
                {'name': 'sales', 'type': 'FLOAT'},
                {'name': 'sales_pred', 'type': 'FLOAT'},
                {'name': 'inventory_per', 'type': 'FLOAT'}]

min_date = df_bq.timestamp.min().strftime('%Y-%m-%d %H:%M:%S')
max_date = df_bq.timestamp.max().strftime('%Y-%m-%d %H:%M:%S')
# bq_client.query(
# f"""
# DELETE from {project_id}.{destination_table}
# where timestamp between {min_date} and {max_date}
# """
# )
# Save results to BigQuery

# timestamp + Indicators + price
df_bq.to_gbq(destination_table,
             project_id=project_id,
             if_exists='replace',
             table_schema=table_schema
             )

#%%


