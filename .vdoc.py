# type: ignore
# flake8: noqa
#
#
#
#
#
#
#
#
#
#
#
#
#| label: load-packages_cw
import pandas as pd
import requests
import io
from itables import show
import datetime
#
#
#
#| label: load-data-cw
df_crosswalk = pd.read_csv("data/pw_log_crosswalk.csv")
df = pd.read_csv("data/df_pw.csv")

#
#
#
#| label: prep-usage-data

# Get Year and Month from date
# df = df.set_index('dataset_id')
df['all_requests'] = df['requests'] + df['requests_redirect']
df['time'] = pd.to_datetime(df['time'], utc=True)
df['year'] = df['time'].dt.year
df['month'] = df['time'].dt.month
```
#
# This year
thisyear = datetime.date.today().year

# Get all data
#alldata = df.loc[df.index == 'all_datasets']
alldata = df[df['dataset_id'] == 'all_datasets']
# alldata_thisyear = alldata[alldata['year'] == thisyear]

# Columns for stats
# Extract file types by removing '_reqs' from the column names
stats_cols  = ['data_volume', 'requests', 'nc_req', 'dods_req', 'text_req',
       'metadata_req', 'graph_req', 'json_req', 'mat_req', 'images_req',
       'file_downloads_req', 'other_req', 'nc_volume', 'dods_volume',
       'text_volume', 'metadata_volume', 'graph_volume', 'json_volume',
       'mat_volume', 'images_volume', 'file_downloads_volume', 'other_size',
       'requests_redirect', 'nc_req_redirect', 'dods_req_redirect',
       'text_req_redirect', 'metadata_req_redirect', 'graph_req_redirect',
       'json_req_redirect', 'mat_req_redirect', 'images_req_redirect',
       'file_download_req_redirect', 'other_req_redirect', 'all_requests']


# Compute sum for all datasets
alldata_sum = alldata.groupby(['year'])[stats_cols].sum().reset_index()

# Compute mean stats for all dataset
alldata_mean = alldata.groupby(['year'])[['unique_visitors']].mean().reset_index()



# ValueBox Data
total_requests = alldata_sum.loc[alldata_sum['year'] == thisyear, 'requests'].iloc[0]
total_redirect = alldata_sum.loc[alldata_sum['year'] == thisyear, 'requests_redirect'].iloc[0]
unique_users = int(alldata_mean.loc[alldata_mean['year'] == thisyear, 'unique_visitors'].iloc[0])
total_data = alldata_sum.loc[alldata_sum['year'] == thisyear, 'data_volume'].iloc[0]


#
#
#
# This year
thisyear = datetime.date.today().year

# Color 
colors = ['#003f5c', '#58508d', '#bc5090', '#ff6361', '#ffa600']
# Get all data
#alldata = df.loc[df.index == 'all_datasets']
alldata = df[df['dataset_id'] == 'all_datasets']
# alldata_thisyear = alldata[alldata['year'] == thisyear]


# Compute sum for all datasets
alldata_sum = alldata.groupby(['year'])[stats_cols].sum().reset_index()

# Compute mean stats for all dataset
alldata_mean = alldata.groupby(['year'])[['unique_visitors']].mean().reset_index()


# ValueBox Data
total_requests = alldata_sum.loc[alldata_sum['year'] == thisyear, 'requests'].item()
total_redirect = alldata_sum.loc[alldata_sum['year'] == thisyear, 'requests_redirect'].item()
unique_users = int(alldata_mean.loc[alldata_mean['year'] == thisyear, 'unique_visitors'].item())
total_data = alldata_sum.loc[alldata_sum['year'] == thisyear, 'data_volume'].item()


# Top 10

df_all_thisyear = df[df['year'] == thisyear].merge(df_crosswalk, on='dataset_id', how='inner')
top10_thisyear = (df_all_thisyear.groupby('group_title', as_index=False)[['all_requests', 'requests', 'requests_redirect']]
                  .sum(numeric_only=True)
                  .nlargest(10, 'all_requests'))

# Top 10 Time series

df_all = df.merge(df_crosswalk, on='dataset_id', how='inner')

top10_years = (df_all[df_all['group_title'].isin(top10_thisyear['group_title'])]
              .groupby(['group_title', 'year'], as_index=False)[['all_requests']]
              .sum())
              
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| title: Top 10 Data Chart

import plotly.express as px

top10_years['year'] = top10_years['year'].astype(str)
top10_years = top10_years.sort_values(by='year')

#sorted = top10_years.sort_values(by=['year', 'all_requests'], ascending=False)

fig = px.bar(top10_years, x='group_title', y='all_requests', 
              color='year', labels={"group_title": "Group Name", "all_requests": "Requests"}, color_discrete_sequence=colors[::-1],barmode='group', width=None)
#fig.update_traces(textposition="top center")
fig.update_layout(autosize=True, 
       width=None, 
       height=None, 
       xaxis=dict(tickangle=15),
       legend=dict(
       orientation="h",  # Horizontal orientation for label
       yanchor="bottom",
       y=1.1,            # Position above plot (relation to 1)
       xanchor="center",
       x=0.5,
       font=dict(size=15))
              )


#
#
#
#| title: Top 10 Data Table

wide_top10_years = top10_years.pivot(index='group_title', columns ='year', values='all_requests').reset_index()
wide_top10_years.columns.name = None  # Remove the name from columns index if not needed
wide_top10_years = (wide_top10_years
              .rename_axis(None, axis=1)
              .rename(columns={'group_title': 'Group Name'}))


show(wide_top10_years)

#
#
#
#
#
#
#| title: Requests by File Type


import plotly.express as px
df_req = alldata_sum[alldata_sum['year'] == thisyear].filter(regex='_req$')
df_req.columns = df_req.columns.str.extract(r'^(.*)_req$')[0]
df_req_long = df_req.reset_index(drop=True).melt(var_name='File Type', value_name='Requests')

fig = px.pie(df_req_long, values='Requests', names='File Type', title='Request by file type')
fig.show()


#
#
#
#
#| title: Download by File Type

import plotly.express as px
df_volume= alldata_sum[alldata_sum['year'] == thisyear].filter(regex='_volume$')
df_volume.columns = df_volume.columns.str.extract(r'^(.*)_volume$')[0]
df_volume_long = df_volume.reset_index(drop=True).melt(var_name='File Type', value_name='Volume')
fig = px.pie(df_volume_long, values='Volume', names='File Type')

fig


#
#
#
#
#
#
#

 # top10 Ice
top10_ice_thisyear = (df_all_thisyear[df_all_thisyear['var']=='ice'].groupby('group_title', as_index=False)[['all_requests', 'requests', 'requests_redirect']]
                  .sum(numeric_only=True)
                  .nlargest(10, 'all_requests'))


# ice sumed by year and dataset_title
df_ice = df_all[df_all['var'] == 'ice'].groupby(['year', 'dataset_title'], as_index=False).sum(numeric_only=True).reset_index()
top10_ice = df_ice[df_ice['year'] == thisyear].nlargest(10, 'all_requests')
ice_stats = df_ice[df_ice['year'] == thisyear].sum(numeric_only=True)


# value box
req_ice = ice_stats[ice_stats.index == 'all_requests'].item()
user_ice = ice_stats[ice_stats.index == 'unique_visitors'].item()
data_ice = ice_stats[ice_stats.index == 'data_volume'].item()

# top10_ice chart
top10_ice_years = (df_all[df_all['dataset_title'].isin(top10_ice['dataset_title'])]
              .groupby(['dataset_title', 'year'], as_index=False)[['all_requests']]
              .sum())

# top10_ice data table
#top10_ice.loc[:, ['year', 'dataset_title', 'all_requests']]
top10_ice_wide = top10_ice_years.pivot(index='dataset_title', columns ='year', values='all_requests').reset_index()
top10_ice_wide.columns.name = None
top10_ice_wide = (top10_ice_wide
              .rename_axis(None, axis=1)
              .rename(columns={'dataset_title': 'Dataset Title'}))


#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#| title: Top 10 ice

import plotly.express as px

# top10_ice chart
top10_ice_years['year'] = top10_years['year'].astype(str)
#sorted = top10_sst_years.sort_values(by=['year', 'all_requests'], ascending=False)


fig = px.bar(top10_ice_years, x='dataset_title', y='all_requests', 
              color='year', labels={"dataset_title": "Dataset Name", "all_requests": "Requests"}, color_discrete_sequence=colors[::-1],barmode='group', width=None)

fig.update_layout(autosize=True, 
       width=None, 
       height=None, 
       xaxis=dict(tickangle=15),
       legend=dict(
       orientation="h",  # Horizontal orientation
       yanchor="bottom",
       y=1.1,            # Position slightly above the plot area
       xanchor="center",
       x=0.5,
       font=dict(size=13))
              )
# show(top10_years)
#
#
#
#| title: Top 10 Data Table

show(top10_ice_wide)

#
#
#
#
#
#
