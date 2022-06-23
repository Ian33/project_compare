# -*- coding: utf-8 -*-
"""
Created on Tue Apr  6 17:30:24 2021

@author: IHiggins
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 13:45:12 2020

@author: IHiggins
"""

import pyodbc
import pandas as pd
import fpdf

from datetime import timedelta
import plotly.express as px
import plotly.io
import plotly.graph_objects as go
import configparser
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from PyPDF2 import PdfFileReader, PdfFileMerger
#from PyPDF2 import PdfFileMerger, PdfFileReader
from PyPDF2 import PdfFileMerger

#from pdfrw import PdfReader, PdfWriter
#import PyPDF2


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=KCITSQLPRNRPX01;'
                      'Database=gData;'
                      'Trusted_Connection=yes;')


config = configparser.ConfigParser()
config.read('gdata_config.ini')

# production NRDOSQLPrX01
#DEV KCITSQLDEVNRP01


## Time Range
# Start Time
start_time = '01-10-2021'
start_time = pd.to_datetime(start_time) + timedelta(hours=7)

# End Time
end_time = '01-05-2022'
end_time = pd.to_datetime(end_time)+ timedelta(hours=7)

data_units = "water level (wl feet)"
# data type name is for configuration file
# water_temperature
# Conductivity
#data_type_name = "discharge"
data_type_name = "water_level"
#RPWS sites
#site_sql_id_list = 1544,1545,1546,1547,1548,1550,1551,1552,1553,1554,1555,1692,1693,1707,1708,1709,1710
#safc sites
site_sql_id_list = 1412, 1469, 1470, 1471, 1472, 1473, 1513, 1543, 1638, 1639, 1640, 1641, 1642, 1643, 1654, 1655, 1656, 1657, 1658, 1659, 1660, 1661, 1662, 1663, 1664, 1667, 1668, 1719, 1886, 1926, 1927, 1964, 1965, 1966, 2103, 2104


#site_sql_id_list = 1693, 1692
run_average = "True"
#as start_time and end_time are defined before fx you dont have to send them to fx...i think

def select_data_type(site_sql_number, data_type_name):
    data_type = config[data_type_name]["corrected_data"]
    return data_type

def select_datetime(site_sql_number, data_type_name):
    datetime = config[data_type_name]["datetime"]
    return datetime

def select_table(site_sql_number, data_type_name):
    table = config[data_type_name]["table"]
    return table

def get_site_id(site_sql_number):
    # You could use tblGaugeLLID to find out if a site is a lakelevel well or ect, but this does take some processing power I dont want to commit to right now
    #Gage_Lookup = pd.read_sql_query('select G_ID, SITE_CODE, SITE_NAME, GAGETAG from tblGaugeLLID WHERE G_ID = '+str(SITE_SQL_NUMBER)+';',conn)
    site_id_lookup = pd.read_sql_query('select SITE_CODE from tblGaugeLLID WHERE G_ID = '+str(site_sql_number)+';',conn)
    site_id = site_id_lookup.to_numpy().reshape(-1)[0]
    return str(site_id) 

def site_query(site_sql_number, data_type, datetime, table, site_id):
    site_df = pd.read_sql_query('select '+str(datetime)+', '+str(data_type)+' from '+str(table)+' WHERE G_ID = '+str(site_sql_number)+' AND '+str(datetime)+' between ? and ?',conn, params=[str(start_time), str(end_time)])
    site_df[str(datetime)] = pd.to_datetime(site_df[str(datetime)]) - timedelta(hours=7)
    site_df.rename(columns={ site_df.columns[0]: "datetime" }, inplace = True)
    site_df.rename(columns={ site_df.columns[1]: site_id }, inplace = True)
    
    return site_df


   
count = 1
# gets sql number from site_sql_id_list and uses list to drive function
for site_sql_number in site_sql_id_list:
    if data_type_name == "water_level":
        try:
            data_type_name = "water_level"
            # define variables for sql query
            # print_function(site_sql_number)
            data_type = select_data_type(site_sql_number, data_type_name)
            datetime = select_datetime(site_sql_number, data_type_name)
            table = select_table(site_sql_number, data_type_name)
            site_id = get_site_id(site_sql_number)
            # run the sql query
            if count == 1:    
                site = site_query(site_sql_number, data_type, datetime, table, site_id)
            else:
                site_x = site_query(site_sql_number, data_type, datetime, table, site_id)
                site = pd.merge(site, site_x, on='datetime', how='outer')
            count = count+1
        except:
            data_type_name = 'groundwater_level'
            # define variables for sql query
            # print_function(site_sql_number)
            data_type = select_data_type(site_sql_number, data_type_name)
            datetime = select_datetime(site_sql_number, data_type_name)
            table = select_table(site_sql_number, data_type_name)
            site_id = get_site_id(site_sql_number)
            # run the sql query
            if count == 1:    
                site = site_query(site_sql_number, data_type, datetime, table, site_id)
            else:
                site_x = site_query(site_sql_number, data_type, datetime, table, site_id)
                site = pd.merge(site, site_x, on='datetime', how='outer')
            count = count+1
    else:
        data_type_name = data_type_name
        # define variables for sql query
        # print_function(site_sql_number)
        data_type = select_data_type(site_sql_number, data_type_name)
        datetime = select_datetime(site_sql_number, data_type_name)
        table = select_table(site_sql_number, data_type_name)
        site_id = get_site_id(site_sql_number)
        # run the sql query
        if count == 1:    
            site = site_query(site_sql_number, data_type, datetime, table, site_id)
        else:
            site_x = site_query(site_sql_number, data_type, datetime, table, site_id)
            site = pd.merge(site, site_x, on='datetime', how='outer')
        count = count+1

#### reformat dataframe ####
site.sort_values(by='datetime', inplace=True)
site.set_index('datetime', inplace=True)
# reorder
site = site.reindex(sorted(site.columns), axis=1)
#### save file ####
site.to_csv(r"W:\STS\hydro\GAUGE\Temp\Data\\compare.csv")


def average():
    if run_average == "True":
    #for row in site.iterrows():
    #    site['average'] = site.mean(skipna=True, axis=0)
        
    # reset index
        site['average'] = site.mean(skipna=True, axis=1)
    #site.apply(lambda x: site.mean(skipna=True),axis=1)
        #print(row)

average()
print(site)

#### set figure parameters ####
fig_height = 800
fig_width = 1500


# reset index for graph
site.reset_index(inplace=True)

fig = make_subplots(rows=2, cols=1,subplot_titles=(''), horizontal_spacing = 0.02, vertical_spacing = 0.07)
j = 1
for i in site.columns:
    if i == "datetime":
       do_nothing = "do_nothing"
    else:
        fig.append_trace(go.Scatter(
            x=site['datetime'],
            y=site[i],
            name=i), row=1, col=1)
    j += 1
# this figure height seems to make things worse
# fig.update_layout(height=fig_height, width=fig_width, title_text=f"{data_type_name}")
fig.update_layout(height=fig_height, width=fig_width, title_text=f"{data_type_name}")
fig.update_xaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)
fig.update_yaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)

        
fig.update_layout(plot_bgcolor='rgba(0,0,0,0)')
fig.update_xaxes(showgrid=False)
fig.update_yaxes(showgrid=False)
fig.update_xaxes(showticklabels=True)
fig.update_yaxes(showticklabels=True)
fig.update_layout(
    margin=dict(l=10, r=10, t=50, b=20)
)
fig.update_layout(
            xaxis_title="datetime",
            yaxis_title=data_units,
            )
main_graph_path = "W:\STS\hydro\GAUGE\Temp\Data"
main_graph_title = "compare"

# fig.write_image(file=r""+main_graph_path+"\\"+main_graph_title+"_"+data_type_name+".pdf",format='pdf')
plotly.io.write_image(fig,file=r'C:\Users\ihiggins\.spyder-py3\j1.jpeg',format='jpeg')
fig.write_html("W:\STS\hydro\GAUGE\Temp\Data\\compare")

# set up pdf
# orientation: P or L
pdf = fpdf.FPDF(format='letter', orientation = 'L') #pdf format

# add all sites graph
pdf.add_page()
# pdf.image(r'C:\Users\ihiggins\.spyder-py3\j1.jpeg', x=img_x, y=img_y, w=img_width, h=img_height)
pdf.image(r'C:\Users\ihiggins\.spyder-py3\j1.jpeg')

# pdf of sites
def pdf_graph(i):
    
    fig = make_subplots(rows=1, cols=1, specs=[[{"secondary_y": True}]])
    #fig = make_subplots(rows=2, cols=1,subplot_titles=(''), horizontal_spacing = 0.02, vertical_spacing = 0.07)
    fig.add_trace(go.Scatter(
        x=site['datetime'],
            y=site[i],
            name=i), row=1, col=1, secondary_y=False)
    if run_average == "True":
        fig.add_trace(go.Scatter(
            x=site['datetime'],
            y=site['average'],
            name="average", line=go.scatter.Line(color="gray", width=.5)), row=1, col=1, secondary_y=True)
    fig.update_layout(height=fig_height, width=fig_width, title_text=i)
        
    fig.update_xaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)
    fig.update_yaxes(showline=True, linewidth=.5, linecolor='black', mirror=True)

    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)')
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=False)
    fig.update_xaxes(showticklabels=True)
    fig.update_yaxes(showticklabels=True)
    fig.update_layout(
         margin=dict(l=10, r=10, t=50, b=20)
        )

    fig.update_layout(
        xaxis_title="datetime",
        yaxis_title=data_units,
        )

    #fig.write_image(file=r""+main_graph_path+"\\"+i+"_"+data_type_name+".pdf",format='pdf')
    # fig.write_image(file=r'C:\Users\ihiggins\.spyder-py3\j.jpeg',format='pdf')
    
    # map
    # plotly.io.write_image(fig_map,file='pltx_map All.jpeg',format='jpeg')
    
    #fig.show()
    
    #pdf.image(r'C:\Users\ihiggins\.spyder-py3\pltx_map All.jpeg', x=100, y=img_y, w=img_width, h=img_height)
    return fig

for i in site.columns:
    # donnt create a graph of average
    if i != "average":
        if i == "datetime":
           do_nothing = "do_nothing"
        else:
            fig = pdf_graph(i)
            img_width = 315
            img_height = 205
            img_y = 0 # up and down
            img_x = 2 # left to right
            pdf.add_page()
            plotly.io.write_image(fig,file=r"C:\Users\ihiggins\.spyder-py3\\"+i+".jpeg",format='jpeg')
            pdf.image(r"C:\Users\ihiggins\.spyder-py3\\"+i+".jpeg", x=img_x, y=img_y, w=img_width, h=img_height)
            #pdf.image(fig, x=img_x, y=img_y, w=img_width, h=img_height)
            print(i)
           # j = 1

pdf.output(r"W:\STS\hydro\GAUGE\Temp\Ian's Temp\test.pdf")
#webbrowser.open_new("W:\STS\hydro\GAUGE\Temp\Data\\STE.pdf")

