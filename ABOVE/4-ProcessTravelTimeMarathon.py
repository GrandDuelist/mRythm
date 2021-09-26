# -*- coding: utf-8 -*-
#%% 
import pandas as pd
data = pd.read_csv("result/1-traveltime-years/0-year-average-delay.csv")
year_average_delay = data
#%%
year_average_delay = data[(data['Average Delay']>= 0) & (data['Average Delay']<=2)]
marathon_2015 = year_average_delay[(year_average_delay['Year']==2015) & (year_average_delay['Month']==1) & (year_average_delay['Day']==1)]
#marathon_2015 = year_average_delay[(year_average_delay['Year']==2015) & (year_average_delay['Month']==10) & (year_average_delay['Day']==28)]
#spring_festival = year_average_delay[(year_average_delay['Year']==2015) & (year_average_delay['Month']==1) & (year_average_delay['Day']==1)]

no_marathon_2015 =  year_average_delay[(year_average_delay['Year']==2015) & (year_average_delay['Month']==1) & (year_average_delay['Day']==2)]
marathon_2015["Marathon"] = pd.Series(["Marathon"]*len(marathon_2015))
no_marathon_2015["Marathon"] = pd.Series(["General"] * len(no_marathon_2015))
marathon_data = pd.concat([marathon_2015,no_marathon_2015])

#%% calculate average delay -- marathon
marathon_hour_delay = marathon_data.groupby(['Marathon','Time'])['Average Delay'].mean()
marathon_hour_delay = marathon_hour_delay.reset_index()
marathon_hour_delay.to_csv("result/1-traveltime-socialevent/1-marathon-hour-delay.csv")

