import pandas as pd
import darts

def aggregate(period, data_frame):
    if period=='day':
        dayly_time_series = darts.TimeSeries.from_dataframe(data_frame, freq='D')
        return dayly_time_series
    if period=='week':
        data_frame['week'] = pd.Series(data_frame.index, index=data_frame.index).dt.isocalendar().week  # An extra column is added to the data frame that contains the data points' numbers of the week 
        data_frame['week'] = (pd.DataFrame(data_frame.index.year, index = data_frame.index, columns=['year'])['year'].apply(lambda x:str(x)+"W")+
                                           data_frame['week'].apply(lambda x: str(x).zfill(2)))
        weekly_grouper = data_frame.groupby(['week'])
        for group in weekly_grouper:
            print(group)
        weekly_data_frame = pd.DataFrame([group[1]['daily_consumption'].sum() for group in weekly_grouper if (group[1].index[-1].weekday()==6 and len(group)==7)], index = [group[1].index[-1] for group in weekly_grouper if (group[1].index[-1].weekday()==6 and len(group)==7)])
        weekly_time_series = darts.TimeSeries.from_dataframe(weekly_data_frame, freq='W')
        return weekly_time_series
    if period=='month':
        data_frame['month'] = data_frame.index.month  # An extra column is added to the data frame that contains the data points' numbers of the month 
        data_frame['month'] = pd.DataFrame(data_frame.index.year, index = data_frame.index)[0].apply(lambda x:str(x)+"M")+data_frame['month'].apply(lambda x: str(x).zfill(2))
        monthly_grouper = data_frame.groupby(['month'])
        for group in monthly_grouper:
            print(group)
        monthly_data_frame = pd.DataFrame([group[1]['daily_consumption'].sum() for group in monthly_grouper if (group[1].index[-1].is_month_end and len(group) >= 25)], index = [group[1].index[-1] for group in monthly_grouper (group[1].index[-1].is_month_end and len(group) >= 25)])
        monthly_time_series = darts.TimeSeries.from_dataframe(monthly_data_frame, freq='M')
        return monthly_time_series