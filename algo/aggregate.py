import pandas as pd
import darts

def aggregate(period, data_frame):
    if period=='day':
        dayly_time_series = darts.TimeSeries.from_dataframe(data_frame, freq='D')
        return dayly_time_series
    if period=='week':
        data_frame['week'] = pd.Series(data_frame.index, index=data_frame.index).dt.isocalendar().week  # An extra column is added to the data frame that contains the data points' numbers of the week 
        weekly_grouper = data_frame.groupby(['week'])
        for group in weekly_grouper:
            print(group)
        weekly_data_frame = pd.DataFrame([group[1]['daily_consumption'].sum() for group in weekly_grouper], index = [group[1].index[-1] for group in weekly_grouper])
        weekly_time_series = darts.TimeSeries.from_dataframe(weekly_data_frame.iloc[1:], freq='W')
        return weekly_time_series
    if period=='month':
        data_frame['month'] = data_frame.index.month  # An extra column is added to the data frame that contains the data points' numbers of the month 
        monthly_grouper = data_frame.groupby(['month'])
        monthly_data_frame = pd.DataFrame([group[1]['daily_consumption'].sum() for group in monthly_grouper], index = [group[1].index[-1] for group in monthly_grouper])
        monthly_time_series = darts.TimeSeries.from_dataframe(monthly_data_frame.iloc[1:], freq='M')
        return monthly_time_series