import pandas as pd
import numpy as np
import calendar

def update_same_period_consumption_lists(timestamp, last_timestamp, data, consumption_same_period_dict):
    period_changed_dict = {period: None for period in consumption_same_period_dict.keys()}
    for period in consumption_same_period_dict.keys():
        if period == 'H':
            if timestamp.hour == last_timestamp.hour:
                consumption_same_period_dict['H'].append(data)
                new_hour = False
            else:
                new_hour = True
            period_changed_dict['H'] = new_hour
    
        if period == 'D':
            if timestamp.date() == last_timestamp.date():
                consumption_same_period_dict['D'].append(data)
                new_day = False
            else:
                new_day = True
            period_changed_dict['D'] = new_day
    
        if period == 'W':
            if timestamp.week == last_timestamp.week and timestamp.year == last_timestamp.year:
                consumption_same_period_dict['W'].append(data)
                new_week = False
            else:
                new_week = True
            period_changed_dict['W'] = new_week

        if period == 'M':   
            if timestamp.month == last_timestamp.month and timestamp.year == last_timestamp.year:
                consumption_same_period_dict['M'].append(data)
                new_month = False
            else:
                new_month = True
            period_changed_dict['M'] = new_month
    
    return [period_changed_dict, consumption_same_period_dict]


def update_period_consumption_dict(period_changed_dict, consumption_same_period_dict, overall_period_consumption_dict):
    for period in consumption_same_period_dict.keys():
        if period_changed_dict[period]:
            consumption_same_period_dict[period].sort(key= lambda data: todatetime(data['Time']))        
            consumption_max = float(consumption_same_period_dict[period][-1]['Consumption'])
            consumption_min = float(consumption_same_period_dict[period][0]['Consumption'])
            overall_period_consumption = consumption_max-consumption_min

            if not np.isnan(overall_period_consumption):
                period_key = todatetime(consumption_same_period_dict[period][-1]['Time']).tz_localize(None).floor('h')
                num_days_in_month = calendar.monthrange(period_key.year, period_key.month)[1]
        
                if period == 'H':
                    period_key = period_key
                elif period == 'D':
                    period_key = period_key.floor('d')
                elif period == 'W':
                    period_key = period_key.floor('d') + (6-period_key.dayofweek)*pd.Timedelta(1,'d')
                elif period == 'M':
                    period_key = period_key.floor('d') + (num_days_in_month-period_key.day)*pd.Timedelta(1,'d')
                
                overall_period_consumption_dict[period][period_key] = overall_period_consumption
    return overall_period_consumption_dict


def todatetime(timestamp):
        if str(timestamp).isdigit():
            if len(str(timestamp))==13:
                return pd.to_datetime(int(timestamp), unit='ms')
            elif len(str(timestamp))==19:
                return pd.to_datetime(int(timestamp), unit='ns')
        else:
            return pd.to_datetime(timestamp)