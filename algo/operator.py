"""
   Copyright 2022 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

__all__ = ("Operator", )

import util
from .convert import convert_and_fill_to_timeseries
import pandas as pd
import numpy as np
import os
import pickle
import abc
import calendar

from util.logger import logger

class Operator(util.OperatorBase):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        data_path = config.data_path
        if not os.path.exists(data_path):
            os.mkdir(data_path)

        self.period = config.time_period

        self.period_consumption_dict = {}
        self.consumption_same_period = []
        self.timestamp = None
        self.prediction_length = int(config.prediction_length)

        self.predicted_values = []
        self.min_training_samples = 3

        self.first_data_time = None
        self.period_consumption_dict_file_path = f'{data_path}/period_consumption_dict.pickle'
        self.predicted_values_file = f'{data_path}/predicted_values.pickle'

        if os.path.exists(self.period_consumption_dict_file_path):
            with open(self.period_consumption_dict_file_path, 'rb') as f:
                self.period_consumption_dict = pickle.load(f)
        if os.path.exists(self.predicted_values_file):
            with open(self.predicted_values_file, 'rb') as f:
                self.predicted_values = pickle.load(f)

    def todatetime(self, timestamp):
        if str(timestamp).isdigit():
            if len(str(timestamp))==13:
                return pd.to_datetime(int(timestamp), unit='ms')
            elif len(str(timestamp))==19:
                return pd.to_datetime(int(timestamp), unit='ns')
        else:
            return pd.to_datetime(timestamp)

    def update_period_consumption_dict(self):
        self.consumption_same_period.sort(key= lambda data: self.todatetime(data['Time']))        
        #min_index = np.argmin([float(datapoint['Consumption']) for datapoint in self.consumption_same_period])
        #max_index = np.argmax([float(datapoint['Consumption']) for datapoint in self.consumption_same_period])
        consumption_max = float(self.consumption_same_period[-1]['Consumption'])
        consumption_min = float(self.consumption_same_period[0]['Consumption'])
        overall_period_consumption = consumption_max-consumption_min

        if not np.isnan(overall_period_consumption):
            period_key = self.todatetime(self.consumption_same_period[-1]['Time']).tz_localize(None).floor('d')
            num_days_in_month = calendar.monthrange(period_key.year, period_key.month)[1]
            if self.period == 'week' and period_key.dayofweek < 6:
                period_key = period_key + (6-period_key.dayofweek)*pd.Timedelta(1,'d')
            if self.period == 'month' and period_key.day < num_days_in_month:
                period_key = period_key + (num_days_in_month-period_key.day)*pd.Timedelta(1,'d')
            self.period_consumption_dict[period_key] = overall_period_consumption

        with open(self.period_consumption_dict_file_path, 'wb') as f:
            pickle.dump(self.period_consumption_dict, f)
        return
    
    def run(self, data, selector='energy_func'):
        self.timestamp = self.todatetime(data['Time']).tz_localize(None)
        logger.info('energy: '+str(data['Consumption'])+'  '+'time: '+str(self.timestamp))

        if self.consumption_same_period == []:
            self.consumption_same_period.append(data)
            self.first_data_time = self.timestamp
        else:
            last_timestamp = self.todatetime(self.consumption_same_period[-1]['Time']).tz_localize(None)
            if (self.period == "day" and self.timestamp.date() == last_timestamp.date() \
                or (self.period == "week" and self.timestamp.week == last_timestamp.week and self.timestamp.year == last_timestamp.year)
                or (self.period == "month" and self.timestamp.month == last_timestamp.month and self.timestamp.year == last_timestamp.year)):
                    self.consumption_same_period.append(data)   
            else:
                self.update_period_consumption_dict()
                self.consumption_same_period = [data]
                
                time_series_data_frame = pd.DataFrame.from_dict(self.period_consumption_dict, orient='index', columns=['period_consumption'])
                time_series = convert_and_fill_to_timeseries(self.period, time_series_data_frame)
                
                if len(time_series) >= self.min_training_samples:
                    self.fit(time_series)
                    predicted_value = self.predict(self.prediction_length).first_value()
                    logger.info(f"Prediction: {predicted_value}")
                    self.predicted_values.append((self.timestamp, predicted_value))
                    with open(self.predicted_values_file, 'wb') as f:
                        pickle.dump(self.predicted_values,f)
                    return {'value': predicted_value, 'timestamp': self.timestamp.strftime('%Y-%m-%d %X')}
        
    @abc.abstractmethod
    def fit(train_time_series):
        """To be implemented """
        return 

    @abc.abstractmethod
    def predict(number_of_steps):
        """To be implemented """
        return 