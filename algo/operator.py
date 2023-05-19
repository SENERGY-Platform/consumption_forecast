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
from .aggregate import aggregate
import pandas as pd
import numpy as np
import os
import pickle
import abc
from collections import deque

from util.logger import logger

class Operator(util.OperatorBase):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        data_path = config.data_path
        if not os.path.exists(data_path):
            os.mkdir(data_path)

        self.period = config.time_period

        self.day_consumption_dict = {}
        self.consumption_same_day = []
        self.timestamp = None
        self.prediction_length = int(config.prediction_length)

        self.predicted_values = []

        self.first_data_time = None

        self.num_days_coll_data = 30

        self.day_consumption_dict_file_path = f'{data_path}/day_consumption_dict.pickle'
        self.predicted_values_file = f'{data_path}/predicted_values.pickle'

        if os.path.exists(self.day_consumption_dict_file_path):
            with open(self.day_consumption_dict_file_path, 'rb') as f:
                self.day_consumption_dict = pickle.load(f)
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

    def update_day_consumption_dict(self):
        min_index = np.argmin([float(datapoint['Consumption']) for datapoint in self.consumption_same_day])
        max_index = np.argmax([float(datapoint['Consumption']) for datapoint in self.consumption_same_day])
        day_consumption_max = float(self.consumption_same_day[max_index]['Consumption'])
        day_consumption_min = float(self.consumption_same_day[min_index]['Consumption'])
        overall_day_consumption = day_consumption_max-day_consumption_min
        if np.isnan(overall_day_consumption)==False:
            self.day_consumption_dict[self.todatetime(self.consumption_same_day[-1]['Time']).tz_localize(None).floor('d')] = overall_day_consumption
        with open(self.day_consumption_dict_file_path, 'wb') as f:
            pickle.dump(self.day_consumption_dict, f)
        return
    
    def run(self, data, selector='energy_func'):
        self.timestamp = self.todatetime(data['Time']).tz_localize(None)
        if pd.Timestamp.now().tz_localize(None)-self.timestamp >= 8*pd.Timedelta(30,'days'):
            return
        logger.info('energy: '+str(data['Consumption'])+'  '+'time: '+str(self.timestamp))
        if self.consumption_same_day == []:
            self.consumption_same_day.append(data)
            self.first_data_time = self.timestamp
        else:
            if self.timestamp.date() == self.todatetime(self.consumption_same_day[-1]['Time']).tz_localize(None).date():
                self.consumption_same_day.append(data)
            else:
                self.update_day_consumption_dict()
                if self.timestamp-self.first_data_time >= pd.Timedelta(self.num_days_coll_data,'d'):
                    time_series_data_frame = pd.DataFrame.from_dict(self.day_consumption_dict, orient='index', columns=['daily_consumption'])
                    time_series = aggregate(self.period, time_series_data_frame)
                    self.consumption_same_day = [data]
                    if len(time_series) >= 3:
                        self.fit(time_series)
                        predicted_value = self.predict(self.prediction_length).first_value()
                        logger.info(f"Prediction: {predicted_value}")
                        self.predicted_values.append((self.timestamp, predicted_value))
                        with open(self.predicted_values_file, 'wb') as f:
                            pickle.dump(self.predicted_values,f)
                        return {'value': predicted_value, 'timestamp': self.timestamp.strftime('%Y-%m-%d %X')}
                    else:
                        return
                self.consumption_same_day = [data]

    @abc.abstractmethod
    def fit(train_time_series):
        """To be implemented """
        return 

    @abc.abstractmethod
    def predict(number_of_steps):
        """To be implemented """
        return 