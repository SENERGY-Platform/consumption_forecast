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
        self.last_two_data_points = deque(maxlen=2)
        self.consumption_same_day = []
        self.timestamp = None
        self.prediction_length = int(config.prediction_length)

        self.first_data_time = None

        self.num_days_coll_data = 10

        self.day_consumption_dict_file_path = f'{data_path}/day_consumption_dict.pickle'

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
            self.day_consumption_dict[self.last_two_data_points[0].floor(freq='d')] = overall_day_consumption
        with open(self.day_consumption_dict_file_path, 'wb') as f:
            pickle.dump(self.day_consumption_dict, f)
        return
    
    def run(self, data, selector='energy_func'):
        self.timestamp = self.todatetime(data['Time']).tz_localize(None)
        logger.info('energy: '+str(data['Consumption'])+'  '+'time: '+str(self.timestamp))
        self.last_two_data_points.append(data)
        if len(self.last_two_data_points)==1:
            self.first_data_time = self.todatetime(self.last_two_data_points[0]['Time']).tz_localize(None)
        if self.timestamp.date() == self.todatetime(self.last_two_data_points[0]['Time']).tz_localize(None).date():
            self.consumption_same_day.append(data)
            return
        else:
            self.update_day_consumption_dict()
            if self.timestamp-self.first_data_time >= pd.Timedelta(self.num_days_coll_data,'d'):
                time_series_data_frame = pd.DataFrame.from_dict(self.day_consumption_dict, orient='index', columns=['daily_consumption'])
                time_series = aggregate(self.period, time_series_data_frame)
                self.fit(time_series)
                predicted_value = self.predict(self.prediction_length).first_value()
                logger.debug(f"Prediction: {predicted_value}")
                return {'value': predicted_value, 'timestamp': self.timestamp.strftime('%Y-%m-%d %X')}
            self.consumption_same_day = [data]

    @abc.abstractmethod
    def fit(train_time_series):
        """To be implemented """
        return 

    @abc.abstractmethod
    def predict(number_of_steps):
        """To be implemented """
        return 