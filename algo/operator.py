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
from .process_and_aggregate import update_same_period_consumption_lists, update_period_consumption_dict
import pandas as pd
from collections import defaultdict
import os
import pickle
import abc


from util.logger import logger

class Operator(util.OperatorBase):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        data_path = config.data_path
        if not os.path.exists(data_path):
            os.mkdir(data_path)

        self.initial_data = True

        self.overall_period_consumption_dict = {}

        self.periods = config.time_periods

        self.consumption_same_period_dict = {period: [] for period in self.periods}
        
        self.timestamp = None
        self.prediction_length = int(config.prediction_length)

        self.predicted_values_dict = defaultdict(list)
        self.min_training_samples = 3

        self.overall_period_consumption_dict_file_path = f'{data_path}/overall_period_consumption_dict.pickle'
        self.predicted_values_dict_file = f'{data_path}/predicted_values_dict.pickle'

        if os.path.exists(self.overall_period_consumption_dict_file_path):
            with open(self.overall_period_consumption_dict_file_path, 'rb') as f:
                self.overall_period_consumption_dict = pickle.load(f)
        if os.path.exists(self.predicted_values_dict_file):
            with open(self.predicted_values_dict_file, 'rb') as f:
                self.predicted_values_dict = pickle.load(f)
    
    def run(self, data, selector='energy_func'):
        self.timestamp = self.todatetime(data['Time']).tz_localize(None)
        logger.info('energy: '+str(data['Consumption'])+'  '+'time: '+str(self.timestamp))

        if self.initial_data:
            for period in self.periods:
                self.consumption_same_period_dict[period] = [data]
            self.initial_data = False
        
        else:
            last_timestamp = self.todatetime(self.consumption_same_period[-1]['Time']).tz_localize(None)

            period_changed_dict, self.consumption_same_period_dict = update_same_period_consumption_lists(self.timestamp, last_timestamp, data, self.consumption_same_period_dict)

            self.overall_period_consumption_dict = update_period_consumption_dict(period_changed_dict, self.consumption_same_period_dict, self.overall_period_consumption_dict)
            with open(self.overll_period_consumption_dict_file_path, 'wb') as f:
                pickle.dump(self.overall_period_consumption_dict, f)
            
            for period, new_period in period_changed_dict.keys():
                if new_period:
                    overall_period_consumption_df = pd.DataFrame.from_dict(self.overall_period_consumption_dict[period], orient='index', 
                                                                                             columns=[f'{period}_consumption'])
                    overall_period_consumption_ts = convert_and_fill_to_timeseries(period, overall_period_consumption_df)
                    
                    self.consumption_same_period_dict[period] = [data] 

                    if len(overall_period_consumption_ts) >= self.min_training_samples:
                        self.fit(overall_period_consumption_ts)
                        predicted_value = self.predict(self.prediction_length).first_value()
                        logger.info("Prediction for next " + period + f": {predicted_value}")
                        self.predicted_values_dict[period].append((self.timestamp, predicted_value))
                        with open(self.predicted_values_dict_file, 'wb') as f:
                            pickle.dump(self.predicted_values_dict,f)
                    return {f'forecast_{period}': self.predicted_values_dict[period][-1] for period in self.periods}
        
    @abc.abstractmethod
    def fit(train_time_series):
        """To be implemented """
        return 

    @abc.abstractmethod
    def predict(number_of_steps):
        """To be implemented """
        return 