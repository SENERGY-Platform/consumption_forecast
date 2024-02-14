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

from operator_lib.util import OperatorBase, logger
from algo.convert import convert_and_fill_to_timeseries
from algo.process_and_aggregate import update_same_period_consumption_lists, update_period_consumption_dict, todatetime
import pandas as pd
from collections import defaultdict
import os
import pickle
import abc
import math


from operator_lib.util import Config
class CustomConfig(Config):
    data_path = "/opt/data"
    model = "prophet"
    prediction_length = 1
    add_time_covariates = False
    time_periods = ["D"]

class Operator(OperatorBase):
    __metaclass__ = abc.ABCMeta
    configType = CustomConfig

    def init(self, *args, **kwargs):
        super().init(*args, **kwargs)
        data_path = self.config.data_path
        if not os.path.exists(data_path):
            os.mkdir(data_path)

        self.initial_data = True

        self.periods = self.config.time_periods
        self.all_possible_periods = {'H', '4H', 'D', 'W', 'M', 'Y'}
        self.period_translation_dict = {'H': 'Hour', 'D': 'Day', 'W': 'Week', 'M': 'Month', 'Y': 'Year'}
        self.period_single_or_multi_output = {'H': 'H', 'D': '4H', 'W': 'D', 'M': 'D', 'Y': 'W'}

        self.overall_period_consumption_dict = {period: {} for period in self.all_possible_periods}

        self.consumption_same_period_dict = {period: [] for period in self.all_possible_periods}
        
        self.timestamp = None

        self.predicted_values_dict = defaultdict(list)
        self.min_training_samples = 3

        self.last_total_value_dict = {period: {'Consumption' :0} for period in self.periods}

        self.overall_period_consumption_dict_file_path = f'{data_path}/overall_period_consumption_dict.pickle'
        self.predicted_values_dict_file = f'{data_path}/predicted_values_dict.pickle'

        if os.path.exists(self.overall_period_consumption_dict_file_path):
            with open(self.overall_period_consumption_dict_file_path, 'rb') as f:
                self.overall_period_consumption_dict = pickle.load(f)
        if os.path.exists(self.predicted_values_dict_file):
            with open(self.predicted_values_dict_file, 'rb') as f:
                self.predicted_values_dict = pickle.load(f)
    
    def run(self, data, selector='energy_func'):
        self.timestamp = todatetime(data['Time']).tz_localize(None)
        logger.info('energy: '+str(data['Consumption'])+'  '+'time: '+str(self.timestamp))

        if self.initial_data:
            for period in self.all_possible_periods:
                self.consumption_same_period_dict[period] = [data]
            self.initial_data = False
        
        else:
            last_timestamp = todatetime(self.consumption_same_period_dict[self.periods[0]][-1]['Time']).tz_localize(None)
            if last_timestamp > self.timestamp:
                return

            period_changed_dict, self.consumption_same_period_dict = update_same_period_consumption_lists(self.timestamp, last_timestamp, data, self.consumption_same_period_dict)

            self.overall_period_consumption_dict = update_period_consumption_dict(period_changed_dict, self.consumption_same_period_dict, self.overall_period_consumption_dict)
            with open(self.overall_period_consumption_dict_file_path, 'wb') as f:
                pickle.dump(self.overall_period_consumption_dict, f)

            for period in self.all_possible_periods:
                if period_changed_dict[period]:
                    self.consumption_same_period_dict[period] = [data] 

            operator_output = self.check_periods_for_changes(period_changed_dict)
            
            if operator_output:
                logger.info(f"Operator Output: {operator_output}")
                return operator_output

    def check_periods_for_changes(self, period_changed_dict):
        at_least_one_period_changed = False 

        for period in self.periods:
            timeseries_frequency = self.period_single_or_multi_output[period]
            
            if period_changed_dict[timeseries_frequency]:
                at_least_one_period_changed = True
                overall_period_consumption_df = pd.DataFrame.from_dict(self.overall_period_consumption_dict[timeseries_frequency], orient='index', 
                                                                                             columns=[f'{timeseries_frequency}_consumption'])

                overall_period_consumption_ts = convert_and_fill_to_timeseries(timeseries_frequency, overall_period_consumption_df)

                self.last_total_value_dict[period] = self.consumption_same_period_dict[period][-1]

                if len(overall_period_consumption_ts) >= self.min_training_samples:
                    if timeseries_frequency == period:
                        predicted_value = self.predict_single_output(overall_period_consumption_ts)
                    else:
                        predicted_value = self.predict_multi_output(timeseries_frequency, period, overall_period_consumption_ts)

                    logger.info("Prediction for this " + period + f": {predicted_value}")
                    self.predicted_values_dict[period].append((self.timestamp, predicted_value))
                    with open(self.predicted_values_dict_file, 'wb') as f:
                        pickle.dump(self.predicted_values_dict,f)

        if at_least_one_period_changed:
            return {
                f'{self.period_translation_dict[period]}Prediction': self.predicted_values_dict[period][-1][1] for period in self.periods if self.predicted_values_dict[period]
            }|{
                f'{self.period_translation_dict[period]}PredictionTotal': self.predicted_values_dict[period][-1][1] + self.last_total_value_dict[period]['Consumption'] for period in self.periods if self.predicted_values_dict[period]
            }|{
                f'{self.period_translation_dict[period]}Timestamp': self.timestamp.to_period(period).to_timestamp(how="end").strftime('%Y-%m-%d %X') for period in self.periods if self.predicted_values_dict[period]
            }
        

    def predict_single_output(self, overall_period_consumption_ts):
        self.fit(overall_period_consumption_ts)

        n_steps = 1
        predicted_value = self.predict(n_steps).first_value()
        predicted_value = (predicted_value + abs(predicted_value))/2 # This cuts the predicted value at 0 to prevent negative values
        return predicted_value

    def predict_multi_output(self, time_series_frequency, period, overall_period_consumption_ts):
        self.fit(overall_period_consumption_ts)

        missing_steps, weekly_proportion = self.compute_output_nr(time_series_frequency, period, overall_period_consumption_ts)
        
        predicted_values = self.predict(missing_steps)
        predicted_values = (predicted_values + abs(predicted_values))/2 # This cuts the predicted timeseries at 0
        predicted_period_consumption = self.compute_period_pred(predicted_values, overall_period_consumption_ts, period, weekly_proportion)
        return predicted_period_consumption

    def compute_output_nr(self, small_period, target_period, time_series):# ts: Darts.Timeseries of frequency (1 per small_period)
        time_last_value = time_series.time_index[-1]
        end_next_target_period = self.timestamp.to_period(target_period).to_timestamp(how="end")
        time_until_next_target_period = end_next_target_period - time_last_value
        
        if small_period == 'H':
            return int(time_until_next_target_period/pd.Timedelta(1,small_period)), None
        elif small_period == '4H':
            return int(time_until_next_target_period/pd.Timedelta(small_period)), None
        elif small_period == 'D':
            return int(time_until_next_target_period/pd.Timedelta(1,small_period)), None
        elif small_period == 'W':
            number_weeks_proportion = time_until_next_target_period/pd.Timedelta(1,'W') 
            return math.ceil(number_weeks_proportion), number_weeks_proportion-math.floor(number_weeks_proportion)
        
    def compute_period_pred(self, predicted_series, true_values_series, target_period, weekly_proportion=None):
        time_last_value = true_values_series.time_index[-1]
        begin_last_period = self.timestamp.to_period(target_period).to_timestamp(how="begin")
        if begin_last_period > time_last_value:
            true_sum_since_last_period_begin = 0
        else:
            true_sum_since_last_period_begin = true_values_series[begin_last_period:].sum(axis=0).first_value()
        
        if weekly_proportion:
            prediction_for_overlapping_week = weekly_proportion * predicted_series[-1].first_value()
            if len(predicted_series)==1:
                predictions_for_weeks_inside_period = 0
            else:
                predictions_for_weeks_inside_period = predicted_series[:-1].sum(axis=0).first_value()
            predicted_sum_from_now_until_end_of_period = prediction_for_overlapping_week + predictions_for_weeks_inside_period
        else:
            predicted_sum_from_now_until_end_of_period = predicted_series.sum(axis=0).first_value()
        
        return true_sum_since_last_period_begin + predicted_sum_from_now_until_end_of_period

    @abc.abstractmethod
    def fit(train_time_series):
        """To be implemented """
        return 

    @abc.abstractmethod
    def predict(number_of_steps):
        """To be implemented """
        return 
    
from operator_lib.operator_lib import OperatorLib
from algo import get_estimator
if __name__ == "__main__":
    aux_operator = Operator()
    operator = get_estimator(aux_operator.config)
    OperatorLib(Operator(), name="leakage-detection-operator", git_info_file='git_commit')