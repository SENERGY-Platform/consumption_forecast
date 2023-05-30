import darts
from darts.dataprocessing.transformers import MissingValuesFiller

def convert_and_fill_to_timeseries(period, data_frame):
    if period == 'day':
        time_series = darts.TimeSeries.from_dataframe(data_frame, freq='D')
    elif period == 'week':
        time_series = darts.TimeSeries.from_dataframe(data_frame, freq='W')
    elif period == 'month':
        time_series = darts.TimeSeries.from_dataframe(data_frame, freq='M')
    
    transformer = MissingValuesFiller()
    filled_time_series = transformer.transform(time_series)
    return filled_time_series