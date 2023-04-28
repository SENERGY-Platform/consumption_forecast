import darts

def predict_new_value(time_series_data_frame):
    series = darts.TimeSeries.from_dataframe(time_series_data_frame)
    new_value = None
    return new_value