from models.nhits import DartNHITS
from models.prophet import DartProphet

def get_estimator(config):
    model = config['model']
    if model == 'nhits':
        return DartNHITS(config)
    elif model == 'prophet':
        return DartProphet(config)