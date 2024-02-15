from darts.models import Prophet
from .helper import create_darts_encoder

class DartProphet():
    def __init__(self, config) -> None:
        model_kwargs = {}
        if config.add_time_covariates:
            encoders = create_darts_encoder(config.period)
            model_kwargs['add_encoders'] = encoders

        self.model = Prophet(country_holidays="DE", **model_kwargs)
        # weekly and yearly seasonalities are automatically included in Prophet
        # https://facebook.github.io/prophet/docs/seasonality,_holiday_effects,_and_regressors.html
        self.min_training_samples = 4

    def fit(self, train_ts):
        self.model.fit(train_ts)
        
    def predict(self, number_steps):
        return self.model.predict(number_steps)