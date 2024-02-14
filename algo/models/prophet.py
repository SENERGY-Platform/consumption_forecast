from darts.models import Prophet
from .helper import create_darts_encoder
from main import Operator

class DartProphet(Operator):
    def init(self, *args, **kwargs) -> None:
        super().init(*args, **kwargs)
        model_kwargs = {}
        if self.config.add_time_covariates:
            encoders = create_darts_encoder(self.period)
            model_kwargs['add_encoders'] = encoders

        self.model = Prophet(country_holidays="DE", **model_kwargs)
        # weekly and yearly seasonalities are automatically included in Prophet
        # https://facebook.github.io/prophet/docs/seasonality,_holiday_effects,_and_regressors.html
        self.min_training_samples = 4

    def fit(self, train_ts):
        self.model.fit(train_ts)
        
    def predict(self, number_steps):
        return self.model.predict(number_steps)