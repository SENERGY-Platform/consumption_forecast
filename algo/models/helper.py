from darts.dataprocessing.transformers.scaler import Scaler 

def create_darts_encoder(period):
    date_covariates = ["month"]

    if period == "day":
        date_covariates.append("dayofweek")

    encoders = {
            "datetime_attribute": {"future": date_covariates},
            #"position": {"past": ["relative"], "future": ["relative"]}, TODO absolute past future?
            "transformer": Scaler(),
    }

    return encoders