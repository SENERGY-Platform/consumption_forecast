# Daily/Weekly/Monthly Forecast

## Config options

| key                | type                                                    | description                                                  |  default |
|--------------------|---------------------------------------------------------|--------------------------------------------------------------|----------|
| `model`               | string                                               | Name of used prediction model; either 'prophet' or 'nhits'   | 'prophet'|
| `add_time_covariates` | bool                                                 | If set to True add. data variables can be put into the model |  False   |
| `time_periods`        | string                                               | comma sep. str containing the values 'H', 'D', 'W' or 'M'    |   'D'    |

Additional explanation for the config variable 'time_periods': 
* 'H' stands for hourly forecast
* 'D' stands for daily forecast
* 'W' stands for weekly forecast
* 'M' stands for monthly forecast

Examples: 
* If 'time_periods' is set to 'H,W,M', the operator will provide hourly, weekly and monthly forecasts
* If 'time_periods' is set to 'D', the operator will compute daily forecasts
