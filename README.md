# Daily/Weekly/Monthly Forecast


## Input
| key                | type                                                 | description                                               |
|--------------------|------------------------------------------------------|-----------------------------------------------------------|
| `Consumption`            | float                                               | Numeric value that describes any kind of consumption.                     |
| `Time`            | string                                              | Time stamp of consumption value                    |

## Output 

| key                | type                                                 | description                                                                       | 
|--------------------|------------------------------------------------------|-----------------------------------------------------------------------------------|
| `value`            | string                                               | ID of source providing weather forecast data.                                     |
| `type`             | string                                               | Anomaly Type                                                                      |
| `sub_type`         | string                                               | Anomaly Sub Type                                                                  |
| `mean`             | float                                                | Current mean of point outlier detector that detected the anomaly                  |
| `threshold`        | float                                                | Threshold that was used to compare with single values                             |
| `device_id`        | string                                               | Device for which the anomaly was detected                                         |
| `initial_phase`    | string                                               | Message whether and how long the operator is in the initialization/training phase |


## Config options

| key                | type                                                    | description                                                    |  default |
|--------------------|---------------------------------------------------------|----------------------------------------------------------------|----------|
| `model`               | string                                               | Name of used prediction model; either 'prophet' or 'nhits'     | 'prophet'|
| `add_time_covariates` | bool                                                 | If set to True add. data variables can be put into the model   |  False   |
| `time_periods`        | string                                               | comma sep. str containing the values 'H', 'D', 'W', 'M' or 'Y' |   'D'    |

Additional explanation for the config variable 'time_periods': 
* 'H' stands for hourly forecast
* 'D' stands for daily forecast
* 'W' stands for weekly forecast
* 'M' stands for monthly forecast
* 'Y' stands for yearly forecast

Examples: 
* If 'time_periods' is set to 'H,W,M', the operator will provide hourly, weekly and monthly forecasts
* If 'time_periods' is set to 'D', the operator will compute daily forecasts
