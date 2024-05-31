# Daily/Weekly/Monthly Forecast


## Input
| key                | type                                                 | description                                               |
|--------------------|------------------------------------------------------|-----------------------------------------------------------|
| `Consumption`            | float                                               | Numeric value that describes any kind of consumption.                     |
| `Time`            | string                                              | Timestamp of consumption value                    |

## Output 

| key                | type                                                 | description                                                                       | 
|--------------------|------------------------------------------------------|-----------------------------------------------------------------------------------|
| `DayPrediction`            | float                                               | Forecasted Consumption for next day.                                    |
| `DayPredictionTotal`             | float                                               | Forecasted meter reading at end of next day.                                                                      |
| `DayTimestamp`         | float                                               | Last timestamp of next day.                                                                  |
| `MonthPrediction`             | float                                                | Forecasted Consumption for next month.                   |
| `MonthPredictionTotal`        | float                                                | Forecasted meter reading at end of next month.                             |
| `MonthTimestamp`        | float                                               | Last timestamp of next month.                                        |
| `YearPrediction`            | float                                               | Forecasted Consumption for next year.                                      |
| `YearPredictionTotal`             | float                                               | Forecasted meter reading at end of next year.                                                                    |
| `YearTimestamp`         | float                                               | Last timestamp of next year.                                                                 |
| `HourPrediction`             | float                                                | Forecasted Consumption for next hour.                  |
| `HourPredictionTotal`        | float                                                | Forecasted meter reading at end of next hour.                             |
| `HourTimestamp`        | float                                               | Last timestamp of next hour.                                        |
| `WeekPrediction`            | float                                               | Forecasted Consumption for next week.                                     |
| `WeekPredictionTotal`             | float                                               | Forecasted meter reading at end of next week.                                                                       |
| `WeekTimestamp`         | float                                               | Last timestamp of next week.                                                                  |


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
