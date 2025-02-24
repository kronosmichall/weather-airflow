select
    country, "time"
from
    {{ source('weather_dataset', 'all_weather_info')}}