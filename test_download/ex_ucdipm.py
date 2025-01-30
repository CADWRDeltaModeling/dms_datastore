# https://ipm.ucanr.edu/weather/weather-data-export.cfm?stnKey=281&searchStr=lodi&startDate=2024-10-27&endDate=2024-11-25&unit=f&interval=1440&weatherApp=caWeatherApp&export=text
# %%
import requests
import pandas as pd
import io

UCD_IPM_URL = "https://ipm.ucanr.edu/weather/weather-data-export.cfm"


def get_weather_data(start_date, end_date, stnKey=281, searchStr="lodi"):
    url = f"{UCD_IPM_URL}?stnKey={stnKey}&startDate={start_date}&endDate={end_date}&unit=f&interval=1440&weatherApp=caWeatherApp&export=text"
    response = requests.get(url)
    assert response.status_code == 200
    df = pd.read_csv(io.StringIO(response.text), skiprows=6, delimiter="\t")
    df.index = pd.to_datetime(df["Date"])
    df.index.freq = pd.infer_freq(df.index)
    df.drop(columns=["Date"], inplace=True)
    return df


# %%
df = get_weather_data("2024-10-27", "2024-11-30")
df.to_csv("lodi_weather.csv")
# %%
