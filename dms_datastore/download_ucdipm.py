import requests
import pandas as pd
import io

UCD_IPM_URL = "https://ipm.ucanr.edu/weather/weather-data-export.cfm"


def get_weather_data(start_date, end_date, stnKey=281):
    url = f"{UCD_IPM_URL}?stnKey={stnKey}&startDate={start_date}&endDate={end_date}&unit=f&interval=1440&weatherApp=caWeatherApp&export=text"
    response = requests.get(url)
    assert response.status_code == 200
    df = pd.read_csv(io.StringIO(response.text), skiprows=6, delimiter="\t")
    df.index = pd.to_datetime(df["Date"])
    df.index.freq = pd.infer_freq(df.index)
    df.drop(columns=["Date"], inplace=True)
    return df


# argparse for calling get_weather_data from command line


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", help="Start date in format YYYY-MM-DD")
    parser.add_argument("end_date", help="End date in format YYYY-MM-DD")
    parser.add_argument("--stnKey", help="Station key", default=281)
    args = parser.parse_args()
    df = get_weather_data(args.start_date, args.end_date)
    df.to_csv(f"ucdipm_{args.stnKey}.csv")


if __name__ == "__main__":
    main()
