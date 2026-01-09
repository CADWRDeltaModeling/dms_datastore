import requests
import pandas as pd
import io
import click

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


def download_ucdipm(start_date, end_date, stnkey=281):
    """Download weather data from UC Davis IPM."""

    df = get_weather_data(start_date, end_date, stnkey)
    df.to_csv(f"ucdipm_{stnkey}.csv")


@click.command()
@click.argument("start_date")
@click.argument("end_date")
@click.option("--stnkey", default=281, type=int)
def download_ucdipm_cli(start_date, end_date, stnkey):
    """CLI for downloading UC Davis IPM weather data.

    Arguments:
        START_DATE: Start date in format YYYY-MM-DD
        END_DATE: End date in format YYYY-MM-DD

    Options:
        --stnkey: Station key (default: 281)
    """

    download_ucdipm(start_date, end_date, stnkey)


if __name__ == "__main__":
    download_ucdipm_cli()
