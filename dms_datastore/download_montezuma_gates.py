import requests
import pandas as pd
from . import store_utils as utils
import datetime
import os
import click


@click.command()
@click.option("--base-dir", default="data/raw/montezuma_gate_log")
def main(base_dir="data/raw/montezuma_gate_log"):
    """
    Download the Montezuma Slough Salinity Control Gates log from the California Natural Resources Agency
    # https://data.cnra.ca.gov/dataset/suisun-marsh-salinity-control-gates-log/resource/265729e9-4ac0-469e-828b-2564ac077689
    """
    utils.ensure_dir(base_dir)
    today = datetime.datetime.now()
    date_str = today.strftime("%Y-%m-%d")
    url = "https://data.cnra.ca.gov/dataset/e76622ca-b6e9-4e78-a08e-deb9580d49b3/resource/265729e9-4ac0-469e-828b-2564ac077689/download/smscg-log.xlsx"
    response = requests.get(url)
    assert response.status_code == 200
    fname = url.split("/")[-1]
    xlsfname = os.path.join(base_dir, fname.split(".")[0] + ".xlsx")
    with open(xlsfname, "wb") as fh:
        fh.write(response.content)
    df = pd.read_excel(xlsfname, parse_dates=True, index_col=0)
    df = df.sort_index()
    conv_dir = os.path.dirname(xlsfname).replace("/raw/", "/converted/")
    utils.ensure_dir(conv_dir)
    df.to_csv(os.path.join(conv_dir, fname.split(".")[0] + ".csv"))
