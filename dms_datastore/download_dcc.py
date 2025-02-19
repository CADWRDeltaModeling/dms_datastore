# Delta Cross Channel
# Download the gate operations from https://www.usbr.gov/mp/cvo/vungvari/Ccgates.pdf

import os
import pandas as pd
import datetime
import tabula  # for PDF parsing
import requests
from . import store_utils as utils
import click

click.command()


@click.option("--base-dir", default="data/raw/dxc_gate")
def main(base_dir="data/raw/dxc_gate"):
    """
    Download the Delta Cross Channel gate log from the US Bureau of Reclamation
    https://www.usbr.gov/mp/cvo/vungvari/Ccgates.pdf

    """
    utils.ensure_dir(base_dir)
    today = datetime.datetime.now()
    date_str = today.strftime("%Y-%m-%d")
    url = "https://www.usbr.gov/mp/cvo/vungvari/Ccgates.pdf"
    response = requests.get(url)
    assert response.status_code == 200
    fname = url.split("/")[-1]
    pdfname = os.path.join(base_dir, fname.split(".")[0] + ".pdf")
    with open(pdfname, "wb") as fh:
        fh.write(response.content)
    pages = tabula.read_pdf(
        pdfname, pages="all", guess=False, encoding="ISO-8859-1"  # for windows maybe?
    )  # columns=['date','time','remarks'])
    df = pd.concat(pages)
    df.columns = ["date", "time", "value"]
    df = df.dropna()
    df["datetime"] = df["date"] + " " + df["time"]
    df = df[df["datetime"] != "DATE TIME"]
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df[["datetime", "value"]]
    df = df.set_index("datetime")
    df = df.sort_index()
    df["action"] = df["value"].str.split(expand=True)[0]
    df["comments"] = df["value"].str.split().map(lambda x: " ".join(x[1:]))
    df = df.drop(columns=["value"])
    # df['action'].unique()
    df["action"] = (
        df["action"]
        .map({"open": 2, "closed": 0, "gate": 0, "partially": 1, "-": 0, "close": 0})
        .astype("int")
    )
    conv_dir = os.path.dirname(pdfname).replace("/raw/", "/converted/")
    utils.ensure_dir(conv_dir)
    df.to_csv(os.path.join(conv_dir, fname.split(".")[0] + ".csv"))
