import requests
import pandas as pd
import datetime
import os
import click
import tabula
from . import store_utils as utils


@click.command()
@click.option("--base-dir", default="data/raw/montezuma_gate_log")
def main(base_dir="data/raw/montezuma_gate_log"):
    download_and_parse_active_gate_log(base_dir)
    download_and_parse_archived_pdf(base_dir)


def download_and_parse_active_gate_log(base_dir="data/raw/montezuma_gate"):
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


def download_and_parse_archived_pdf(base_dir="data/raw/montezuma_gate_log"):
    """
    Download and parse the archived PDF log of the Montezuma Slough Salinity Control Gates.
    """
    utils.ensure_dir(base_dir)
    url = "https://data.cnra.ca.gov/dataset/e76622ca-b6e9-4e78-a08e-deb9580d49b3/resource/7b3ab962-202b-43c2-9ac7-08f2303b153b/download/histsmscgopnew.pdf"
    response = requests.get(url)
    assert response.status_code == 200
    pdf_fname = os.path.join(base_dir, "histsmscgopnew.pdf")
    with open(pdf_fname, "wb") as fh:
        fh.write(response.content)

    # Parse the PDF using tabula-py
    dfs = tabula.read_pdf(
        pdf_fname, pages="all", multiple_tables=True, encoding="ISO-8859-1"
    )  # for windows maybe?)

    # Combine all tables into a single DataFrame
    for i in range(len(dfs)):
        dfs[i]["DATE"] = pd.to_datetime(dfs[i]["DATE"], errors="coerce")
        dfs[i] = dfs[i].set_index("DATE")
    df = pd.concat(dfs)
    df = df.sort_index()

    # Save the DataFrame to CSV
    conv_dir = os.path.dirname(pdf_fname).replace("/raw/", "/converted/")
    utils.ensure_dir(conv_dir)
    df.to_csv(os.path.join(conv_dir, "histsmscgopnew.csv"), index=True)


if __name__ == "__main__":
    main()
