# download the raw report
# https://www.ebmud.com/water/about-your-water/water-supply/water-supply-reports/daily-water-supply-report
# For a particular date it is the URL below
# https://www.ebmud.com/a?url=https://legacy.ebmud.com/if/daily-water-supply-report/WSE_DailyReport.asp?Date=4/19/2022

import os
import datetime
import requests
import glob
import pandas as pd
from dms_datastore import store_utils as utils


def build_filename(date_str, base_dir):
    return base_dir + "/" + date_str.replace("/", "-") + ".html"


def parse_mokelumne_flow(fname):
    tables = pd.read_html(fname)
    df = tables[2]
    date = os.path.basename(fname).split(".")[0]
    val = df[df[0] == "Mokelumne River below WID"][4].values[0].split()[0]
    return date, float(val.replace(",", ""))


def update_existing(date, value, fname):
    name = fname.split(".csv")[0]
    if not len(glob.glob(f"{name}*.csv")):
        print(f"{fname} does not exist! Starting a new file named: {fname}")
        df = pd.DataFrame(columns=["Date", "Value"])
        df = df.set_index("Date")
        df = df.astype("float")
    else:
        df = utils.read_by_years(name)
    #
    dfnew = pd.DataFrame(
        [[float(value)]], columns=df.columns, index=[pd.to_datetime(date)]
    )
    dfnew.index.name = "Date"
    # update the new data frame values
    df = pd.concat([df, dfnew])
    # if duplicate indexes, keep the last one (latest)
    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()
    utils.store_by_years(df, name)


def update_last_7days(
    fname="mokelumne_flow.csv",
    raw_dir="raw",
    converted_dir="formatted",
):
    today = datetime.datetime.now()
    for i in range(7):
        report_date = today - datetime.timedelta(days=(i + 1))
        date_str = report_date.strftime("%m/%d/%Y")
        download_fname = build_filename(date_str, raw_dir)
        rvals = parse_mokelumne_flow(download_fname)
        update_existing(*rvals, fname=os.path.join(converted_dir, fname))


def save_report(date_str, base_dir):
    response = requests.get(
        f"https://www.ebmud.com/a?url=https://legacy.ebmud.com/if/daily-water-supply-report/WSE_DailyReport.asp?Date={date_str}"
    )
    assert response.status_code == 200
    fname = build_filename(date_str, base_dir)
    with open(fname, "w") as fh:
        fh.write(response.text)


def download_last_7(base_dir="raw"):
    utils.ensure_dir(base_dir)
    today = datetime.datetime.now()
    for i in range(7):
        report_date = today - datetime.timedelta(days=i + 1)
        date_str = report_date.strftime("%m/%d/%Y")
        save_report(date_str, base_dir=base_dir)


def main():
    download_last_7()
    update_last_7days()


if __name__ == "__main__":
    main()
