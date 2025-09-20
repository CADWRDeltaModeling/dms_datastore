import requests
import pandas as pd
import datetime
import os
import click
import tabula
from . import store_utils as utils
from vtools import ts_splice
from csv import QUOTE_NONNUMERIC, QUOTE_MINIMAL


@click.command()
@click.option("--base-dir", default="smscg")
@click.option("--outfile", default="dms_smscg_gate.csv")
def main(base_dir=".", outfile="dms_smscg_gate.csv"):
    raw_dir = os.path.join(base_dir, "raw")
    convert_dir = os.path.join(base_dir, "converted")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(convert_dir, exist_ok=True)
    df0 = download_and_parse_archived_pdf(raw_dir)
    df1 = download_and_parse_active_gate_log(raw_dir)
    df_final = reconcile_archive_with_new(df0, df1)
    # Write CSV with only "remarks" and "user_remarks" quoted
    #df_final = _quote_selected_columns(df_final, ["remarks", "user_remarks"])
    outfile = os.path.join(convert_dir, outfile)
    df_final.to_csv(outfile, index=True, quoting=QUOTE_MINIMAL,date_format="%Y-%m-%dT%H:%M")



def reconcile_archive_with_new(df_archive,df_new):
    df_archive.index.name="datetime"
    df_new.index.name="datetime"  # This should already be true, modulo case convention
    
    df_archive.columns = [x.lower().replace(" ","_") for x in df_archive.columns]
    df_new.columns = [x.lower().replace(" ","_") for x in df_new.columns]
    final_columns = ["flashboards","gate_1","gate_2","gate_3","action","remarks","user_remarks"]
    df_archive = df_archive.reindex(columns=final_columns)
    df_new = df_new.reindex(columns=final_columns)
    df_final = ts_splice((df_archive,df_new),transition="prefer_last")
    cols = ["gate_1", "gate_2", "gate_3"]

    mapping = {
        "O": "Open",
        "OPEN": "Open",
        "C": "Closed",
        "CLSD": "Closed",
        "CLOSED": "Closed",
        "OP": "Tidal",
        "M-OP": "Tidal",
        "TIDAL": "Tidal",
    }

    for col in cols:
        s = df_final[col].astype("string").str.strip().str.upper()
        df_final.loc[:, col] = s.map(mapping).fillna(df_final[col])
        # Ensure the time index is sorted and unique
        df_final = df_final[~df_final.index.duplicated(keep="last")]
        df_final = df_final.sort_index()
       
    return df_final

def download_and_parse_active_gate_log(raw_dir="raw"):
    """
    Download the Suisun Marsh Salinity Control Gates log from the California Natural Resources Agency
    # https://data.cnra.ca.gov/dataset/suisun-marsh-salinity-control-gates-log/resource/265729e9-4ac0-469e-828b-2564ac077689
    """
    utils.ensure_dir(raw_dir)
    today = datetime.datetime.now()
    date_str = today.strftime("%Y-%m-%d")
    url = "https://data.cnra.ca.gov/dataset/e76622ca-b6e9-4e78-a08e-deb9580d49b3/resource/265729e9-4ac0-469e-828b-2564ac077689/download/smscg-log.xlsx"
    response = requests.get(url)
    assert response.status_code == 200
    fname = url.split("/")[-1]
    xlsfname = os.path.join(raw_dir, fname.split(".")[0] + ".xlsx")
    with open(xlsfname, "wb") as fh:
        fh.write(response.content)
    df = pd.read_excel(xlsfname, parse_dates=True, index_col=0)
    df = df.sort_index()
    conv_dir = os.path.dirname(xlsfname).replace("/raw/", "/converted/")
    utils.ensure_dir(conv_dir)
    #df.to_csv(os.path.join(raw_dir, fname.split(".")[0] + ".csv"))
    return df



def download_and_parse_archived_pdf(base_dir="raw"):
    """
    Download and parse the archived PDF log of the Suisun Marsh Salinity Control Gates.
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
    #df.to_csv(os.path.join(conv_dir, "histsmscgopnew.csv"), index=True)
    return df

if __name__ == "__main__":
    main()
