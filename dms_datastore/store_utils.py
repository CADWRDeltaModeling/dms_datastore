import os
import dask.dataframe as ddf
import numpy as np
import pandas as pd


def ensure_dir(dirname="raw"):
    if not os.path.exists(dirname):
        os.makedirs(dirname)


#
def store_by_years(df, name):
    """Store dataframe df to the path with name (no extension as it appended as .csv) partitioned by year"""
    ensure_dir(os.path.dirname(name))
    df = df.reset_index()
    df["year"] = df["Date"].dt.year
    for y in df["year"].unique():
        dfy = df[df["year"] == y]
        dfy = dfy.drop("year", axis=1)
        dfy = dfy.set_index("Date")
        dfy.to_csv(f"{name}_{y}.csv")


def read_by_years(
    name, type_map={"Date": "datetime64[ns]", "Value": "float"}, **csv_kwargs
):
    """Read dataframe from name (no extension as assumed to .csv) and data assumed to be partitioned by year"""
    df = ddf.read_csv(f"{name}_*.csv", **csv_kwargs).compute()
    df = df.astype(dtype=type_map)
    df = df.set_index("Date")
    df.index.freq = df.index.inferred_freq  # as it is read by dask dataframe
    return df


def update_by_years(dfnew, name, keep_old=False):
    """Update data at name (path name with extension of .csv as it assumed and partitioned by year) with data"""
    try:
        dfold = read_by_years(name)
        # concat the rows from both
        dfc = pd.concat([dfnew, dfold], axis=1)
        # keep old or new values
        if keep_old:
            keep = 1
            other = 0
        else:
            keep = 0
            other = 1
        df = dfc.iloc[:, [keep]].combine_first(dfc.iloc[:, [other]])
    except IndexError as ierr:  # if there is nothing to update
        df = dfnew
    df.sort_index()
    return df
