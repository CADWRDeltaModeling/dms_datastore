#!/usr/bin/env python
"""Test SMSCG reader with selector"""

from dms_datastore.read_ts import read_last_resort_csv
import pandas as pd

# Test direct read with selector
try:
    ts = read_last_resort_csv(
        "//cnrastore-bdo/Modeling_Data/hydraulic_structures/incoming/MSCK_Data_20250101.csv",
        selector=["DTHST.MSCK_GATE01.POS_FT", "DTHST.MSCK_GATE02.POS_FT", "DTHST.MSCK_GATE03.POS_FT"],
        force_regular=False,
        freq=None,
        na_values=["(null)", "null", "NULL", ""]
    )
    print("SUCCESS: read_last_resort_csv with selector worked")
    print(f"Shape: {ts.shape}")
    print(f"Index type: {type(ts.index)}")
    print(f"First rows:\n{ts.head()}")
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
