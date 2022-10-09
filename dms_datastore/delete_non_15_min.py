#!/usr/bin/env python
# -*- coding: utf-8 -*-import pandas as pd
import matplotlib.pyplot as plt
from dms_datastore.read_ts import *
import os

direct="raw/incoming"
direct="raw"
with open("non_15_min_files_checked.txt") as infile:
    for line in infile:
        if ":" in line:
            filename = line.split(":")[0]
            path = os.path.join(direct,filename)
            if os.path.exists(path):
                print(f"Removing {path}")
                os.remove(path)
            else:
                print(f"Path not found: {path}")
