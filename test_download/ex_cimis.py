# %%
from dms_datastore import download_cimis
import pandas as pd

# %%
with open("cimis_password.secret", "r") as f:
    password = f.read().strip()
# %%
cx = download_cimis.CIMIS(password=password)
# %%
dfcat = cx.get_stations_info()
dfcat["Connect"] = pd.to_datetime(dfcat["Connect"])
min_year = dfcat["Connect"].dt.year.min()
active_stations = list(dfcat[dfcat["Status"] == "Active"]["Station Number"])
dfcat.to_csv("cimis_stations.csv", index="Station Number")
# %%
current_year = pd.to_datetime("today").year
# %%
cx.download_all_hourly_zipped(min_year, current_year - 2)
# %%
for year in range(current_year - 2, current_year):
    cx.download_hourly_unzipped(year, active_stations)
# %%
cx.download_current_year(active_stations)
# %%
import tqdm

for station in tqdm.tqdm(dfcat["Station Number"], total=len(dfcat)):
    try:
        dfs = cx.load_station(station)
        dfs.to_csv(f"cimis_{station:03d}.csv", index="Date")
    except Exception as e:
        print(f"Error: {e}")
        continue
