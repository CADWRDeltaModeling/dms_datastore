# Source map

Files worth reading before making changes.

## Documentation

| File | Contents |
| --- | --- |
| [README.rst](../../README.rst) | Data model, flags, units, configuration system |
| [README-commands.md](../../README-commands.md) | CLI command reference and workflow examples |
| [README-dropbox.md](../../README-dropbox.md) | Dropbox ingestion via recipe YAML |
| [usgs_to_usbr_transition.md](../../usgs_to_usbr_transition.md) | Notes on the USGS → USBR source transition |
| [AGENTS.md](../../AGENTS.md) | Agent rules for this repository |
| [PACKAGE_GUIDE.md](PACKAGE_GUIDE.md) | Module map and architecture |
| [TESTING_GUIDE.md](TESTING_GUIDE.md) | Test layout and invocation |

## Code and config

| File | Contents |
| --- | --- |
| `dms_datastore/config_data/dstore_config.yaml` | Central config: repo roots, station DBs, filename patterns, source priority |
| `dms_datastore/dstore_config.py` | Config loading and `config_file()` label resolution |
| `dms_datastore/filename.py` | `interpret_fname` / `meta_to_filename` |
| `dms_datastore/read_multi.py` | `read_ts_repo` implementation |
| `dms_datastore/read_ts.py`, `write_ts.py` | CSV + YAML front-matter I/O |
| `dms_datastore/auto_screen.py`, `screeners.py` | Screening rules and flagging |
| `dms_datastore/dropbox_data.py`, `dropbox_recipes/` | Recipe-driven ingestion |
| `station_dbase.csv` (per config) | Station registry — sole owner of coordinates |
| [pyproject.toml](../../pyproject.toml) | Entry points in `[project.scripts]`, pytest config |
