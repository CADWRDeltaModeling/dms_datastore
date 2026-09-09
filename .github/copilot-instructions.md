# dms_datastore — Workspace Instructions

Local project rules in [AGENTS.md](../AGENTS.md) take precedence. Read it first.

Organization defaults are in BayDeltaSCHISM https://raw.githubusercontent.com/CADWRDeltaModeling/BayDeltaSCHISM/refs/heads/master/AGENTS.md and apply only where AGENTS.md is silent.

Deeper material is split out of AGENTS.md:

- [docs/PACKAGE_GUIDE.md](docs/PACKAGE_GUIDE.md) — module map and architecture
- [docs/SOURCE_MAP.md](docs/SOURCE_MAP.md) — key files to read first
- [docs/TESTING_GUIDE.md](docs/TESTING_GUIDE.md) — test layout and invocation
- `skills/` — `repository-format`, `repository-ingestion`, `dropbox-ingestion`, `station-registry`


# Build and Test

The `dms_datastore` conda environment is assumed to exist. Always activate it before running any tests or install commands.

```bash
# Install (development mode)
conda activate dms_datastore
pip install --no-deps -e .

```

pytest is configured in `pyproject.toml` (`[tool.pytest.ini_options]`): strict markers, JUnit XML output, ignores `setup.py` and `build/`.

