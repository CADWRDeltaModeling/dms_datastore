# dms_datastore agent guidance

`dms_datastore` provides general time-series repository, registry, retrieval, and data-management functionality.

It is not a SCHISM package. Keep SCHISM- and BayDeltaSCHISM-specific workflow assumptions in downstream packages.

It is not a DSM2 or HEC-DSS-aware package. Keep materials involving pydss out of the repo.

## Package role

`dms_datastore` depends on `vtools`.

Repository APIs should provide consistent handling of:

- time-series identity
- metadata
- flags
- missing-value conventions
- regularity
- repository selection and lookup

Consumers should not need to reimplement these guarantees.

## Dependency policy

- Assume the established scientific Python/tool stack is available.
- Do not program indirect dependencies on HEC-DSS.
- Do not add to the spatial dependency stack without a specific need.
- Do not add to the plotting dependency stack without a specific need.

## Coding practice

- Plan before coding.
- Keep functions single-purpose.
- Keep functions testable.
- Do not refactor outside the scope of the requested work. Alert the user if broader refactoring appears warranted.
- Do not contract existing documentation.
- Preserve NumPy-style documentation; repair it when interfaces change.
- Prefer explicit errors such as `ValueError` over elaborate recovery from invalid arguments.
- Avoid making inference from surrounding files the only way to use an API. Inference may be a convenience, but important inputs should also be supplyable explicitly.
- Assume the established tool stack exists; do not add elaborate defensive discovery for expected dependencies.
- Don't check something retrieved with is_regular=True for regulatity or any time series retrieved with api functions for index uniqueness. 
- Preserve repository semantics and metadata rather than optimizing for one downstream consumer.

## Command-line interfaces

When creating, modifying, reviewing, or documenting a command-line
interface, follow `docs/CLI_GUIDE.md`.

## Elements of style

- Prefer failure to robustification and silent passes.
- For long batch processes, use the established log-and-quarantine pattern rather than aborting the whole run or swallowing errors.
- Command-line interfaces use `click`.
- Every CLI command should have a workhorse function providing the same capability programmatically.
- Layer designs so that opening and validating data is separable from the programmatic work. This is not always possible for downloaders.

## Data access and regularity

Repository-aware readers are preferred over raw Pandas ingestion for repository data.

- For continuous, regular data prefer `read_ts_repo`: `from dms_datastore import read_ts_repo`.
- The default repo is `screened` (defined in `dstore_config.yaml`). Others include `processed` for filled/transformed/derived data and `structures` for irregular gated data.
- Everything in the `screened` tier is regular. Structures are not regular.
- `force_regular` is normally `True`. Report and solve problems rather than reverting it. Setting `force_regular=False` to make an error go away is an antipattern.
- Use `read_ts(file_or_pattern)` for explicit files or glob patterns.
- Avoid `pd.read_csv` except in special cases. It omits wildcard handling, regression issues, flag handling, NA codes, and `#` comments, and it loses metadata.
- After reading repository data, do not re-check regularity or duplicate index values. The repository guarantees them.
- Scripts using `read_ts_repo` in applied settings may assume "back door" acquisition using known station ids, but should offer a CLI or config path to acquire from files instead. [TODO: provide tools for this]

Continuous-data workflows normally expect regular time series. When irregularity violates the repository contract, diagnose and correct the underlying problem rather than silently changing behavior to accept it.

Interfaces and repositories should provide the regularity guarantees expected by downstream scripts.

## Configuration

Configuration in this package should remain generic.

Where configuration behavior analogous to OmegaConf is needed, maintain near-parity rather than depending on SCHISM-specific configuration tools.

`schimpy.schism_yaml` does not belong in this package.

Config paths are resolved by `dstore_config.config_file(label)`, which checks the current working directory first and then `config_data/`. Utilities such as `dropbox_data.py` and `reformat` accept either a path or a label that resolves through `config_file()`; for that reason `str` is often preferred over `Path` in those signatures.

## Time-series dependencies

Use `vtools` for reusable time-series algorithms rather than reproducing them locally.

Examples include:

- prioritized merging and tiling
- interval operations
- filtering
- conservative interpolation

Use lower-case frequency strings such as:

`min`, `h`, `d`, `s`

Do not move generic `vtools` algorithms into `dms_datastore` merely because repository code uses them.

## Testing

- Use `pytest`.
- Mark tests requiring web connectivity as `integration`.
- Normal GitHub Actions should exclude integration tests.
- User-launched test runs should still be able to include them when appropriate.
- Use `tmp_path` and `monkeypatch` for config isolation; do not couple unit tests to a shared repository path.

See [.github/docs/TESTING_GUIDE.md](.github/docs/TESTING_GUIDE.md) for the test layout and how to run each suite.

## Specialized knowledge

Task-specific detail lives in skills and reference docs rather than in this file.

| Topic | Where |
| --- | --- |
| Module map, layers, data-flow stages | [.github/docs/PACKAGE_GUIDE.md](.github/docs/PACKAGE_GUIDE.md) |
| Key files to read first | [.github/docs/SOURCE_MAP.md](.github/docs/SOURCE_MAP.md) |
| Test layout and invocation | [.github/docs/TESTING_GUIDE.md](.github/docs/TESTING_GUIDE.md) |
| File naming grammar, CSV/front-matter format, metadata semantics | `repository-format` skill |
| Download → reformat → screen → update sequence | `repository-ingestion` skill |
| Dropbox recipe ingestion | `dropbox-ingestion` skill |
| Station registry, coordinates, sublocations | `station-registry` skill |
