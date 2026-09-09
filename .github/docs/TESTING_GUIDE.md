# Testing guide

## Environment

The `dms_datastore` conda environment is assumed to exist. Activate it before installing or testing.

```bash
conda activate dms_datastore
pip install --no-deps -e .
```

## Suites

| Location | Kind | Notes |
| --- | --- | --- |
| `tests/` | Unit and integration | Config is monkeypatched; no real repository is required |
| `test_repo/` | Repository integration | Pass `--repo=<path>` to pytest |

```bash
conda activate dms_datastore
pytest tests
pytest test_repo --repo=<path-to-repo>
```

## Conventions

- Use `tmp_path` and `monkeypatch` to isolate configuration.
- Do not couple unit tests to the shared repository path.
- Mark tests that require web connectivity as `integration`.
- GitHub Actions excludes `integration`; user-launched runs may include it.

## pytest configuration

Configured in `[tool.pytest.ini_options]` of [pyproject.toml](../../pyproject.toml): strict markers, JUnit XML output, and ignores for `setup.py` and `build/`.
