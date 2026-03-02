import os
import time

import pytest
import requests


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def test_ncro_sites_session_get_harness():
    if not _env_true("RUN_NCRO_HTTP_HARNESS"):
        pytest.skip("Set RUN_NCRO_HTTP_HARNESS=1 to run NCRO HTTP latency harness")

    url = os.getenv("NCRO_HARNESS_URL", "https://wdlhyd.water.ca.gov/hydstra/sites")
    timeout_seconds = float(os.getenv("NCRO_HARNESS_TIMEOUT", "200"))
    max_elapsed_seconds = float(os.getenv("NCRO_HARNESS_MAX_SECONDS", "0"))

    session = requests.Session()
    start = time.perf_counter()
    response = session.get(url, timeout=timeout_seconds)
    elapsed = time.perf_counter() - start

    assert response.ok, f"HTTP {response.status_code} from {url}"

    if max_elapsed_seconds > 0:
        assert (
            elapsed <= max_elapsed_seconds
        ), f"session.get({url}) took {elapsed:.2f}s (threshold={max_elapsed_seconds:.2f}s)"

    print(
        f"NCRO harness: GET {url} completed in {elapsed:.2f}s "
        f"(status={response.status_code}, timeout={timeout_seconds:.1f}s)"
    )
