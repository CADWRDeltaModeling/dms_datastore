import logging

import pytest


@pytest.fixture(autouse=True)
def _restore_package_logger():
    """Restore the ``dms_datastore`` logger's state after every test.

    CLI commands call :func:`dms_datastore.logging_config.configure_logging`,
    which sets ``propagate=False`` on the package logger and attaches stream
    and file handlers. That is correct for real runs (it prevents double
    logging through the root logger) but the state is process-global, so a
    CLI test leaks it into every later test. With ``propagate=False``,
    records never reach the root logger where ``caplog`` listens on some
    pytest versions (e.g. 9.0.3), and ``caplog.text`` comes back empty.
    The result is order- and version-dependent failures in tests that assert
    on warnings (e.g. ``test_read_ts_index_name.py``).

    Handlers added during the test are closed so file handlers pointing into
    ``tmp_path`` do not stay open (which also blocks cleanup on Windows).
    """
    logger = logging.getLogger("dms_datastore")
    propagate, level, handlers = logger.propagate, logger.level, list(logger.handlers)
    yield
    for handler in logger.handlers:
        if handler not in handlers:
            handler.close()
    logger.handlers[:] = handlers
    logger.propagate = propagate
    logger.setLevel(level)
