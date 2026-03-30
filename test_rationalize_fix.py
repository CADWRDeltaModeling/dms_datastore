#!/usr/bin/env python
"""Quick test to verify rationalize_time_partitions fix."""

from dms_datastore.filename import interpret_fname, naming_spec
from dms_datastore.rationalize_time_partitions import RAW_NAMING

# Test that RAW_NAMING is defined at module level
print("Testing RAW_NAMING is defined at module level...")
assert RAW_NAMING is not None
print("✓ RAW_NAMING is defined")

# Test that it can parse a raw filename
test_filename = "dwr_des_KSWC_flow_1980_1999.csv"
print(f"\nTesting interpret_fname with: {test_filename}")
try:
    meta = interpret_fname(test_filename, naming=RAW_NAMING)
    print(f"✓ Successfully parsed: {meta}")
    assert meta["agency"] == "dwr_des"
    assert meta["param"] == "flow"
    assert meta["syear"] == "1980"
    assert meta["eyear"] == "1999"
    print("✓ All metadata fields correct")
except ValueError as e:
    print(f"✗ Failed: {e}")
    raise

print("\n✓ All tests passed!")
