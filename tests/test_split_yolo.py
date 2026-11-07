import numpy as np
import pandas as pd

from dms_datastore.processed.process_yolo import split_yolo_total


def _series(values):
    return pd.Series(values, index=pd.date_range("2026-01-01", periods=len(values), freq="D"),
                     dtype=float)


def test_split_without_measured_toe():
    total = _series([0.0, 3000.0, 4000.0, 10000.0, np.nan])
    toe, yolo = split_yolo_total(total)
    np.testing.assert_allclose(toe, [0.0, 3000.0, 4000.0, 4300.0, np.nan])
    np.testing.assert_allclose(yolo, [0.0, 0.0, 0.0, 5700.0, np.nan])
    np.testing.assert_allclose((toe + yolo)[:4], total[:4])


def test_split_with_measured_toe_and_negative_clip():
    total = _series([3000.0, 5000.0, 4500.0])
    toe_in = _series([2500.0, 3000.0, 4600.0])
    toe, yolo = split_yolo_total(total, toe=toe_in)
    np.testing.assert_allclose(toe, [2500.0, 3100.0, 4595.0])
    np.testing.assert_allclose(yolo, [0.0, 1900.0, 0.0])


def test_split_parameters_and_frame_input():
    total = pd.DataFrame({"value": [100.0, 300.0]})
    toe, yolo = split_yolo_total(total, threshold=200.0, toe_share=0.5)
    assert isinstance(toe, pd.DataFrame)
    np.testing.assert_allclose(toe["value"], [100.0, 250.0])
    np.testing.assert_allclose(yolo["value"], [0.0, 50.0])


def test_split_transforms():
    from dms_datastore.processed.process_yolo import (
        transform_yolo_bypass,
        transform_yolo_toe_drain,
    )

    total = pd.DataFrame({"yolo": [3000.0, 10000.0]})
    np.testing.assert_allclose(transform_yolo_toe_drain(total)["yolo"], [3000.0, 4300.0])
    np.testing.assert_allclose(transform_yolo_bypass(total)["yolo"], [0.0, 5700.0])
