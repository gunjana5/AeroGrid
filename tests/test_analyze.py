# expects T-04 / T-07 on the sample csv

from pathlib import Path

import pandas as pd
import pytest

from analyze_telemetry import find_failing_turbines, load_telemetry
from stream_monitor import TEMP_THRESHOLD_C, VIBRATION_THRESHOLD_MM_S
from thresholds import (
    TEMP_THRESHOLD_C as SHARED_TEMP,
    VIBRATION_THRESHOLD_MM_S as SHARED_VIB,
)

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "telemetry_data.csv"


def test_load_telemetry_has_expected_columns():
    df = load_telemetry(CSV_PATH)
    assert {"timestamp", "turbine_id", "temperature_c", "vibration_mm_s", "rpm"} <= set(
        df.columns
    )
    assert len(df) == 5000


def test_load_telemetry_missing_columns_raises(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("timestamp,turbine_id\n1,T-01\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing columns"):
        load_telemetry(bad)


def test_failing_turbines_are_t04_and_t07():
    df = load_telemetry(CSV_PATH)
    failing = find_failing_turbines(df)
    assert set(failing["turbine_id"]) == {"T-04", "T-07"}


def test_t04_fails_temperature_rule():
    df = load_telemetry(CSV_PATH)
    failing = find_failing_turbines(df).set_index("turbine_id")
    assert bool(failing.loc["T-04", "fails_temp_rule"]) is True
    assert float(failing.loc["T-04", "avg_temperature_c"]) > 85.0


def test_t07_fails_vibration_rule():
    df = load_telemetry(CSV_PATH)
    failing = find_failing_turbines(df).set_index("turbine_id")
    assert bool(failing.loc["T-07", "fails_vibration_rule"]) is True
    assert float(failing.loc["T-07", "max_vibration_mm_s"]) > 15.0


def test_healthy_turbine_not_flagged():
    df = pd.DataFrame(
        {
            "timestamp": ["t1", "t2"],
            "turbine_id": ["T-99", "T-99"],
            "temperature_c": [60.0, 70.0],
            "vibration_mm_s": [5.0, 8.0],
            "rpm": [15.0, 16.0],
        }
    )
    failing = find_failing_turbines(df)
    assert failing.empty


def test_batch_and_stream_share_thresholds():
    assert TEMP_THRESHOLD_C is SHARED_TEMP
    assert VIBRATION_THRESHOLD_MM_S is SHARED_VIB
