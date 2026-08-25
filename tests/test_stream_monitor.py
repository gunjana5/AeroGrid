# transition-only alerts + sqlite persistence

from pathlib import Path

import pytest

from stream_monitor import (
    Reading,
    StreamMonitor,
    format_alert,
    iter_csv_readings,
    parse_reading,
    run_stream,
)

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "telemetry_data.csv"


def test_parse_reading():
    row = {
        "timestamp": "4/15/2026 18:48",
        "turbine_id": "T-09",
        "temperature_c": "71.5",
        "vibration_mm_s": "10.3",
        "rpm": "16.9",
    }
    reading = parse_reading(row)
    assert reading.turbine_id == "T-09"
    assert reading.temperature_c == 71.5
    assert reading.vibration_mm_s == 10.3


def test_alert_on_vibration_transition(tmp_path):
    db = tmp_path / "test.db"
    with StreamMonitor(db_path=db) as monitor:
        first = monitor.process_reading(
            Reading("t1", "T-01", 50.0, 5.0, 10.0)
        )
        assert first is None

        alert = monitor.process_reading(
            Reading("t2", "T-01", 50.0, 20.0, 10.0)
        )
        assert alert is not None
        assert alert.turbine_id == "T-01"
        assert any("vibration" in r for r in alert.reasons)

        # still failing - no second alert
        again = monitor.process_reading(
            Reading("t3", "T-01", 50.0, 21.0, 10.0)
        )
        assert again is None
        assert len(monitor.alerts) == 1
        assert monitor.reading_count() == 3


def test_alert_on_avg_temperature_transition(tmp_path):
    # avg of 80 then 100 = 90 > 85
    db = tmp_path / "test.db"
    with StreamMonitor(db_path=db) as monitor:
        assert (
            monitor.process_reading(Reading("t1", "T-02", 80.0, 1.0, 10.0))
            is None
        )
        alert = monitor.process_reading(
            Reading("t2", "T-02", 100.0, 1.0, 10.0)
        )
        assert alert is not None
        assert any("temp" in r for r in alert.reasons)
        assert (
            monitor.process_reading(Reading("t3", "T-02", 100.0, 1.0, 10.0))
            is None
        )


def test_sqlite_persists_readings(tmp_path):
    db = tmp_path / "persist.db"
    with StreamMonitor(db_path=db) as monitor:
        monitor.process_reading(Reading("t1", "T-03", 60.0, 4.0, 12.0))
        monitor.process_reading(Reading("t2", "T-03", 61.0, 4.5, 12.0))
        assert monitor.reading_count() == 2

    with StreamMonitor(db_path=db) as monitor:
        assert monitor.reading_count() == 2


def test_sqlite_persists_alerts(tmp_path):
    db = tmp_path / "alerts.db"
    with StreamMonitor(db_path=db) as monitor:
        monitor.process_reading(Reading("t1", "T-01", 50.0, 5.0, 10.0))
        monitor.process_reading(Reading("t2", "T-01", 50.0, 20.0, 10.0))
        # still failing - one row only
        monitor.process_reading(Reading("t3", "T-01", 50.0, 21.0, 10.0))
        assert monitor.alert_count() == 1
        row = monitor._conn.execute(
            "SELECT turbine_id, reasons, reading_count FROM alerts"
        ).fetchone()
        assert row[0] == "T-01"
        assert "vibration" in row[1]
        assert row[2] == 2

    with StreamMonitor(db_path=db) as monitor:
        assert monitor.alert_count() == 1


def test_iter_csv_batches():
    batches = list(iter_csv_readings(CSV_PATH, batch_size=100))
    total = sum(len(b) for b in batches)
    assert total == 5000
    assert len(batches[0]) == 100


def test_run_stream_flags_t04_and_t07(tmp_path, capsys):
    db = tmp_path / "stream.db"
    alerts = run_stream(csv_path=CSV_PATH, db_path=db, speed=0.0)
    flagged = {a.turbine_id for a in alerts}
    assert flagged == {"T-04", "T-07"}
    assert len(alerts) == 2
    out = capsys.readouterr().out
    assert "ALERT T-04" in out
    assert "ALERT T-07" in out


def test_format_alert():
    from stream_monitor import Alert

    text = format_alert(
        Alert(turbine_id="T-04", reasons=["avg temp 90.58 C"], reading_count=12)
    )
    assert "ALERT T-04" in text
    assert "90.58" in text


def test_missing_csv_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        run_stream(csv_path=tmp_path / "missing.csv", db_path=tmp_path / "x.db")
