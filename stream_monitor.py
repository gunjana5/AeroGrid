# replay csv -> sqlite -> alert on transition only

from __future__ import annotations

import argparse
import csv
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional

# same thresholds as batch (brief rules)
TEMP_THRESHOLD_C = 85.0
VIBRATION_THRESHOLD_MM_S = 15.0

DEFAULT_CSV = Path(__file__).parent / "telemetry_data.csv"
DEFAULT_DB = Path(__file__).parent / "aerogrid.db"

# readings + alerts
SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    turbine_id TEXT NOT NULL,
    temperature_c REAL NOT NULL,
    vibration_mm_s REAL NOT NULL,
    rpm REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_readings_turbine ON readings(turbine_id);

CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    turbine_id TEXT NOT NULL,
    reasons TEXT NOT NULL,
    reading_count INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


@dataclass
class Reading:
    timestamp: str
    turbine_id: str
    temperature_c: float
    vibration_mm_s: float
    rpm: float


@dataclass
class TurbineState:
    # running avg via sum/count - no list of temps
    temp_sum: float = 0.0
    count: int = 0
    max_vibration: float = 0.0
    # latch so a still-failing turbine doesn't spam
    is_alerting: bool = False

    @property
    def avg_temperature(self) -> float:
        return self.temp_sum / self.count if self.count else 0.0

    def update(self, reading: Reading) -> None:
        self.temp_sum += reading.temperature_c
        self.count += 1
        if reading.vibration_mm_s > self.max_vibration:
            self.max_vibration = reading.vibration_mm_s

    def is_failing(self) -> bool:
        return (
            self.avg_temperature > TEMP_THRESHOLD_C
            or self.max_vibration > VIBRATION_THRESHOLD_MM_S
        )

    def failure_reasons(self) -> list[str]:
        reasons: list[str] = []
        if self.avg_temperature > TEMP_THRESHOLD_C:
            reasons.append(f"avg temp {self.avg_temperature:.2f} C")
        if self.max_vibration > VIBRATION_THRESHOLD_MM_S:
            reasons.append(f"max vibration {self.max_vibration:.1f} mm/s")
        return reasons


@dataclass
class Alert:
    turbine_id: str
    reasons: list[str]
    reading_count: int


@dataclass
class StreamMonitor:
    # not kinesis - local sqlite stand-in
    db_path: Path
    states: dict[str, TurbineState] = field(default_factory=dict)
    alerts: list[Alert] = field(default_factory=list)
    _conn: Optional[sqlite3.Connection] = field(default=None, repr=False)

    def connect(self) -> None:
        self._conn = sqlite3.connect(self.db_path)
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> StreamMonitor:
        self.connect()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def store_reading(self, reading: Reading) -> None:
        if self._conn is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        self._conn.execute(
            """
            INSERT INTO readings
                (timestamp, turbine_id, temperature_c, vibration_mm_s, rpm)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                reading.timestamp,
                reading.turbine_id,
                reading.temperature_c,
                reading.vibration_mm_s,
                reading.rpm,
            ),
        )
        # commit per row - fine for demo volume
        self._conn.commit()

    def store_alert(self, alert: Alert) -> None:
        if self._conn is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        self._conn.execute(
            """
            INSERT INTO alerts (turbine_id, reasons, reading_count)
            VALUES (?, ?, ?)
            """,
            (alert.turbine_id, "; ".join(alert.reasons), alert.reading_count),
        )
        self._conn.commit()

    def process_reading(self, reading: Reading) -> Optional[Alert]:
        # alert only on the edge into failing - no spam while still bad
        state = self.states.setdefault(reading.turbine_id, TurbineState())
        state.update(reading)
        self.store_reading(reading)

        failing = state.is_failing()
        if failing and not state.is_alerting:
            state.is_alerting = True
            alert = Alert(
                turbine_id=reading.turbine_id,
                reasons=state.failure_reasons(),
                reading_count=state.count,
            )
            self.alerts.append(alert)
            self.store_alert(alert)
            return alert

        # recover clears the latch so a later spike can alert again
        if not failing and state.is_alerting:
            state.is_alerting = False

        return None

    def reading_count(self) -> int:
        if self._conn is None:
            return 0
        row = self._conn.execute("SELECT COUNT(*) FROM readings").fetchone()
        return int(row[0])

    def alert_count(self) -> int:
        if self._conn is None:
            return 0
        row = self._conn.execute("SELECT COUNT(*) FROM alerts").fetchone()
        return int(row[0])


def parse_reading(row: dict[str, str]) -> Reading:
    return Reading(
        timestamp=row["timestamp"],
        turbine_id=row["turbine_id"],
        temperature_c=float(row["temperature_c"]),
        vibration_mm_s=float(row["vibration_mm_s"]),
        rpm=float(row["rpm"]),
    )


def iter_csv_readings(csv_path: Path, batch_size: int = 1) -> Iterator[list[Reading]]:
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        batch: list[Reading] = []
        for row in reader:
            batch.append(parse_reading(row))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def format_alert(alert: Alert) -> str:
    reason_text = "; ".join(alert.reasons)
    return (
        f"ALERT {alert.turbine_id}: {reason_text} "
        f"(after {alert.reading_count} readings)"
    )


def run_stream(
    csv_path: Path,
    db_path: Path,
    speed: float = 0.0,
    batch_size: int = 1,
) -> list[Alert]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find CSV: {csv_path}")

    with StreamMonitor(db_path=db_path) as monitor:
        for batch in iter_csv_readings(csv_path, batch_size=batch_size):
            for reading in batch:
                alert = monitor.process_reading(reading)
                if alert is not None:
                    print(format_alert(alert))
                # speed=0 for pytest / ci - bump to watch it tick
                if speed > 0:
                    time.sleep(speed)

        print()
        print(f"Stream complete. Readings stored: {monitor.reading_count()}")
        print(f"Alerts emitted: {len(monitor.alerts)}")
        if monitor.alerts:
            print("Turbines flagged:", ", ".join(a.turbine_id for a in monitor.alerts))
        return list(monitor.alerts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Replay AeroGrid telemetry as a mock stream with SQLite + alerts."
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help=f"Path to telemetry CSV (default: {DEFAULT_CSV.name})",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"SQLite database path (default: {DEFAULT_DB.name})",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="Seconds to sleep between rows (default: 0 for tests/CI)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help="Rows to process per batch (default: 1)",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    print("AeroGrid stream monitor")
    print(f"CSV: {args.csv}")
    print(f"DB:  {args.db}")
    print(
        f"Rules: avg temp > {TEMP_THRESHOLD_C} C OR "
        f"any vibration > {VIBRATION_THRESHOLD_MM_S} mm/s"
    )
    print()
    run_stream(
        csv_path=args.csv,
        db_path=args.db,
        speed=args.speed,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
