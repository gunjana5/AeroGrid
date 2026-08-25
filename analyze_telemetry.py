# flags T-04 / T-07 on the sample csv

from pathlib import Path

import pandas as pd

# brief thresholds - keep in sync with stream_monitor
TEMP_THRESHOLD_C = 85.0
VIBRATION_THRESHOLD_MM_S = 15.0

DEFAULT_CSV = Path(__file__).parent / "telemetry_data.csv"


def load_telemetry(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # sample has these five - fail early if the csv is trimmed
    expected_cols = {"timestamp", "turbine_id", "temperature_c", "vibration_mm_s", "rpm"}
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing columns: {sorted(missing)}")
    return df


def find_failing_turbines(df: pd.DataFrame) -> pd.DataFrame:
    # group by turbine, then avg-temp / max-vibration (same rules as the stream)
    summary = (
        df.groupby("turbine_id", as_index=False)
        .agg(
            avg_temperature_c=("temperature_c", "mean"),
            max_vibration_mm_s=("vibration_mm_s", "max"),
            reading_count=("turbine_id", "count"),
        )
    )

    summary["fails_temp_rule"] = summary["avg_temperature_c"] > TEMP_THRESHOLD_C
    summary["fails_vibration_rule"] = summary["max_vibration_mm_s"] > VIBRATION_THRESHOLD_MM_S
    summary["requires_maintenance"] = summary["fails_temp_rule"] | summary["fails_vibration_rule"]

    return summary[summary["requires_maintenance"]].sort_values("turbine_id")


def main() -> None:
    csv_path = DEFAULT_CSV
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Could not find {csv_path.name}. Place telemetry_data.csv in the same folder as this script."
        )

    df = load_telemetry(csv_path)
    failing = find_failing_turbines(df)

    print("AeroGrid telemetry analysis")
    print(f"Rows analysed: {len(df)}")
    print(f"Turbines in sample: {df['turbine_id'].nunique()}")
    print()
    print("Anomaly rules:")
    print(f"  - average temperature > {TEMP_THRESHOLD_C} C")
    print(f"  - max vibration > {VIBRATION_THRESHOLD_MM_S} mm/s")
    print()

    if failing.empty:
        print("Failing turbine IDs: none")
        return

    print("Failing turbine IDs:")
    for _, row in failing.iterrows():
        reasons = []
        if row["fails_temp_rule"]:
            reasons.append(f"avg temp {row['avg_temperature_c']:.2f} C")
        if row["fails_vibration_rule"]:
            reasons.append(f"max vibration {row['max_vibration_mm_s']:.1f} mm/s")
        reason_text = "; ".join(reasons)
        print(f"  - {row['turbine_id']} ({reason_text})")

    print()
    print("Summary list:", ", ".join(failing["turbine_id"].tolist()))


if __name__ == "__main__":
    main()
