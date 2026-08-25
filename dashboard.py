# fills aerogrid.db via stream_monitor if empty

from __future__ import annotations

import argparse
import json
import sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

from stream_monitor import DEFAULT_CSV, DEFAULT_DB, SCHEMA, run_stream

# only plot the two that trip the brief rules
CHART_TURBINES = ("T-04", "T-07")


def db_has_readings(db_path: Path) -> bool:
    if not db_path.exists():
        return False
    conn = sqlite3.connect(db_path)
    try:
        # table might be missing on a fresh / broken file
        row = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='readings'"
        ).fetchone()
        if not row or row[0] == 0:
            return False
        count = conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
        return int(count) > 0
    except sqlite3.Error:
        # corrupt / half-written db - treat as empty and rebuild
        return False
    finally:
        conn.close()


def ensure_db(csv_path: Path, db_path: Path) -> None:
    if db_has_readings(db_path):
        print(f"using existing db: {db_path}")
        return
    # first visit fills the db so charts aren't empty
    print(f"db empty - running stream_monitor (speed=0) into {db_path}")
    run_stream(csv_path=csv_path, db_path=db_path, speed=0.0)


def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    # Row so dict(r) works for json
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def load_alerts(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, turbine_id, reasons, reading_count, created_at
        FROM alerts
        ORDER BY id
        """
    ).fetchall()
    return [dict(r) for r in rows]


def load_series(conn: sqlite3.Connection, turbine_id: str) -> dict[str, list[Any]]:
    # chart.js wants parallel arrays
    rows = conn.execute(
        """
        SELECT timestamp, temperature_c, vibration_mm_s
        FROM readings
        WHERE turbine_id = ?
        ORDER BY id
        """,
        (turbine_id,),
    ).fetchall()
    return {
        "labels": [r["timestamp"] for r in rows],
        "temperature_c": [r["temperature_c"] for r in rows],
        "vibration_mm_s": [r["vibration_mm_s"] for r in rows],
    }


def build_html(alerts: list[dict[str, Any]], series: dict[str, dict[str, list[Any]]]) -> str:
    # inline page - no flask, just stdlib http
    # strings from this db, not user html
    alert_rows = "".join(
        f"<tr><td>{a['id']}</td><td>{a['turbine_id']}</td>"
        f"<td>{a['reasons']}</td><td>{a['reading_count']}</td>"
        f"<td>{a['created_at']}</td></tr>"
        for a in alerts
    )
    if not alert_rows:
        alert_rows = '<tr><td colspan="5">no alerts yet</td></tr>'

    chart_blocks = []
    for tid in CHART_TURBINES:
        chart_blocks.append(
            f"""
      <section class="chart-block">
        <h2>{tid}</h2>
        <canvas id="temp-{tid}" height="120"></canvas>
        <canvas id="vib-{tid}" height="120"></canvas>
      </section>
"""
        )

    # bake series into the page so there's no separate /api
    series_json = json.dumps(series)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>AeroGrid dashboard</title>
  <!-- chart.js from cdn - fine for local demo -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #0f1419;
      --panel: #1a222c;
      --text: #e8eef4;
      --muted: #8a9aab;
      --line: #2a3542;
      --temp: #e07a3d;
      --vib: #3d9be0;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(ellipse at top, #1a2836 0%, var(--bg) 55%);
      color: var(--text);
      min-height: 100vh;
      padding: 2rem 1.5rem 3rem;
    }}
    h1 {{ font-size: 1.6rem; font-weight: 600; margin: 0 0 0.25rem; }}
    .sub {{ color: var(--muted); margin: 0 0 1.75rem; font-size: 0.95rem; }}
    h2 {{ font-size: 1.1rem; margin: 0 0 0.75rem; font-weight: 600; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      margin-bottom: 2rem;
    }}
    th, td {{
      text-align: left;
      padding: 0.65rem 0.8rem;
      border-bottom: 1px solid var(--line);
      font-size: 0.9rem;
    }}
    th {{ color: var(--muted); font-weight: 500; }}
    .charts {{
      display: grid;
      gap: 1.5rem;
      grid-template-columns: 1fr;
    }}
    @media (min-width: 900px) {{
      .charts {{ grid-template-columns: 1fr 1fr; }}
    }}
    .chart-block {{
      background: var(--panel);
      padding: 1rem 1rem 0.5rem;
    }}
    canvas {{ margin-bottom: 0.75rem; }}
  </style>
</head>
<body>
  <h1>AeroGrid</h1>
  <p class="sub">local sqlite view - alerts + T-04 / T-07 metrics</p>

  <h2>alerts</h2>
  <table>
    <thead>
      <tr>
        <th>id</th><th>turbine</th><th>reasons</th><th>readings</th><th>created</th>
      </tr>
    </thead>
    <tbody>
      {alert_rows}
    </tbody>
  </table>

  <div class="charts">
    {"".join(chart_blocks)}
  </div>

  <script>
    const SERIES = {series_json};

    function makeChart(canvasId, label, labels, data, color) {{
      const el = document.getElementById(canvasId);
      if (!el) return;
      new Chart(el, {{
        type: "line",
        data: {{
          labels,
          datasets: [{{
            label,
            data,
            borderColor: color,
            backgroundColor: "transparent",
            pointRadius: 0,
            borderWidth: 1.5,
            tension: 0.15
          }}]
        }},
        options: {{
          responsive: true,
          animation: false,
          plugins: {{
            legend: {{ labels: {{ color: "#8a9aab" }} }}
          }},
          scales: {{
            x: {{
              ticks: {{
                color: "#8a9aab",
                maxTicksLimit: 6,
                maxRotation: 0
              }},
              grid: {{ color: "#2a3542" }}
            }},
            y: {{
              ticks: {{ color: "#8a9aab" }},
              grid: {{ color: "#2a3542" }}
            }}
          }}
        }}
      }});
    }}

    // one temp + one vib chart per flagged turbine
    for (const tid of Object.keys(SERIES)) {{
      const s = SERIES[tid];
      makeChart("temp-" + tid, "temp °C", s.labels, s.temperature_c, "#e07a3d");
      makeChart("vib-" + tid, "vibration mm/s", s.labels, s.vibration_mm_s, "#3d9be0");
    }}
  </script>
</body>
</html>
"""


def render_page(db_path: Path) -> str:
    # rebuild html each request - data is small, keeps the handler dumb
    with open_db(db_path) as conn:
        alerts = load_alerts(conn)
        series = {tid: load_series(conn, tid) for tid in CHART_TURBINES}
    return build_html(alerts, series)


def make_handler(db_path: Path) -> type[BaseHTTPRequestHandler]:
    # closure so the handler can see db_path
    class DashboardHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path not in ("/", "/index.html"):
                self.send_error(404)
                return
            body = render_page(db_path).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: Any) -> None:
            # quieter than the default BaseHTTPRequestHandler spam
            print(f"[dashboard] {self.address_string()} {fmt % args}")

    return DashboardHandler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Serve a tiny AeroGrid dashboard from aerogrid.db."
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    ensure_db(csv_path=args.csv, db_path=args.db)
    handler = make_handler(args.db)
    server = HTTPServer((args.host, args.port), handler)
    url = f"http://{args.host}:{args.port}/"
    print(f"AeroGrid dashboard at {url}")
    print("Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
