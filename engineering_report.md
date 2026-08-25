# Engineering Report - AeroGrid Telemetry

**To:** AeroGrid CTO  
**From:** Gunjana Narsinghani, Data Engineer (contract)  
**Re:** Telemetry analysis findings and proposed streaming architecture

## Findings

I wrote a Python script using pandas to analyse the 24-hour telemetry file (`telemetry_data.csv`). The file has 5000 rows across 10 turbines, so I grouped the readings by turbine before applying the anomaly rules from the brief.

Two turbines need urgent maintenance:

| Turbine | Rule failed | Metric |
|---------|-------------|--------|
| **T-04** | Average temperature > 85 °C | 90.58 °C |
| **T-07** | Max vibration > 15 mm/s | 25.0 mm/s |

No other turbines failed either rule in this sample.

## Architecture justification

The legacy server is struggling because it tries to ingest and process the full IoT stream in one place. The proposed design splits that work apart:

1. Sensors send data through an edge gateway into **AWS Kinesis**, which buffers traffic when readings spike.
2. **AWS Lambda** runs the same anomaly checks in real time and triggers alerts through **Amazon SNS**.
3. Recent data goes into **Amazon Timestream** for dashboards; older data is archived in **Amazon S3**.

That stops one server doing everything and reduces the risk of crashes. See `architecture_diagram.png` for the full flow.

## Cost optimisation

- Use S3 lifecycle policies to move older telemetry to Glacier after 30 days.
- Keep Timestream for recent dashboard queries; keeping every raw reading in hot storage would cost more than S3 for older data.

## Next steps

- The batch script validates the anomaly logic on historical data.
- The streaming design applies the same rules as readings arrive, so issues show up as they happen rather than only in a batch file.
- The analysis is containerised with Docker so it runs the same way on another machine.

---

*Kind regards,*  
*Gunjana Narsinghani*
