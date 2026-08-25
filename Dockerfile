# default cmd is the brief batch script; override for stream

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY analyze_telemetry.py stream_monitor.py dashboard.py thresholds.py ./
COPY telemetry_data.csv ./

# stream / dashboard are what i added after the brief:
#   docker run --rm aerogrid python stream_monitor.py --speed 0
CMD ["python", "analyze_telemetry.py"]
