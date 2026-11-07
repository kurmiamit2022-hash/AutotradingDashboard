FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ARG WITH_LIVE=0
COPY requirements-live.txt .
RUN if [ "$WITH_LIVE" = "1" ]; then pip install --no-cache-dir -r requirements-live.txt; fi
COPY signal_dashboard.py .
RUN mkdir -p /data
ENV HOST=0.0.0.0 INTERVAL=15m DB_PATH=/data/dashboard.db ORDER_LOG=/data/orders_log.jsonl
VOLUME /data
CMD ["python", "signal_dashboard.py"]
