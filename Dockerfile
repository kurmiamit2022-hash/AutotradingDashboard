FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ARG WITH_LIVE=1
COPY requirements-live.txt .
RUN if [ "$WITH_LIVE" = "1" ]; then pip install --no-cache-dir -r requirements-live.txt; fi
COPY signal_dashboard.py cash_symbols.tx[t] ./
RUN mkdir -p /data
ENV MALLOC_ARENA_MAX=2 HOST=0.0.0.0 INTERVAL=15m DB_PATH=/data/dashboard.db ORDER_LOG=/data/orders_log.jsonl
VOLUME /data
CMD ["python", "signal_dashboard.py"]
