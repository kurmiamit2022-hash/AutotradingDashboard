FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY signal_dashboard.py .
ENV HOST=0.0.0.0 INTERVAL=15m
CMD ["python", "signal_dashboard.py"]
