FROM python:3.11-slim
WORKDIR /app
COPY heartbeat_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY heartbeat_service/ ./heartbeat_service/
COPY log_pipeline/node_status.py ./log_pipeline/node_status.py
COPY log_pipeline/__init__.py ./log_pipeline/__init__.py 2>/dev/null || true
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "heartbeat_service.consumer"]
