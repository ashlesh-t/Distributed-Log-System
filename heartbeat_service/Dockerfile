FROM python:3.11-slim
WORKDIR /app
COPY heartbeat_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY heartbeat_service/ ./heartbeat_service/
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "heartbeat_service.consumer"]
