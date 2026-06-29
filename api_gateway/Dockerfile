FROM python:3.11-slim
WORKDIR /app
COPY api_gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api_gateway/ ./api_gateway/
COPY heartbeat_service/restart_handler.py ./heartbeat_service/restart_handler.py
COPY heartbeat_service/__init__.py ./heartbeat_service/__init__.py 2>/dev/null || true
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "api_gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
