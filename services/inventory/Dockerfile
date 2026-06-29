FROM python:3.11-slim
WORKDIR /app
COPY services/inventory/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ ./common/
COPY services/inventory/service.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "service.py"]
