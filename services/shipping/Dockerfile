FROM python:3.11-slim
WORKDIR /app
COPY services/shipping/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ ./common/
COPY services/shipping/service.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "service.py"]
