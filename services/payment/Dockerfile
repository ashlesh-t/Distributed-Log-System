FROM python:3.11-slim
WORKDIR /app
COPY services/payment/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ ./common/
COPY services/payment/service.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "service.py"]
