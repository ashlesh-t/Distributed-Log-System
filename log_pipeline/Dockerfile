FROM python:3.11-slim
WORKDIR /app
COPY log_pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY log_pipeline/ ./log_pipeline/
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "log_pipeline.main"]
