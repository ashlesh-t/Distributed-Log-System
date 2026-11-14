FROM python:3.11-slim
WORKDIR /app
COPY api_gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api_gateway/ ./api_gateway/
COPY common/__init__.py common/constants.py common/secret_box.py common/url_safety.py ./common/
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "api_gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
